// 검증 판정을 관제가 읽을 수 있게 내보냅니다.
package com.kafkick.batch.config;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToDoubleFunction;

import org.springframework.stereotype.Component;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRuleRepository;
import com.kafkick.core.verification.VerificationRun;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * <b>{@code verdict = FAIL} 은 정상 종료라 알림이 하나도 안 울린다.</b>
 *
 * <p>불일치를 판정으로 남기고 배치는 계속 가는 것이 설계다. 그래서 판정이
 * {@code verification_runs} 에 적힐 뿐, <b>누가 그 행을 조회하기 전까지 아무도 모른다</b> —
 * 게이트 판정의 본체인데 통로가 없었다.
 *
 * <pre>
 *   cy_verification_verdict{dataset,scope}    PASS=0, FAIL=1
 *   cy_verification_findings{dataset,scope}   검출 건수
 * </pre>
 *
 * <p><b>이름에 {@code _total} 을 붙이지 않는다.</b> 카운터 규약이라 Micrometer 의
 * Prometheus 렌더러가 게이지에서는 <b>떼어 낸다</b> — 코드가 부르는 이름과 관제가 보는
 * 이름이 갈려 알림이 영원히 안 뜬다. CY-347 에서 실제로 그렇게 만들었다가 노출 테스트가
 * 잡았다. 이 둘은 <b>마지막 실행의 값</b>이지 누적이 아니다.
 *
 * <p><b>값을 잡 실행 중에 밀어 넣지 않고 되읽는다</b>({@code VerificationMetricsRefresher} 가
 * 60초마다 채운다). 검증은 <b>사람이 손으로, 드물게</b> 돌리므로 프로세스 게이지로 두면
 * 재배포하는 순간 판정이 지표에서 사라진다.
 *
 * <p>만료 지표도 같은 이유로 되읽기가 됐다(CY-421) — 그쪽은 <i>마지막으로 성공한 실행의
 * {@code asOf}</i> 로 다시 센다. 이 클래스와는 재는 대상이 다를 뿐 축은 같다. 재배포하면
 * 판정이 지표에서 사라지는데 DB 에는 남아 <b>관제와 진실이 갈린다</b> — 금요일 {@code FAIL}
 * 이 주말 재시작으로 없어지는 모양이다. {@code VerificationMetricsRefresher} 가 주기적으로
 * 이 클래스를 채운다.
 *
 * <p><b>모르는 것과 통과한 것은 다르다.</b> 판정이 없으면 {@code 0}(=PASS)이 아니라
 * {@code NaN} 이다. {@code 0} 을 내면 <b>합격으로 읽혀</b> 알림이 조용해진다.
 * 그 상태 자체는 {@code VerificationMetricsUnknown} 이 본다 — {@code NaN} 은 시리즈로
 * 존재하되 비교가 전부 거짓이라 <b>다른 알림이 통째로 침묵하기 때문</b>이다.
 *
 * <p><b>붙어 있는 데이터셋 하나만 만든다.</b> 한 프로세스는 DB 하나를 본다 —
 * {@code rejectDatasetMismatch} 가 스키마와 라벨이 어긋난 실행을 시작 전에 거부하므로,
 * CLEAN DB 에는 {@code dataset='CORRUPT'} 인 행이 <b>생길 수 없다.</b> 둘 다 등록하면
 * 안 붙은 쪽이 <b>영원히 {@code NaN}</b> 이고, {@code VerificationMetricsUnknown} 이
 * 기동 30분 뒤 발화해 안 꺼진다 — 상시 거짓 알림 하나가 같은 alertname 그룹에 깔리면
 * 정작 중요한 발화가 거기 묻힌다.
 *
 * <p>판정 근거는 {@code CleanSchemaGuard}·{@code rejectDatasetMismatch} 와 <b>같은 것</b>을
 * 쓴다({@code uk_coupon_member} 의 존재). 같은 사실을 세 곳이 각자 판정하면 어긋나는 날
 * 어느 쪽이 맞는지 아무도 모른다.
 *
 * <p><b>게이지를 미리 등록하되 {@code FULL} 조합만 만든다.</b> 처음 판정이 날 때 등록하면
 * 그전에는 시리즈가 <b>없어서</b> {@code NaN} 이 아니라 {@code absent} 가 되고,
 * {@code != itself} 로 거는 감시가 안 먹는다. 반대로 {@code INCREMENTAL} 까지 만들면
 * {@code rejectUnsupportedScope} 가 막고 있어 <b>영원히 {@code NaN}</b> 이고, 그것이
 * 집계에 전염된다.
 */
@Component
public class VerificationMetrics {

    private static final double UNKNOWN = Double.NaN;

    /**
     * 한 조합의 마지막 판정. <b>둘을 한 덩어리로 바꾼다.</b>
     *
     * <p>따로 {@code set} 하면 그 사이에 스크레이프가 끼어 {@code verdict} 는 이번 실행,
     * {@code findings} 는 앞 실행 값인 샘플이 나온다.
     */
    private record Snapshot(VerdictType verdict, int findings) {
    }

    /** 이 프로세스가 붙어 있는 데이터셋. 게이지도 조회도 이 하나뿐이다. */
    private final DatasetType served;

    /** 값이 {@code null} 이면 아직 모른다. */
    private final AtomicReference<Snapshot> latest = new AtomicReference<>();

    public VerificationMetrics(MeterRegistry registry, VerificationRuleRepository rules) {
        this.served = rules.hasCleanOnlyConstraints() ? DatasetType.CLEAN : DatasetType.CORRUPT;
        gauge(registry, "cy_verification_verdict", served, latest,
                s -> VerdictCode.of(s.verdict()),
                "마지막 검증 판정. PASS=0, FAIL=1");
        gauge(registry, "cy_verification_findings", served, latest,
                s -> s.findings(),
                "마지막 검증이 검출한 건수");
    }

    /** 이 프로세스가 보고 있는 데이터셋. 갱신기가 이것만 조회한다. */
    public DatasetType served() {
        return served;
    }

    /**
     * 되읽은 판정을 그대로 싣는다.
     *
     * <p><b>붙어 있는 데이터셋·{@code FULL} 이 아니면 거부한다.</b> 라벨은 등록 시점에
     * 고정돼 있어서, 다른 조합을 실으면 <b>라벨이 거짓말을 하고</b> 두 범위가 한 시계열을
     * 덮어쓴다. 증분이 열리는 티켓은 등록부터 넓혀야 한다.
     */
    public void record(VerificationRun run) {
        if (run.dataset() != served || run.scope() != ScopeType.FULL) {
            throw new IllegalArgumentException(
                    "이 프로세스의 게이지는 " + served + "/FULL 로 등록돼 있습니다. "
                            + "다른 조합을 실으면 라벨이 실제 값과 갈립니다. "
                            + "받은 값=" + run.dataset() + "/" + run.scope());
        }
        latest.set(new Snapshot(run.verdict(), run.findingCount()));
    }

    /**
     * <b>판정이 없다는 것을 그대로 내보낸다.</b> 직전 값을 들고 있으면 관제는 그것을 지금의
     * 판정으로 읽는다 — 그 조합으로 닫힌 실행이 아직 없거나, 되읽기가 실패한 경우다.
     */
    public void markUnknown() {
        latest.set(null);
    }

    /**
     * <b>{@code FULL} 만 라벨로 단다.</b> {@code INCREMENTAL} 은 {@code rejectUnsupportedScope}
     * 가 시작 전에 거부하므로 만들면 영원히 {@code NaN} 이다. 증분이 열리는 티켓이 여기를
     * 넓히면서 그때 판정 규칙도 함께 정한다 — 라벨을 미리 달아 둔 것은 그때 <b>지표 이름을
     * 안 바꾸려는 것</b>이다.
     */
    private static void gauge(MeterRegistry registry, String name, DatasetType dataset,
            AtomicReference<Snapshot> holder, ToDoubleFunction<Snapshot> field,
            String description) {
        Gauge.builder(name, holder, h -> {
                    Snapshot snapshot = h.get();
                    return snapshot == null ? UNKNOWN : field.applyAsDouble(snapshot);
                })
                .tag("dataset", dataset.name())
                .tag("scope", ScopeType.FULL.name())
                .description(description)
                .register(registry);
    }
}
