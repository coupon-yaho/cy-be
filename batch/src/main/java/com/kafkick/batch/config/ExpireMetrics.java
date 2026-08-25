// 만료 실행의 결과를 관제가 읽을 수 있게 내보냅니다.
package com.kafkick.batch.config;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToDoubleFunction;

import org.springframework.stereotype.Component;

import com.kafkick.core.expiration.PendingExpiration;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * <b>잡의 생사만으로는 "성공했는데 아무것도 안 했다" 를 못 잡는다.</b>
 *
 * <p>기존 알림 셋({@code BatchJobFailed}·{@code ExpireNotSucceeding}·
 * {@code BatchJobRunningTooLong})은 전부 그 축이다. 셋 다 통과하면서 기한 지난 발급건이
 * 계속 쌓이는 상태가 있고, 그것이 이 잡의 가장 나쁜 실패 모드다 — 재고를 되돌리는 유일한
 * 배치가 조용히 일을 안 한다.
 *
 * <p><b>게이지를 셋으로 가르는 것이 핵심이다.</b> 막힌 회차의 몫은 설계상 계속 남는다.
 * 합쳐서 알림을 걸면 사람이 재고를 고칠 때까지 24시간 울리고, 그 알림은 곧 무시된다.
 *
 * <pre>
 *   cy_expire_pending             기한 지났는데 ISSUED 인 전체
 *   cy_expire_blocked_pending     그중 그 실행이 건너뛴 회차의 몫
 *   cy_expire_unexplained_pending 그 둘의 차. <b>알림이 보는 것은 이것 하나다</b>
 *   cy_expire_blocked_coupons     그 실행이 건너뛴 회차 수
 *   cy_expire_measured_at_seconds 위 넷이 기준으로 삼은 asOf   ← 스냅샷이 미는 것은 다섯
 *   cy_expire_clean_schema        되읽기가 붙은 스키마 (1 · 0 · NaN)  ← 스냅샷 밖이다
 * </pre>
 *
 * <p><b>차를 관제가 아니라 여기서 뺀다.</b> 게이지 둘을 내보내고 알림 식이
 * {@code pending - blocked_pending} 을 하면, 두 값을 따로 {@code set} 하는 사이에
 * 스크레이프가 끼어 <b>한쪽만 새 값인 샘플</b>이 나온다. 값 자체는 한 문장에서 함께
 * 세어 왔는데(그러려고 {@code COUNT_PENDING} 을 한 문장으로 만들었다) 마지막 한 걸음에서
 * 다시 갈리는 것이다. 뺄셈을 여기서 해 <b>한 시계열</b>로 내보내면 그 틈이 없다.
 * 나머지 셋은 진단용으로 남긴다.
 *
 * <p><b>이름에 {@code _total} 을 붙이지 않는다.</b> 그것은 카운터 규약이라 Micrometer 의
 * Prometheus 렌더러가 게이지에서는 <b>떼어 낸다</b> — 붙여 두면 코드가 부르는 이름과 관제가
 * 보는 이름이 갈리고, 알림 규칙이 영원히 안 뜬다. 실제로 그렇게 만들었다가 노출 테스트가 잡았다.
 *
 * <p>그래서 알림이 둘로 갈린다 — {@code unexplained_pending > 0} 은
 * <b>배치가 일을 안 한다</b>(서버를 본다), {@code blocked_coupons > 0} 은
 * <b>데이터가 어긋나 있다</b>(데이터를 본다). 설계가 정한
 * <i>"서버를 고칠 상황과 데이터를 볼 상황을 같은 알람으로 묶지 않는다"</i> 가 이것이다.
 *
 * <h2>계약 — 이 게이지는 "마지막으로 성공한 만료 실행이 남긴 몫" 이다</h2>
 *
 * <p><b>기록자는 {@link ExpirePendingRefresher} 하나다</b>(CY-421). 잡이 {@code afterJob}
 * 에서 자기 결과를 밀어 넣던 구조였는데, 그러면 값이 <b>프로세스와 함께 죽는다</b> —
 * 만료가 5분 크론일 때는 몇 분이면 돌아와 안 드러났지만, 배치 창(일 1회)으로 옮긴 뒤로는
 * 재기동부터 다음 04:10 까지 <b>최대 하루</b>가 {@code NaN} 이고 그 사이
 * {@code ExpireLeavesWorkBehind} 가 발화할 수 없다.
 *
 * <p>그래서 <b>지금 시각이 아니라 그 실행의 {@code asOf} 로 다시 센다.</b> 게이지의 등식
 * (<i>"처리했어야 하는데 안 된 몫"</i>)은 그 {@code asOf} 에서만 성립한다 — 지금 시각으로
 * 세면 낮 동안 기한을 넘긴 쿠폰이 정상적으로 쌓여 매일 critical 이 뜬다.
 *
 * <p><b>얼리는 것은 "건너뛴 회차 <u>목록</u>" 하나다.</b> 그 목록은 배치 메타에서 그대로
 * 가져오고, <b>행 수는 그 {@code asOf} 로 지금 다시 센다</b> —
 * {@code pending} 은 전체, {@code blocked_pending} 은 그 고정 목록에 걸리는 몫이다.
 * 한 문장에서 함께 세므로 {@code total < blocked} 가 안 나온다({@code PendingExpiration}).
 *
 * <p>목록을 얼리는 이유는 그 값의 뜻이 <i>"그 실행이 건너뛰기로 <b>결정한</b> 회차"</i>
 * 이기 때문이다. 다시 계산하면 04:10 에 어긋났던 재고를 10:00 에 고치는 순간 그 몫이
 * {@code unexplained} 로 옮겨 가 <b>데이터를 고쳤더니 서버 critical 이 뜬다</b> —
 * 아래 <i>"서버를 고칠 상황과 데이터를 볼 상황을 같은 알람으로 묶지 않는다"</i> 를 정면으로
 * 어긴다. 건너뛴 회차가 다음 창까지 안 만료되는 것은 {@code ExpireSkippingBrokenCoupons}
 * (channel: data)의 몫이다.
 *
 * <p><b>고정과 유동이 섞여 있다는 사실은 게이지 {@code description} 이 진다</b> —
 * 그것이 {@code # HELP} 로 스크레이프에 나가는 유일한 텍스트이고, 새벽에 javadoc 을 여는
 * 사람은 없다.
 *
 * <p><b>스크레이프 때 세지 않는다.</b> 300만 행에 {@code COUNT(*)} 를 15초마다 때리는 꼴이다.
 * 되읽기가 주기로 밀어 넣고, 게이지는 그 값을 들고만 있는다. (그 비용 걱정 자체는
 * {@code idx_issuance_status_expires} 가 생긴 뒤로 과했다 — 두 질의 다 전체 행이 아니라
 * <b>대기 건수</b>에 비례한다. 실측은 {@code ExpirePendingRefresher} 에 적었다.)
 *
 * <p><b>성공한 실행이 없으면 값이 없다.</b> 기동 직후 스크레이프가 0 을 읽으면
 * <i>"밀린 것이 없다"</i> 로 오해된다 — 아직 모르는 것과 없는 것은 다르다. 그래서 그때는
 * {@code NaN} 을 내고, <b>판정할 재료가 없는 실행</b>도 {@link #markUnknown()} 으로 여기로
 * 되돌린다 — 제외 목록을 못 읽었거나 되읽기가 실패한 경우다.
 *
 * <p><b>실패한 실행은 게이지를 안 지운다.</b> 마지막 성공이 남긴 몫은 여전히 사실이고,
 * <i>"잡이 실패했다"</i> 는 {@code BatchJobFailed} 가, <i>"안 돌고 있다"</i> 는
 * {@code ExpireNotSucceeding} 이 각각 진다 — 한 게이지에 세 질문을 얹지 않는다.
 *
 * <p><b>{@code NaN} 이 조용한 이유를 정확히 적어 둔다 — 시리즈가 없어서가 아니다.</b>
 * Prometheus 는 {@code NaN} 을 <b>값으로 저장한다.</b> 시리즈는 존재하고
 * {@code absent()} 는 발화하지 않는다. 알림이 조용한 것은 {@code NaN > 0} 이 거짓이기
 * 때문이다. 이 차이가 중요한 것은 <b>{@code NaN} 이 집계에 전염되기</b> 때문이다 —
 * 인스턴스를 여럿으로 늘려 {@code sum()}·{@code avg_over_time()} 을 걸면 아직 한 번도
 * 안 돈 인스턴스 하나가 <b>집계 전체를 침묵시킨다.</b> 그때는
 * {@code cy_expire_unexplained_pending != NaN} 로 먼저 걸러야 한다.
 */
@Component
public class ExpireMetrics {

    private static final double UNKNOWN = Double.NaN;

    /**
     * 한 실행이 낸 값 묶음. <b>이것을 한 덩어리로 바꾸는 것이 핵심이다.</b>
     *
     * <p>{@code AtomicLong} 을 따로 {@code set} 하면 그 사이에 스크레이프가 끼어
     * <b>서로 다른 실행의 값이 섞인 샘플</b>이 나온다. 기록자가 단일 스레드 하나뿐인 지금은
     * 드물지만, <b>드문 것과 불가능한 것은 다르다</b> — 참조 하나를 통째로 바꾸면 그 갈래가
     * 아예 없어진다. 이 게이지들은 알림의 입력이라 <i>"가끔 섞인다"</i> 를 지고 갈 수 없다.
     */
    private record Snapshot(LocalDateTime asOf, long total, long blocked, long unexplained,
            int blockedCoupons) {
    }

    /** {@code null} 이면 아직 모른다. */
    private final AtomicReference<Snapshot> latest = new AtomicReference<>();

    /**
     * <b>지금 보고 있는 스키마가 정상셋인가.</b> 1 = 정상 · 0 = 오염 · {@code NaN} = 모름.
     *
     * <p>오염셋을 보게 배치를 띄우는 것은 <b>문서화된 정상 절차</b>다({@code docs/14} 의
     * <i>"검증용 셋에 Spring Batch 메타 테이블이 없다"</i> 절). 그때 되읽기는 세지 않고
     * 나머지를 {@code NaN} 으로 두는데, 그것을 {@code ExpireMetricsUnknown} 이
     * <b>고장으로 읽어 상시 warning 을 낸다</b> — 시연 내내 무관한 경보가 켜져 있게 된다.
     * 관제가 그 둘을 가를 수단이 없어서 이 게이지를 만들었다.
     *
     * <p><b>{@code cy_verification_verdict{dataset}} 과 같은 술어다</b> — 그쪽도
     * {@code rules.hasCleanOnlyConstraints()} 로 {@code served} 를 정하고
     * ({@link VerificationMetrics}), {@code record} 가 라벨과 다른 데이터셋을 거부한다.
     * 그래도 라벨이 아니라 불리언으로 두는 이유는 <b>계산 시점이 다르기 때문</b>이다 —
     * 저쪽은 생성자에서 한 번이라 라벨로 고정할 수 있는데, 이쪽은 되읽기마다 다시 읽는다.
     * 라벨은 등록 시점에 박히므로 매 주기 갱신되는 값을 실을 수 없다.
     * 대시보드가 두 계열을 한 패널에 얹어야 하는 날 그 시점 차이부터 정리한다.
     *
     * <p><b>{@code 0} 은 두 뜻을 뭉갠다</b> — {@code hasCleanOnlyConstraints()} 는
     * <i>"오염셋이다"</i> 와 <i>"테이블이 아예 없다"</i> 에 똑같이 {@code false} 를 낸다.
     * {@code SchemaPresenceGuard} 가 뒤엣것에 <b>기동을 실패시키지만</b>, 그것은
     * {@code ApplicationRunner} 단계라 {@code @Scheduled} 가 이미 뜬 뒤다 — 죽기 전
     * 한 틱이 {@code 0} 을 낼 수 있다. 프로세스가 곧 내려가 실해는 없다.
     * 그 가드를 무르는 티켓은 이 값도 셋으로 갈라야 한다.
     */
    private final AtomicReference<Double> cleanSchema = new AtomicReference<>();

    /**
     * <b>청크가 얼마나 찼나</b>(0~1). 만료는 한 청크에 회차 하나만 담으므로,
     * 후보가 회차 경계에 걸리면 {@code chunk-size} 보다 적게 처리하고 끝난다.
     *
     * <p>시드에서는 발급건 id 가 회차별로 뭉쳐 있어 짧아지는 청크가 회차 경계 147개뿐이다
     * ({@code cy-seed} 의 {@code seedgen/issuances.py} 가 회차 단위로 돌며 id 를 증가시킨다).
     * <b>운영은 다르다</b> — 회차가 동시에 열려 있으면 id 가 엇갈려 연속부가 짧아지고,
     * 극단에서는 청크마다 한 건씩만 처리한다. 잡은 여전히 옳지만 <b>느려진다.</b>
     *
     * <p>그 조짐이 여기 보인다. 한 표본으로는 못 본다 — <b>마지막 청크는 언제나 짧아서</b>
     * 게이지로 내면 항상 나쁘게 보인다. 실행 전체의 평균을 봐야 한다:
     * {@code rate(cy_expire_chunk_fill_sum[1d]) / rate(cy_expire_chunk_fill_count[1d])}.
     *
     * <p><b>백분위를 안 낸다.</b> Micrometer 의 백분위는 롤링 윈도(기본 2분)라 표본이
     * 만료되는데, 이 잡은 <b>하루 한 번</b> 04:10 에 돈다 — 하루 중 23시간 55분 동안
     * {@code quantile} 시계열이 0 으로 찍힌다. 그 패널은 곧 무시되고, 충전율이 실제로
     * 나빠진 날에도 아무 변화가 안 보인다. {@code _sum}/{@code _count} 는 누적이라 안 죽는다.
     */
    private final DistributionSummary chunkFill;

    public ExpireMetrics(MeterRegistry registry) {
        this.chunkFill = DistributionSummary.builder("cy_expire_chunk_fill")
                .description("만료 청크 충전율(0~1) — 평균이 낮게 이어지면 회차 경계에 계속 걸린다는 뜻")
                .register(registry);
        gauge(registry, "cy_expire_pending", Snapshot::total,
                "마지막으로 성공한 실행의 asOf 기준으로 지금 다시 센 값 — 기한이 지났는데 아직 ISSUED 인 발급건");
        gauge(registry, "cy_expire_blocked_pending", Snapshot::blocked,
                "같은 값 중, 그 실행이 건너뛴 회차 목록(고정)에 걸리는 몫 — 행 수는 지금 다시 센다");
        gauge(registry, "cy_expire_unexplained_pending", Snapshot::unexplained,
                "배치가 처리했어야 하는데 안 된 몫(전체 - 막힌 몫). 알림이 보는 값");
        gauge(registry, "cy_expire_blocked_coupons", s -> s.blockedCoupons(),
                "마지막으로 성공한 실행이 건너뛴 회차 수. 그 실행의 결정이라 재고를 고쳐도 안 바뀐다");
        // 오염셋 기동은 정상 절차라 그때의 NaN 은 고장이 아니다. ExpireMetricsUnknown 이
        // 이 값으로 그 갈래를 뺀다(근거는 위 필드 javadoc).
        Gauge.builder("cy_expire_clean_schema", cleanSchema,
                        holder -> {
                            Double value = holder.get();
                            return value == null ? UNKNOWN : value;
                        })
                .description("되읽기가 보고 있는 스키마가 정상셋인가 — 1 정상 · 0 오염")
                .register(registry);
        // **위 넷이 어느 시점의 사실인지를 함께 낸다.** 계약이 "마지막으로 성공한 실행이
        // 남긴 몫" 이라, 오늘 만료가 실패하거나 슬롯을 건너뛰면 넷은 **어제 기준**으로
        // 정상인 0 을 낸다 — 그 0 을 지금 상태로 읽으면 이틀치 백로그가 조용히 넘어간다.
        // 이 저장소가 두 곳에 적어 둔 명제가 "0 이 NaN 보다 나쁘다" 이고, 그 위험을
        // 게이지 하나로 관측 가능하게 만든다(ExpireMetricsBackdated 가 이 값을 본다).
        gauge(registry, "cy_expire_measured_at_seconds",
                s -> s.asOf().toEpochSecond(ZoneOffset.UTC),
                "이 게이지 넷이 기준으로 삼은 만료 실행의 asOf");
    }

    /**
     * <b>되읽기가 60초마다 부른다.</b> 한때 잡의 {@code afterJob} 이 실행마다 한 번
     * 불렀는데, 그러면 값이 프로세스와 함께 죽는다(클래스 javadoc 의 계약).
     *
     * <p><b>순서를 안 따진다 — 되읽기가 계산한 것을 그대로 싣는다.</b> 한때 <i>"더 오래된
     * {@code asOf} 는 안 받는다"</i> 규칙이 있었다. 기록자가 둘이던 시절(잡 + 스케줄러)의
     * 장치인데, 하나가 된 뒤로는 <b>얼어붙는 문이 됐다</b> — 되읽기가 고르는 것은
     * <i>7일 창 안에서</i> 가장 나중에 끝난 실행이고, 창은 앞이 잘리므로 그 값이 뒤로 갈 수
     * 있다. 그때 거절은 <b>아무 통로로도 안 나갔다</b>(카운터도 로그도 없다) — 게이지 다섯이
     * 낡은 값을 든 채 세 알림이 전부 초록인 상태가 된다.
     *
     * <p>DB 가 진실이면 되읽기가 읽은 것이 곧 현재다. 순서 방어가 필요해지는 것은
     * <b>만료 손 트리거가 생기는 날</b>이고, 그때는 {@code ExpirePendingRefresher} 의
     * 정렬부터 다시 봐야 한다 — 그 자리에 적어 뒀다.
     */
    public void record(LocalDateTime asOf, PendingExpiration pending, int blockedCouponCount) {
        latest.set(new Snapshot(asOf, pending.total(), pending.blocked(),
                pending.unexplained(), blockedCouponCount));
    }


    /**
     * <b>세지 못했다는 것을 그대로 내보낸다.</b> 직전 값을 들고 있으면 관제는 그것을 지금
     * 상태로 읽는다. 0 을 내면 더 나쁘다 — <i>"밀린 것이 없다"</i> 가 되어 누락 알림이
     * 영원히 조용해진다.
     *
     * <p>부르는 갈래는 넷이다 — 성공한 실행이 창 안에 없거나, 오염 스키마를 보고 있거나,
     * 제외 목록을 못 읽었거나, 되읽기가 실패한 것이다. 그 갈래를
     * {@code ExpireMetricsUnknown} 의 runbook 이 감별한다.
     */
    public void markUnknown() {
        latest.set(null);
    }

    /**
     * <b>정상셋을 보고 있는지 알린다.</b> 오염셋일 때 나머지 다섯이 {@code NaN} 인 것은
     * 고장이 아니라 계약이고, 그 구분을 관제가 할 수 있어야 한다.
     *
     * <p><b>이 값은 {@link #markUnknown()} 이 안 지운다.</b> 스키마 모양은 프로세스 수명
     * 중에 안 바뀌므로 되읽기가 실패해도 마지막으로 확인한 사실이 그대로 맞고, 지우면
     * {@code ExpireMetricsUnknown} 의 조인이 흔들린다.
     */
    public void recordSchema(boolean cleanSchema) {
        this.cleanSchema.set(cleanSchema ? 1.0 : 0.0);
    }

    private void gauge(MeterRegistry registry, String name, ToDoubleFunction<Snapshot> field,
            String description) {
        Gauge.builder(name, latest, holder -> {
                    Snapshot snapshot = holder.get();
                    return snapshot == null ? UNKNOWN : field.applyAsDouble(snapshot);
                })
                .description(description)
                .register(registry);
    }

    /**
     * 청크 하나가 <b>끝까지 간 뒤에</b> 충전율을 기록한다.
     *
     * <p>메트릭은 롤백을 안 따라간다. 청크 중간에서 부르면 {@code STOCK_UNDERFLOW} 로 죽은
     * 청크의 표본이 남고, 재시작이 같은 구간을 다시 세서 분포가 실제보다 낙관적으로 보인다.
     *
     * <p>{@code chunkSize} 가 1 이상인 것은 {@code ExpireJobConfig} 생성자가 진다.
     *
     * @param size      이 청크가 실제로 담은 후보 수
     * @param chunkSize 담을 수 있었던 최대치
     */
    public void chunkFill(int size, int chunkSize) {
        chunkFill.record((double) size / chunkSize);
    }
}
