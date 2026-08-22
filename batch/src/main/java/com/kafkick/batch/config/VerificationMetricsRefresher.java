// 검증 판정을 주기적으로 되읽어 지표에 싣습니다.
package com.kafkick.batch.config;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationRunRepository;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * <b>판정을 잡이 밀어 넣지 않고 여기서 되읽는다.</b>
 *
 * <p>만료 지표는 {@code afterJob} 에서 채운다 — 5분 크론이라 재시작해도 곧 복구되기
 * 때문이다. 검증은 다르다. <b>사람이 손으로, 드물게</b> 돌리므로 프로세스 게이지로 두면
 * 컨테이너를 재배포하는 순간 판정이 사라지는데 {@code verification_runs} 에는 남아 있다 —
 * <b>관제와 진실이 갈린다.</b> 금요일 {@code FAIL} 이 주말 재시작으로 없어지고 월요일에
 * 아무도 그것이 있었다는 것을 모르는 모양이다.
 *
 * <p>되읽으면 재시작해도 한 주기면 복구되고, {@code NaN} 은 <i>"그 조합으로 닫힌 실행이
 * 아예 없다"</i> 라는 정확한 뜻이 된다.
 *
 * <p><b>잡의 성패로 판정을 대신할 수 없다.</b> Step 체인이
 * {@code finalizeRunStep → statsAggregateStep} 이라, 통계 Step 이 죽으면 <b>잡은
 * {@code FAILED} 인데 판정은 이미 커밋돼 있다.</b> 반대로 {@code verdict = FAIL} 인 실행은
 * 잡이 {@code COMPLETED} 다. 두 축이 서로 독립이므로 지표는 <b>판정 자체</b>를 본다.
 *
 * <p><b>{@code batch.scheduling.enabled} 에 묶지 않는다.</b> 그 스위치는 <i>원본을 쓰는
 * 잡을 돌릴 것인가</i> 를 정하는데, 이것은 읽기만 한다. 묶으면 스케줄러를 끈 채
 * {@code verifyJob} 을 돌리는 <b>정상 운영 절차</b>에서 지표가 통째로 죽는다 —
 * 그 절차는 {@code rejectRunningSchedulers} 가 강제하는 것이라 예외가 아니라 기본이다.
 */
@Component
public class VerificationMetricsRefresher {

    private static final Logger log = LoggerFactory.getLogger(VerificationMetricsRefresher.class);

    /**
     * <b>{@code FULL} 만 본다.</b> {@code INCREMENTAL} 은 {@code rejectUnsupportedScope} 가
     * 시작 전에 거부해 닫힌 실행이 생길 수 없다. 조회해도 언제나 빈 값이라 질의만 는다.
     */
    private static final ScopeType SCOPE = ScopeType.FULL;

    private final VerificationRunRepository runs;
    private final VerificationMetrics metrics;

    /**
     * <b>되읽기 실패를 세어 내보낸다.</b> 실패해도 값을 안 지우기로 했으므로, 그것만으로는
     * <i>"판정이 낡고 있다"</i> 를 아무도 모른다. 이 카운터가 그 축을 진다.
     *
     * <p>이름에 {@code _total} 을 붙인다 — 이쪽은 <b>카운터가 맞다.</b> 게이지에 붙였다가
     * 렌더러가 떼어 내는 함정과는 반대 경우다.
     */
    private final Counter refreshFailures;

    /** 연속 실패 횟수. 로그가 스택트레이스로 덮이는 것을 막는 데만 쓴다. */
    private final AtomicLong failures = new AtomicLong();

    /**
     * {@code VerificationMetricsStale} 의 창(10분)에서 증분 3 을 채우려면 최소 세 번은
     * 시도해야 한다. 2분이면 창 안에 다섯 번이라 여유가 있다.
     */
    private static final long MAX_REFRESH_MILLIS = 120_000;

    /**
     * <b>되읽기에 데드라인을 준다.</b> 이 조회는 스케줄러 스레드에서 트랜잭션 밖으로 도는데,
     * 그러면 {@code DataSourceUtils} 가 {@code queryTimeout} 을 안 붙여 <b>끊을 수단이
     * 없다.</b> 커넥션 풀이 마르거나 테이블이 잠긴 날 이 스레드가 붙잡히면 다음 주기도
     * 안 돈다 — 그때 지표는 낡은 값을 든 채 조용하다.
     *
     * <p>읽기 전용이고, 터지면 {@code catch} 가 잡아 <i>"모름"</i> 으로 떨어진다.
     */
    private final TransactionTemplate readLatest;

    public VerificationMetricsRefresher(VerificationRunRepository runs, VerificationMetrics metrics,
            MeterRegistry registry, PlatformTransactionManager transactionManager,
            @Value("${batch.verify.metrics-timeout-ms:5000}") long timeoutMillis,
            @Value("${batch.verify.metrics-refresh-ms:60000}") long refreshMillis) {
        // 주기가 알림 창을 넘으면 VerificationMetricsStale 이 영영 안 뜬다.
        //
        // 그 규칙은 10분 창에서 실패 증분 3 을 본다. 60초 주기면 창 안에 열 번 시도하므로
        // 3 은 "세 번 연속 실패" 를 넘긴 것인데, 주기를 5분으로 올리면 시도가 두 번뿐이라
        // **되읽기가 100% 실패해도 3 을 못 채운다.** 그리고 그 상태는 규칙 본문이 적었듯
        // 다른 어떤 알림에도 안 걸린다 — 게이지가 낡은 PASS 를 계속 들고 있어서다.
        //
        // 여기서 막는다. .example 값만 보는 테스트로는 운영에서 환경변수로 올리는 것을
        // 못 잡는다 — 그게 실제로 이 값이 커지는 경로다.
        if (refreshMillis > MAX_REFRESH_MILLIS) {
            throw new IllegalArgumentException(
                    "batch.verify.metrics-refresh-ms 는 " + MAX_REFRESH_MILLIS + " 이하여야 합니다. "
                            + "그보다 길면 VerificationMetricsStale(10분 창, 증분 3)이 발화 조건을 "
                            + "채울 수 없어 되읽기가 계속 실패해도 아무 알림도 안 뜹니다. "
                            + "받은 값=" + refreshMillis);
        }
        if (timeoutMillis < 1_000 || timeoutMillis % 1_000 != 0) {
            throw new IllegalArgumentException(
                    "batch.verify.metrics-timeout-ms 는 1000 이상이면서 1000 의 배수여야 합니다. "
                            + "스프링의 트랜잭션 타임아웃이 초 단위라 나머지가 조용히 버려집니다. "
                            + "받은 값=" + timeoutMillis);
        }
        this.runs = runs;
        this.metrics = metrics;
        this.refreshFailures = Counter.builder("cy_verification_refresh_failures_total")
                .description("검증 판정 되읽기가 실패한 횟수")
                .register(registry);
        this.readLatest = new TransactionTemplate(transactionManager);
        this.readLatest.setReadOnly(true);
        this.readLatest.setTimeout(Math.toIntExact(timeoutMillis / 1_000));
    }

    /**
     * <b>"닫힌 실행이 없다" 와 "읽기가 실패했다" 를 가른다.</b> 앞만 <i>모름</i>이다.
     *
     * <p>검증 판정은 <b>새 실행이 닫히기 전까지 안 변한다</b> — 되읽기가 실패해도 마지막으로
     * 읽은 판정은 여전히 참이다. 그것을 {@code NaN} 으로 덮으면 정확도가 올라가는 것이
     * 아니라 내려간다. 그리고 값이 {@code 1 → NaN → 1} 로 튀면 두 알림의 {@code for}
     * 타이머가 매번 리셋돼, <b>DB 에 {@code FAIL} 이 앉아 있는데 둘 다 조용하다.</b>
     * 60초 주기이므로 5분에 한 번만 실패해도 {@code for: 5m} 는 영원히 안 찬다.
     *
     * <p><b>실패를 삼키는 것이 스케줄러를 살리려는 것은 아니다.</b> Spring 은 반복 태스크의
     * 예외를 흡수하고 다음 주기를 다시 잡는다. 여기서 잡는 이유는 <b>낡은 값을 지우지
     * 않기 위해서</b>이고, 얼마나 낡았는지는 {@code cy_verification_refresh_failures_total}
     * 이 진다.
     */
    @Scheduled(fixedDelayString = "${batch.verify.metrics-refresh-ms:60000}")
    public void refresh() {
        DatasetType dataset = metrics.served();
        try {
            readLatest.execute(status -> runs.findLatestClosed(dataset, SCOPE))
                    .ifPresentOrElse(metrics::record, metrics::markUnknown);
            failures.set(0);
        } catch (RuntimeException e) {
            recordFailure(dataset, e);
        } catch (Error e) {
            // Error 도 카운터는 올린다. 안 올리면 이 알림이 Error 경로에서만 통째로 꺼진다.
            //
            // 게이지는 어느 쪽이든 낡은 값을 그대로 든다 — markUnknown() 은 "닫힌 실행이
            // 없다" 분기에서만 불리고 예외 경로에서는 아무도 안 건드린다. 그러니 밖으로
            // 던지는 것만으로는 "관제에 정상으로 보이는 상태" 가 없어지지 않는다.
            // 없애는 것은 카운터고, 그것을 VerificationMetricsStale 이 본다.
            //
            // 그러고 나서 다시 던진다. Spring 은 그 실행만 흡수하고 다음 주기를 잡으므로
            // 되읽기 동작은 같고, 스택트레이스가 ERROR 로 남아 원인이 보인다.
            refreshFailures.increment();
            failures.incrementAndGet();
            throw e;
        }
    }

    /**
     * 같은 원인이 이어지면 스택트레이스를 매번 남기지 않는다 — 시연 로그가 덮인다.
     */
    private void recordFailure(DatasetType dataset, RuntimeException e) {
        long streak = failures.incrementAndGet();
        refreshFailures.increment();
        if (streak == 1) {
            log.warn("검증 판정을 되읽지 못했습니다. 직전 값을 유지합니다. dataset={}",
                    dataset, e);
        } else {
            log.warn("검증 판정을 계속 되읽지 못하고 있습니다. dataset={} 연속={} 원인={}",
                    dataset, streak, e.toString());
        }
    }

}
