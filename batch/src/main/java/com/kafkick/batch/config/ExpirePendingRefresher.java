// 만료 대기 지표를 잡 실행 밖에서 주기로 되읽습니다.
package com.kafkick.batch.config;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.expiration.ExpirationRepository;
import com.kafkick.core.expiration.PendingExpiration;
import com.kafkick.core.verification.VerificationRuleRepository;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * <b>만료 대기 지표가 프로세스보다 오래 살게 한다.</b>
 *
 * <p>{@code cy_expire_*} 게이지는 {@code afterJob} 에서만 채워지는 JVM 값이었다. 만료가 5분
 * 크론일 때는 재기동 뒤 몇 분이면 값이 돌아와 그 성질이 드러나지 않았는데,
 * <b>CY-397 이 배치 창(04:10 · 일 1회)으로 옮기면서 그 공백이 최대 하루가 됐다.</b>
 * 그 사이 {@code ExpireLeavesWorkBehind}(백로그가 쌓인다)는 {@code NaN > 0} 이 거짓이라
 * <b>발화할 수 없다</b> — 재고를 되돌리는 유일한 배치가 조용히 일을 안 하는 상태를
 * 감시하려고 만든 알림이 정확히 그 구간에 눈이 먼다.
 *
 * <p>CY-392 가 마지막 성공 시각에 같은 수술을 했다 — <i>"근거는 DB 다, JVM 안 카운터가
 * 아니다"</i>({@code BatchRunMetrics} javadoc). 이 클래스가 나머지 <b>다섯</b>에 그것을 한다.
 *
 * <h2>어떤 {@code asOf} 로 재나 — 이 클래스의 전부다</h2>
 *
 * <p><b>{@code now()} 로 재면 알림의 뜻이 바뀐다.</b> 게이지는 <i>"그 실행이 처리했어야
 * 하는데 안 된 몫"</i> 이고, 그 등식은 <b>실행이 쓴 {@code asOf} 에서만</b> 성립한다.
 * 지금 시각으로 세면 낮 동안 기한을 넘긴 쿠폰이 정상적으로 쌓여 매일 critical 이 뜬다.
 * 그래서 <b>마지막으로 성공한 만료 실행의 {@code asOf}</b> 를 배치 메타에서 읽어 그 시점을
 * 다시 잰다.
 *
 * <h2>제외 목록은 다시 만들지 않는다 — 행 수는 다시 센다</h2>
 *
 * <p>{@code blockedCoupons(asOf)} 를 다시 부르면 <b>데이터를 고쳤을 때 서버 critical 이
 * 뜬다</b> — 04:10 에 재고가 어긋나 건너뛴 회차를 10:00 에 고치면 그 몫이
 * {@code blocked} 에서 {@code unexplained} 로 옮겨 간다. {@code ExpireMetrics} 가 세운
 * <i>"서버를 고칠 상황과 데이터를 볼 상황을 같은 알람으로 묶지 않는다"</i> 를 정면으로 어긴다.
 *
 * <p>게이지 정의가 답을 준다 — {@code cy_expire_blocked_pending} 은 <i>"그중 <b>이번 실행이</b>
 * 건너뛴 회차의 몫"</i> 이다. 그것은 <b>그 실행에 대한 사실</b>이지 지금 재고 상태가 아니다.
 * 그래서 <b>목록만</b> 배치 메타에서 그대로 가져온다({@code BATCH_STEP_EXECUTION_CONTEXT}).
 * <b>행 수는 그 목록을 넘겨 {@code countPending} 한 질의로 지금 다시 센다</b> —
 * {@code total} 과 {@code blocked_pending} 둘 다 그 한 번에서 나온다. 얼리는 것은
 * <b>어느 회차를 뺄지</b>이지 <b>몇 건인지</b>가 아니다.
 *
 * <p>건너뛴 회차가 다음 창까지 안 만료되는 것은 남는다. 그쪽은 설계대로
 * {@code ExpireSkippingBrokenCoupons}(channel: data)의 몫이고, 이 되읽기 덕에 그 알림도
 * 재기동을 넘어 살아남는다.
 *
 * <h2>⚠️ 다시 세는 쪽에 비대칭이 남는다 — 취소·사용 경로가 붙는 날 터진다</h2>
 *
 * <p>{@code COUNT_PENDING} 의 술어는 {@code status='ISSUED' AND expires_at < :asOf} 뿐이다.
 * 그래서 실행이 끝난 <b>뒤에</b> {@code CANCEL_USE}({@code USED → ISSUED})로 되돌아온 행이
 * 새로 세어진다. 그 회차는 얼린 목록에 없으니 {@code blocked} 가 아니라
 * <b>{@code unexplained}</b> 로 들어가고, {@code ExpireLeavesWorkBehind}(critical ·
 * channel: server)가 뜬다 — <b>배치는 아무것도 안 틀렸는데 서버를 보라고 나간다.</b>
 * 만료가 일 1회라 다음 창까지 <b>최대 하루</b> 간다.
 *
 * <p>바로 위에서 목록을 얼려 막은 사고와 <b>같은 모양이 다른 문으로</b> 들어오는 것이다.
 * {@code afterJob} 이 잡 종료 시점에 한 번 세던 시절에는 구조적으로 불가능했고,
 * <b>이 클래스가 그 문을 열었다.</b>
 *
 * <p>지금 도달 불가인 이유는 하나다 — {@code issuances} 의 상태를 쓰는 문장이
 * {@code EXPIRE_BATCH} 하나뿐이다({@code CouponStateMachine} 은 전이표에 이미
 * {@code CANCEL_USE} 를 갖고 있다). <b>취소·사용 티켓이 이 자리를 함께 봐야 한다.</b>
 *
 * <p><b>다만 고치려면 잡이 먼저 무언가를 남겨야 한다.</b> 맞추려는 창은
 * {@code EXPIRE_BATCH} 가 쓰는 {@code updated_at <= :committedAt} 인데, 그
 * {@code committedAt} 은 <b>청크마다 새로 잡히고 어디에도 영속되지 않는다</b>
 * ({@code ExpireJobConfig} 이 청크마다 {@code TimeProvider#now} 로 잡고 Step 문맥에 안 싣는다).
 * 되읽기가 그 값을 알 방법이 지금 없으므로, <b>마지막 청크의 {@code committedAt} 을
 * Step 문맥에 싣는 것이 선행</b>이다({@link ExpireStepContext} 에 키를 하나 더 판다).
 * {@code docs/13} 이 그 트리거로 미뤄 뒀다.
 *
 * <h2>비용</h2>
 *
 * <p>{@code ExpireMetrics} 가 <i>"스크레이프 때 세지 않는다 — 300만에 {@code COUNT(*)} 를
 * 15초마다 때리는 꼴"</i> 이라고 적었다. 그 걱정은 <b>{@code idx_issuance_status_expires}
 * 가 생긴 뒤로는 과하다</b> — 실측하면 <b>전체 행이 아니라 대기 건수에 비례</b>한다.
 *
 * <pre>
 * 발급 40만 · 대기 800건 — 되읽기 한 주기가 내는 문장:
 *   hasCleanOnlyConstraints  information_schema.statistics EXISTS
 *   lastCompletedExpire      3테이블 조인 + LIMIT 1                          1ms
 *   getJobExecution(id)      Spring Batch 가 5~6문장(인스턴스 · 파라미터 ·
 *                            Step · 실행 문맥 blob 둘)
 *   COUNT_PENDING            Index range scan (status, expires_at) rows=800  1ms
 * </pre>
 *
 * <p><b>여덟 남짓이지 하나가 아니다.</b> 한때 이 자리에 <i>"한 문장"</i> 이라고 적혀
 * 있었는데, 그 숫자 위에 주기 하한(10초) 가드가 서 있었다 — 근거가 한 자릿수 틀리면
 * 그 가드도 틀린다. 무거운 쪽은 질의가 아니라 <b>실행 문맥 blob 역직렬화</b>다.
 *
 * <p><b>{@code BLOCKED_COUPONS} 는 여기서 안 돈다</b> — 그 질의는 잡의 태스클릿만 부르고,
 * 되읽기는 그 결과를 배치 메타에서 읽는다. 위 표가 한때 그 줄을 갖고 있던 것은 되읽기가
 * 목록을 다시 만들던 설계의 잔재다.
 *
 * <p>훑는 범위는 <b>"성공한 실행이 못 걷은 몫"</b> 이지 하루치가 아니다 — 실행이 성공하면
 * 그 컷의 대기는 막힌 회차의 몫만 남는다. 위 800건은 <b>실행 직전</b>의 최악을 재려고
 * 심은 값이다. 그래서 주기를 다른 되읽기와 같은 60초로 둔다 — 스크레이프 경로가 아니라
 * 여기서 도는 것이 요지다.
 */
@Component
public class ExpirePendingRefresher {

    private static final Logger log = LoggerFactory.getLogger(ExpirePendingRefresher.class);

    /**
     * 하한은 배치 메타 왕복이 초당 한 번에 가까워지는 것을 막고, 상한은 스크레이프 흔들림을
     * 흡수할 만큼 자주 돌게 한다. 형제 되읽기와 같은 폭이다.
     */
    private static final long MIN_REFRESH_MILLIS = 10_000;

    private static final long MAX_REFRESH_MILLIS = 120_000;

    /** 60주기 = 최소 폭(10초)에서도 10분에 한 줄. 첫 회는 {@code 0 % n == 0} 이라 늘 남는다. */
    private static final int LOG_EVERY_N_MISSES = 60;

    private final JdbcClient jdbcClient;
    private final JobRepository jobRepository;
    private final ExpirationRepository expirations;
    private final ExpireMetrics metrics;

    /**
     * <b>오염 스키마의 대기 건수는 이 게이지의 뜻이 아니다.</b> 잡은 {@code CleanSchemaGuard}
     * 가 {@code beforeJob} 에서 막는데, 되읽기는 잡 밖이라 그 가드를 안 지난다 —
     * 같은 근거({@code hasCleanOnlyConstraints})를 여기서도 본다. 두 판정이 갈리면 잡은
     * 거절되는데 지표만 오염셋을 세는 상태가 열린다.
     *
     * <p>지금 그 구멍이 안 열리는 유일한 이유는 시드가 만든 CORRUPT DB 에 {@code BATCH_*}
     * 가 비어 있어서다 — <b>우연이지 방어가 아니다.</b> CLEAN 을 덤프해 제약만 떼는 방식으로
     * CORRUPT 를 만들면 배치 메타가 따라오고, 그때 오염셋의 밀린 대기 수만 건이
     * {@code ExpireLeavesWorkBehind} critical 로 나간다. {@code docs/contract.json} 의
     * {@code not_verified} 가 <i>"만료 누락은 사건이 아니다"</i> 라고 적은 바로 그 항목이다.
     */
    private final VerificationRuleRepository rules;

    /**
     * 되읽기 실패를 <b>지표로도</b> 낸다. 로그만 남기면 그 실패가 게이지의 {@code NaN} 과
     * 구별되지 않는다 — {@code BatchRunMetricsRefresher} 가 같은 이유로 같은 모양을 쓴다.
     */
    private final Counter refreshFailures;

    private final AtomicLong failures = new AtomicLong();

    /**
     * <b>제외 목록 미독은 실패가 아니라 지속 상태다.</b> {@link #failures} 를 안 올리는 대신
     * 이것으로 로그를 억제한다 — 카운터를 올리면 {@code ExpireMetricsUnknown} 의 runbook 이
     * 쓰는 갈래 감별(<i>"안 증가했으면 제외 목록을 못 읽은 것"</i>)이 거짓이 된다.
     */
    private final AtomicLong contextMisses = new AtomicLong();

    /**
     * <b>스트릭은 실행 하나에 대한 것이다.</b> 여기 안 묶으면 어제 실행의 카운터가 이어져
     * 오늘 실행의 첫 미독이 <b>로그를 안 낸다</b> — 그동안 최신 WARN 은 어제 id 를 가리키고,
     * {@code ExpireMetricsUnknown} 의 runbook 은 그 id 를 {@code BATCH_JOB_EXECUTION} 과
     * 대조하라고 시킨다. 로그와 DB 가 서로 다른 실행을 말하는 상태가 된다.
     */
    private final AtomicLong contextMissExecutionId = new AtomicLong(-1);

    /**
     * 스케줄러 스레드에서 도는 조회라 끊을 수단이 없으면 커넥션 풀이 마른 날 다음 주기도
     * 안 돈다. 읽기 전용으로 열어 데드라인을 심는다.
     */
    private final TransactionTemplate readPending;

    public ExpirePendingRefresher(JdbcClient jdbcClient, JobRepository jobRepository,
            ExpirationRepository expirations, ExpireMetrics metrics,
            VerificationRuleRepository rules, MeterRegistry registry,
            PlatformTransactionManager transactionManager,
            @Value("${batch.metrics.expire-pending-refresh-ms:60000}") long refreshMillis,
            @Value("${batch.metrics.expire-pending-timeout-ms:5000}") long timeoutMillis) {
        // **알림의 for 예산이 이 주기의 배수로 잡혀 있다** — ExpireLeavesWorkBehind(10분) ·
        // ExpireMetricsUnknown(15분). 늘리면 그 예산이 한 주기도 못 채워 구조적으로 못 뜨거나
        // 스크레이프 흔들림에 오탐이 난다. 줄이면 대기 집계가 그만큼 자주 issuances 를 친다
        // (비용 실측은 60초를 전제로 했다). 형제 되읽기 둘이 같은 이유로 같은 가드를 갖는다.
        if (refreshMillis < MIN_REFRESH_MILLIS || refreshMillis > MAX_REFRESH_MILLIS) {
            throw new IllegalArgumentException(
                    "batch.metrics.expire-pending-refresh-ms 는 " + MIN_REFRESH_MILLIS + "~"
                            + MAX_REFRESH_MILLIS + " 이어야 합니다. ExpireLeavesWorkBehind"
                            + "(for 10m)와 ExpireMetricsUnknown(for 15m)의 예산이 이 주기의 "
                            + "배수로 잡혀 있습니다. 받은 값=" + refreshMillis);
        }
        // 스프링 트랜잭션 타임아웃이 초 단위라 999 이하는 0 으로 잘리는데, 0 은 "무제한" 이
        // 아니라 데드라인이 이미 지났음이다. 형제와 같은 가드다.
        if (timeoutMillis < 1_000 || timeoutMillis % 1_000 != 0) {
            throw new IllegalArgumentException(
                    "batch.metrics.expire-pending-timeout-ms 는 1000 이상이면서 1000 의 "
                            + "배수여야 합니다. 받은 값=" + timeoutMillis);
        }
        this.jdbcClient = jdbcClient;
        this.jobRepository = jobRepository;
        this.expirations = expirations;
        this.metrics = metrics;
        this.rules = rules;
        this.refreshFailures = Counter.builder("cy_expire_refresh_failures_total")
                .description("만료 대기 지표 되읽기가 실패한 횟수")
                .register(registry);
        this.readPending = new TransactionTemplate(transactionManager);
        this.readPending.setReadOnly(true);
        // 배치 메타를 읽는 쪽이라 READ COMMITTED 로 충분하고, 지워진 관측 트랜잭션이
        // 같은 격리를 갖고 있었다. 기본값(DB 의 REPEATABLE READ)이면 60초마다 스냅샷을
        // 새로 잡느라 언두를 더 오래 붙든다.
        this.readPending.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.readPending.setTimeout(Math.toIntExact(timeoutMillis / 1_000));
    }

    /**
     * <b>주기는 다른 되읽기와 같은 키를 안 쓴다.</b> 이쪽은 원본 테이블을 읽고 그쪽은 배치
     * 메타를 읽어, 무거워지는 이유가 다르다 — 한 손잡이로 묶으면 한쪽 때문에 다른 쪽을 조인다.
     */
    // **첫 발화를 미룰 수 있게 둔다.** @EnableScheduling 은 모든 테스트 컨텍스트에서
    // 살아 있고 batch.scheduling.enabled 는 스케줄러 셋(만료·정리·회차 전이)만 끈다 — 그래서 이 되읽기는
    // 테스트가 손으로 부르는 refresh() 와 **같은 빈에서 경합한다.** 실제로
    // ExpirationRepository 를 프록시로 감싸 호출을 기록하는 테스트들이 그 틱 하나에
    // 재현 불가로 빨개질 수 있었다. 주기를 늘리는 것은 확률만 줄이므로, 테스트는
    // 지연을 실행 시간보다 길게 줘 **창을 닫는다.** 빈을 조건부로 끄지 않는 이유는
    // 노출 테스트가 그 빈을 손으로 불러야 하기 때문이다.
    @Scheduled(fixedDelayString = "${batch.metrics.expire-pending-refresh-ms:60000}",
            initialDelayString = "${batch.metrics.expire-pending-initial-delay-ms:0}")
    public void refresh() {
        try {
            // **세 단계를 한 트랜잭션에 넣는다.** 트랜잭션 밖이면 DataSourceUtils 가
            // queryTimeout 을 안 붙여 **끊을 수단이 없다** — 배치 메타에 긴 락이 걸리면
            // 스케줄러 스레드가 무기한 블록되고, fixedDelay 라 다음 주기도 안 뜬다.
            // 그때 markFailed 에 도달하지 못해 게이지가 NaN 이 아니라 **낡은 값으로
            // 얼어붙고**, NaN 을 보는 알림도 실패 카운터를 보는 알림도 그것을 못 본다.
            // BatchRunMetricsRefresher 가 같은 이유로 루프 전체를 감싼다.
            //
            // ⚠️ **셋이 한 스냅샷을 보지는 않는다.** READ COMMITTED 라 문장마다 스냅샷이
            // 갱신된다. 여기서 트랜잭션이 하는 일은 일관성이 아니라 **데드라인**이다
            // (DataSourceUtils.applyTimeout 이 그 트랜잭션의 초 단위 상한을 심는다).
            // 어긋난 샘플은 다음 주기(60초)가 덮는다.
            Snapshot fresh = readPending.execute(ignored -> {
                boolean cleanSchema = rules.hasCleanOnlyConstraints();
                metrics.recordSchema(cleanSchema);
                if (!cleanSchema) {
                    // 오염셋을 보고 있다. 그 대기 건수는 이 게이지의 뜻이 아니다.
                    // 그 사실을 지표로도 내보내 ExpireMetricsUnknown 이 이 갈래를 뺀다 —
                    // 오염셋 기동은 docs/14 의 "검증용 셋에 Spring Batch 메타 테이블이
                    // 없다" 절이 적은 정상 절차라, 고장으로 읽으면 시연 내내 warning 이
                    // 하나 켜져 있게 된다.
                    return null;
                }
                Optional<JobExecution> lastRun = lastCompletedExpire();
                if (lastRun.isEmpty()) {
                    // 한 번도 성공한 적이 없다. 0 을 내면 "밀린 것이 없다" 가 되어 누락
                    // 알림이 영원히 조용해진다 — 아직 모르는 것과 없는 것은 다르다.
                    return null;
                }

                JobExecution execution = lastRun.get();
                LocalDateTime asOf = execution.getJobParameters().getLocalDateTime("asOf");
                if (asOf == null) {
                    // SQL 이 PARAMETER_NAME·PARAMETER_TYPE 으로 조인하므로 도달하지 않는다.
                    throw new IllegalStateException("COMPLETED 실행 " + execution.getId()
                            + " 의 asOf 파라미터를 못 읽었습니다");
                }

                // 제외 목록을 못 읽었다는 것과 목록이 비었다는 것은 다르다. 앞은 "판정할
                // 재료가 없다" 이고, 그것을 "막힌 회차가 없다" 로 읽으면 남은 대기가 전부
                // "배치가 처리했어야 하는 몫" 으로 나가 서버를 보라는 알림이 뜬다.
                Optional<List<Long>> blocked = ExpireStepContext.blockedFrom(execution);
                if (blocked.isEmpty()) {
                    // **되읽기 실패로 세지 않는다.** 이것은 다음 만료 창까지 안 변하는
                    // **지속 상태**라, 카운터로 올리면 하루 1,440 이 쌓여 ExpireMetricsStale
                    // 이 뜨고 그 runbook 은 커넥션 풀을 가리킨다 — 원인은 배치 메타의 Step
                    // 문맥이다. ExpireMetricsUnknown 의 갈래 감별("카운터가 안 올랐으면
                    // 제외 목록을 못 읽은 것")이 성립하려면 여기서 안 올려야 한다.
                    //
                    // 같은 이유로 **로그도 매 주기 안 찍는다.** 지속 상태라 다음 04:10 까지
                    // 최대 1,440줄이 쌓이는데, 그것이 ExpireNotSucceeding 의 runbook 이
                    // 감별 수단으로 쓰는 "만료 슬롯을 건너뜁니다" WARN 을 밀어낸다.
                    // markFailed 가 스택트레이스에 대해 이미 같은 판단을 했다.
                    // 실행이 바뀌면 스트릭을 처음부터 센다 — 그 실행의 첫 줄이 나가야
                    // 로그의 id 와 DB 의 최신 COMPLETED 가 같은 것을 가리킨다.
                    boolean sameRun = contextMissExecutionId
                            .getAndSet(execution.getId()) == execution.getId();
                    long miss = sameRun ? contextMisses.getAndIncrement() : 0;
                    if (!sameRun) {
                        contextMisses.set(1);
                    }
                    if (miss % LOG_EVERY_N_MISSES == 0) {
                        log.warn("마지막으로 성공한 만료 실행({})의 제외 목록을 Step 문맥에서 "
                                + "못 읽었습니다. 게이지를 NaN 으로 둡니다. 연속={}",
                                execution.getId(), miss + 1);
                    }
                    return null;
                }

                // 창은 못 읽어도 판정을 포기하지 않는다 — 근거는 ExpireStepContext
                // #maxHistoryIdFrom 에 있다(이 키는 CY-768 이 새로 만든 것이라 배포 직후
                // 마지막 실행에는 반드시 없다).
                Long maxHistoryId =
                        ExpireStepContext.maxHistoryIdFrom(execution).orElse(null);

                return new Snapshot(
                        asOf,
                        expirations.countPending(asOf, maxHistoryId, blocked.get()),
                        blocked.get().size());
            });

            if (fresh == null) {
                metrics.markUnknown();
                failures.set(0);
                return;
            }
            metrics.record(fresh.asOf(), fresh.pending(), fresh.blockedCoupons());
            failures.set(0);
            contextMisses.set(0);
            contextMissExecutionId.set(-1);
        } catch (RuntimeException e) {
            markFailed(e);
        } catch (Error e) {
            // 게이지는 떨어뜨리되 삼키지 않는다. 근거는 BatchRunMetricsRefresher 에 적었다.
            markFailed(e);
            throw e;
        }
    }

    /**
     * 한 트랜잭션이 만든 한 덩어리. 다섯을 따로 내보내면 그 사이에 스크레이프가 끼어
     * <b>서로 다른 실행의 값이 섞인 샘플</b>이 나온다 — {@code ExpireMetrics} 가 같은 이유로
     * 스냅샷 하나를 들고 있다.
     */
    private record Snapshot(LocalDateTime asOf, PendingExpiration pending, int blockedCoupons) {
    }

    /**
     * <b>가장 나중에 끝난 성공 실행.</b> {@code END_TIME} 으로 고른다 — 창 조건과 같은
     * 컬럼이라 {@code V14} 의 {@code (STATUS, END_TIME)} 인덱스에 정렬까지 얹힌다.
     * <b>추정이 아니라 실측이다</b> — 실행 26,000 / 7일 창 안 2,016 을 심고 EXPLAIN ANALYZE
     * 를 떴다({@code V14} 가 쓴 것과 같은 방식):
     *
     * <pre>
     * -&gt; Limit: 1 row(s)                                    (actual time=0.298..0.298 rows=1)
     *   -&gt; Index range scan on e using IX_JOB_EXEC_STATUS_END (reverse)
     *      (cost=911 rows=2023)                              (actual rows=1)
     * </pre>
     *
     * <b>filesort 가 안 붙는다.</b> InnoDB 보조 인덱스는 PK 를 뒤에 붙이므로 이 인덱스가
     * 사실상 {@code (STATUS, END_TIME, JOB_EXECUTION_ID)} 이고, 역방향 스캔이
     * {@code ORDER BY END_TIME DESC, JOB_EXECUTION_ID DESC} 를 <b>그대로</b> 만족한다 —
     * 타이브레이커를 붙여도 정렬 비용이 안 생기는 이유다. 추정 {@code rows=2023}(창 전체)
     * 인데 실제로 읽은 것이 1행인 것은 {@code LIMIT 1} 이 첫 일치에서 끊기 때문이다.
     *
     * <p><b>픽스처는 만료 단일 잡이었다.</b> 세 잡이 섞이면(만료 04:10 → 정리 04:30) 가장
     * 나중에 끝난 {@code COMPLETED} 가 만료가 아니라 정리라, 스캔이 몇 행을 버리고
     * 지나간다 — 재현에서 3행이었다. 창 안 행 수가 그대로라 비용은 같다.
     *
     * <p><b>{@code asOf} 로 정렬하지 않는다.</b> 한때 그렇게 했고 근거는 <i>"밀린 만료를
     * 따라잡으려고 과거 {@code asOf} 로 손 트리거를 치는 것이 권장 절차"</i> 였는데,
     * <b>만료를 손으로 띄우는 경로가 이 저장소에 없다</b>(컨트롤러는 검증용 하나뿐이고
     * {@code spring.batch.job.enabled} 도 false 다). 없는 시나리오 때문에 문자열 정렬과
     * {@code PARAMETER_TYPE} 전제를 지고 있었다.
     *
     * <p>⚠️ <b>그 트리거가 생기는 티켓은 이 자리를 다시 봐야 한다.</b> 과거 {@code asOf}
     * 실행이 나중에 끝나면 {@code END_TIME} 정렬이 그것을 집고, 게이지가 <b>더 좁은 창의
     * 더 작은 값</b>을 낸다 — 관제는 그것을 <i>"밀린 것이 없다"</i> 로 읽는다.
     * 크론만 있는 지금은 {@code asOf} 와 {@code END_TIME} 의 순서가 같아 그 창이 없다.
     *
     * <p>파라미터 조인은 남는다 — 정렬이 아니라 {@code asOf} 를 <b>읽기 위해서</b>다.
     * {@code PARAMETER_TYPE} 을 함께 거는 것은 타입이 다르면 {@code getLocalDateTime} 이
     * 던지고 그 예외가 바깥 {@code catch} 로 흘러 원인이 안 보이기 때문이다.
     *
     * <p>id 만 SQL 로 고르고 나머지는 {@code JobRepository} 가 읽는다 — 실행 문맥은
     * 직렬화된 blob 이라 SQL 로 파싱하면 그 포맷이 두 벌이 된다.
     *
     * <p>창(7일)은 이 조회가 배치 메타 이력 전체에 비례해 자라는 것을 막는다.
     *
     * <p>⚠️ <b>이 창의 안쪽은 {@code cleanupJob} 이 지킨다</b> —
     * {@code batch.cleanup.metadata-keep-days} 의 하한이 {@link BatchMetadataWindow#LOOKBACK_DAYS}
     * (7)보다 커야 하고(최소 8) 기동 때 거절한다. <b>이 숫자를 바꾸면 그 상수도 함께 고쳐라</b> — 창이 커지면
     * 보존 기간이 그것을 못 덮어 게이지가 통째로 {@code NaN} 이 된다.
     */
    private Optional<JobExecution> lastCompletedExpire() {
        Optional<Long> executionId = jdbcClient.sql("""
                        SELECT e.JOB_EXECUTION_ID
                          FROM BATCH_JOB_EXECUTION e
                          JOIN BATCH_JOB_INSTANCE i ON i.JOB_INSTANCE_ID = e.JOB_INSTANCE_ID
                          JOIN BATCH_JOB_EXECUTION_PARAMS p
                            ON p.JOB_EXECUTION_ID = e.JOB_EXECUTION_ID
                           AND p.PARAMETER_NAME = 'asOf'
                           AND p.PARAMETER_TYPE = 'java.time.LocalDateTime'
                         WHERE e.STATUS = 'COMPLETED'
                           AND e.END_TIME > DATE_SUB(NOW(), INTERVAL %d DAY)
                           AND i.JOB_NAME = :jobName
                         ORDER BY e.END_TIME DESC, e.JOB_EXECUTION_ID DESC
                         LIMIT 1
                        """.formatted(BatchMetadataWindow.LOOKBACK_DAYS))
                .param("jobName", ExpireStepContext.JOB_NAME)
                .query(Long.class)
                .optional();
        return executionId.map(jobRepository::getJobExecution);
    }

    /**
     * <b>같은 원인이 이어지면 스택트레이스를 매번 남기지 않는다.</b> 60초 주기라 DB 가 30분
     * 잠기면 트레이스가 서른 개 쌓이고, 그것이 알림 runbook 이 가리키는 WARN 을 밀어낸다.
     */
    private void markFailed(Throwable cause) {
        metrics.markUnknown();
        refreshFailures.increment();
        long streak = failures.incrementAndGet();
        if (streak == 1) {
            log.warn("만료 대기 지표를 되읽지 못했습니다. 게이지를 NaN 으로 둡니다.", cause);
        } else {
            log.warn("만료 대기 지표를 계속 되읽지 못하고 있습니다. 연속={} 원인={}",
                    streak, cause.toString());
        }
    }
}
