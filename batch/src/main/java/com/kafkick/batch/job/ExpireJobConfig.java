// 기한이 지난 발급건을 만료로 넘기는 잡의 배선입니다.
package com.kafkick.batch.job;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.DefaultJobParametersValidator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;

import com.kafkick.batch.config.BinlogFormatGuard;
import com.kafkick.batch.config.CleanSchemaGuard;
import com.kafkick.batch.config.ExpireMetrics;
import com.kafkick.batch.config.ExpireStepContext;
import com.kafkick.batch.schedule.CronSlot;
import com.kafkick.core.expiration.ExpirationRepository;
import com.kafkick.core.expiration.ExpireChunk;
import com.kafkick.core.expiration.exception.ExpirationErrorCode;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

/**
 * <b>재고를 쓰는 유일한 잡이다.</b> 나머지 배치는 원본을 읽기만 한다.
 *
 * <p><b>Spring Batch 를 쓰는 이유는 실행 이력과 파라미터 규율이다.</b> 언제 돌았고 몇 건을
 * 넘겼는지가 남고, {@code asOf} 없이는 시작하지 못하며, 같은 파라미터의 완료된 실행을 다시
 * 돌리지 못한다. 5분마다 도는 잡이라 그 기록이 곧 운영 근거다.
 *
 * <p><b>재스캔 비용은 이유가 아니다.</b> {@code afterId} 는 <b>JobInstance 안에서만</b> 산다.
 * 스케줄러는 주기마다 {@code asOf} 를 새로 잡아 매번 다른 인스턴스를 만들므로, 5분마다 오는
 * 실행은 언제나 {@code id > 0} 부터 훑는다. 이어받는 것은 <b>같은 {@code asOf} 를 다시 도는
 * 경우</b>, 즉 재시작뿐이고 그때는 커밋된 데까지만 건너뛴다
 * — {@code ExpireJobRestartTest} 가 두 방향을 다 재 뒀다.
 *
 * <p><b>정합성은 진도가 아니라 SQL 이 지킨다.</b> 이중 차감을 막는 것은
 * {@code EXPIRE_BATCH} 의 {@code status = 'ISSUED'} 조건이다 — 이미 넘어간 행은 진도를
 * 0 으로 되돌려 다시 훑어도 매치되지 않는다. 그 조건이 이 잡의 멱등성이고,
 * <b>진도는 최적화다.</b> 둘을 섞어 두면 나중에 그 조건을 부차적인 것으로 오해한다.
 *
 * <p><b>청크 하나가 곧 태스클릿 한 번이다.</b> {@code RepeatStatus.CONTINUABLE} 로 돌아오면
 * Spring Batch 가 <b>새 트랜잭션</b>에서 다시 부른다. 그래서 앞 청크는 커밋된 채로 남고,
 * 죽어도 거기까지는 살아 있다.
 *
 * <p><b>끝내는 신호는 후보 0건이다 — 넘어간 건수가 아니다.</b> 한때는 {@code UPDATE} 가
 * 0 을 돌려주는 것을 끝으로 읽었는데, 그러면 <b>후보가 전부 사용된 청크에서 진도가 안 나가</b>
 * 같은 자리를 맴돈다. 이제 후보를 먼저 읽으므로 넘어간 것이 없어도 진도는 그 구간만큼 나간다.
 *
 * <h2>청크 한 번이 하는 일</h2>
 *
 * <pre>
 *   1  후보 선조회        락 없음. id 오름차순 LIMIT chunkSize
 *   2  연속부 자르기       첫 회차와 같은 것까지만        ExpireChunk.from
 *   3  재고 행 잠그기      SELECT … FOR UPDATE           ← 첫 쓰기 락
 *   4  만료 UPDATE        그 회차 · (afterId, lastId]
 *   5  이력 INSERT
 *   6  재고 차감
 *   7  afterId = lastId
 * </pre>
 *
 * <p><b>3번이 1·2번 뒤에 오는 것이 이 잡의 전부다.</b> 잠글 재고 행을 알려면 어느 회차를
 * 건드릴지가 쓰기 전에 정해져 있어야 하고, 그것을 아는 방법이 후보를 먼저 읽는 것뿐이다.
 * 왜 그 순서여야 하는지는 {@link com.kafkick.core.expiration.ExpirationRepository} 에 있다 —
 * 반대로 잡으면 취소가 1213 으로 죽는다.
 */
@Configuration(proxyBeanMethods = false)
public class ExpireJobConfig {

    /**
     * <b>{@code RunningJobProbe} 가 이 이름으로 배치 메타를 조회한다.</b> 리터럴을 두 곳에
     * 적어 두면 한쪽만 고치는 실수를 아무것도 막지 못하는데, 그때 조회는 <b>빈 집합</b>을
     * 돌려주고 <i>"만료가 안 돌고 있다"</i> 와 구분되지 않는다 — 가드가 조용히 꺼진다.
     */
    public static final String JOB_NAME = ExpireStepContext.JOB_NAME;

    private static final Logger log = LoggerFactory.getLogger(ExpireJobConfig.class);

    /**
     * 한 실행 안에서의 진도. 청크가 앞 구간을 다시 훑지 않게 한다.
     *
     * <p><b>JobInstance 안에서만 산다.</b> 주기 실행은 {@code asOf} 가 매번 달라 새 인스턴스라
     * 0 부터 시작하고, 같은 {@code asOf} 로 다시 돌리는 재시작만 이 값을 이어받는다.
     * 어느 쪽이든 결과는 같다 — 멱등성을 지키는 것은 이 값이 아니라 {@code EXPIRE_BATCH} 의
     * {@code status = 'ISSUED'} 조건이다.
     *
     * <p><b>이어받아도 누락이 없는 이유는 옮기는 자리에 있다.</b> 이 값은 가드 셋을 전부
     * 통과한 청크의 <b>맨 끝</b>에서만 옮겨진다. 롤백된 청크는 진도를 안 남긴다.
     */
    static final String AFTER_ID_KEY = "expire.afterId";

    /** 근거는 {@link ExpireStepContext#BLOCKED_COUPONS_KEY} 가 진다. 여기는 별칭이다. */
    static final String BLOCKED_COUPONS_KEY = ExpireStepContext.BLOCKED_COUPONS_KEY;

    /** 근거는 {@link ExpireStepContext#GENERATION_SEPARATOR} 가 진다. 여기는 별칭이다. */
    private static final String GENERATION_SEPARATOR = ExpireStepContext.GENERATION_SEPARATOR;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final int chunkSize;
    private final TransactionAttribute stepTransaction;


    /**
     * <b>두 값을 기동 시점에 거른다.</b> 둘 다 틀렸을 때 잡이 <i>조용히</i> 이상해지는 종류라,
     * 그때 가서는 아무도 모른다.
     *
     * <p>{@code chunk-size} 가 0 이면 {@code LIMIT 0} 이 되는데 MySQL 은 이것을 오류로 보지 않고
     * <b>0건을 돌려준다.</b> 그러면 첫 청크가 곧 종료 신호가 되어 5분마다 정상 완료로 보이면서
     * 아무것도 안 한다 — {@code asOf} 를 빠뜨렸을 때와 완전히 같은 실패 모드다.
     * 그쪽은 파라미터 검증기가 막고 있으니 이쪽도 막는다.
     *
     * <p><b>{@code step-timeout-ms} 는 청크 하나의 데드라인이다.</b> {@code CONTINUABLE} 로
     * 돌아올 때마다 새 트랜잭션이 열리므로 청크마다 다시 센다 — 잡 전체의 상한이 아니다.
     * 그것이 없으면 폭주한 문장을 끊을 수단이 사라진다.
     * {@code innodb_lock_wait_timeout} 은 <b>기다리는 쪽</b>에 걸리는 값이라 잠그고 있는 이 잡에는
     * 상한이 되지 않고, {@code JobOperator.stop()} 은 청크 경계에서만 반응해 단일 문장을 못 끊는다.
     * {@code verifyJob} 의 Step 이 전부 이것을 다는 이유와 같고, <b>재고를 쓰는 이 잡에서 더 세다.</b>
     */
    public ExpireJobConfig(JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Value("${batch.expire.chunk-size:1000}") int chunkSize,
            @Value("${batch.expire.step-timeout-ms:120000}") long stepTimeoutMillis) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException(
                    "batch.expire.chunk-size 는 1 이상이어야 합니다. LIMIT 0 은 오류 없이 0건을 "
                            + "돌려줘 만료가 조용히 멈춥니다. 받은 값=" + chunkSize);
        }
        // 두 조건을 한 갈래로 묶는다. 갈라 두면 순서 때문에 뒤 갈래에 도달할 수 있는 값이
        // 0 과 음의 1000 배수뿐이라, `< 1_000` 을 `< 0` 으로 바꾸는 돌연변이가 살아남았다 —
        // 테스트 이름은 "1초 미만이면 막는다" 인데 실제로 걸리는 것은 앞 갈래였다.
        if (stepTimeoutMillis < 1_000 || stepTimeoutMillis % 1_000 != 0) {
            throw new IllegalArgumentException(
                    "batch.expire.step-timeout-ms 는 1000 이상이면서 1000 의 배수여야 합니다. "
                            + "너무 짧으면 정상 청크가 끊겨 만료가 진행되지 않고, 배수가 아니면 "
                            + "스프링의 트랜잭션 타임아웃이 초 단위라 나머지가 조용히 버려집니다 — "
                            + "ms 라는 단위 이름이 약속한 정밀도가 사라집니다. "
                            + "받은 값=" + stepTimeoutMillis);
        }
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.chunkSize = chunkSize;
        this.stepTransaction = expireStepTransaction(stepTimeoutMillis);

    }

    /**
     * <b>{@code asOf} 가 없으면 조용히 성공한다.</b> 태스클릿이 그 값을 그대로 SQL 에 넘기는데,
     * {@code expires_at < NULL} 은 아무 행도 매치하지 않아 <i>"넘길 대상이 없다"</i> 와
     * 구분되지 않는다 — 5분마다 정상 완료로 보이면서 아무것도 안 하는 상태가 된다.
     * {@code verifyJob} 이 같은 이유로 검증기를 붙여 뒀다.
     */
    @Bean
    public Job expireJob(Step expireStep, BinlogFormatGuard binlogFormatGuard,
            CleanSchemaGuard cleanSchemaGuard) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .validator(new DefaultJobParametersValidator(
                        new String[] {"asOf"}, new String[0]))
                // 이 Step 의 READ COMMITTED DML 이 STATEMENT binlog 서버에서 오류 1665 로
                // 거부된다. 스케줄 실행이든 수동 트리거든 여기를 지나야 만료가 시작한다.
                .listener(binlogFormatGuard)
                // 만료는 원본을 쓰는 유일한 배치다. 오염셋을 보게 띄우면 정답지가 무너진다 —
                // 그것도 "검증기가 틀렸다" 로 보이는 모양으로. 시작 전에 자른다.
                .listener(cleanSchemaGuard)
                // 대기 건수 관측 리스너는 없다 — CY-421 이 ExpirePendingRefresher 로 옮겼다.
                // 잡이 자기 결과를 밀어 넣으면 그 값이 **프로세스와 함께 죽어**, 만료가 일
                // 1회가 된 지금 재기동부터 다음 04:10 까지 백로그 감시가 통째로 꺼진다.
                .start(expireStep)
                .build();
    }

    /**
     * 청크마다 다섯 문장이 한 트랜잭션에서 돈다 — 후보를 읽고 · 재고를 잠그고 · 넘기고 ·
     * 이력을 남기고 · 재고를 되돌린다. <b>첫 청크만 여섯이다</b> — 그 앞에 제외 목록을
     * 한 번 구한다. 순서와 근거는 이 클래스 주석의 표에 있다.
     *
     * <p><b>나눠 담으면 중간 상태가 남는다.</b> 상태만 바뀌고 재고가 안 돌아온 채 죽으면
     * 검증이 그것을 재고 불일치로 잡는다. 실제로는 이 잡이 덜 끝난 것인데 데이터가 틀렸다고 나온다.
     */
    @Bean
    public Step expireStep(ExpirationRepository expirations, TimeProvider timeProvider,
            ExpireMetrics metrics) {
        return new StepBuilder("expireStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    ExecutionContext context = chunkContext.getStepContext()
                            .getStepExecution().getExecutionContext();
                    LocalDateTime asOf = chunkContext.getStepContext().getStepExecution()
                            .getJobParameters().getLocalDateTime("asOf");
                    long afterId = context.getLong(AFTER_ID_KEY, 0L);

                    // 쓰는 시각은 실제 시각이다. asOf 로 백데이트하면 잡이 도는 동안 들어온
                    // 이력보다 우리 이력이 앞서게 찍혀 리플레이가 순서를 뒤집는다 —
                    // USED → EXPIRE 같은 불가능한 전이가 만들어져 V4·V3 가 오탐을 낸다.
                    // 청크마다 새로 잡아 표식으로도 쓴다.
                    LocalDateTime committedAt = timeProvider.now();

                    // asOf 는 "만료 여부를 가르는 컷" 이다. 미래로 주면 기한이 남은 ISSUED 가
                    // 전부 컷 안에 들어와 정상 완료로 넘어가는데, EXPIRED 는 종단 상태라
                    // 되돌릴 전이가 없다. 파라미터 검증기는 키 존재만 보므로 여기서 자른다.
                    // 스케줄러가 주는 값은 now 이하의 크론 슬롯이라 여기 안 걸린다. 다만
                    // 발화가 슬롯 직전에 깨어나는 경우를 CronSlot 이 관용하므로 그만큼 미래일
                    // 수 있다. 그 폭을 여기 다시 적으면 한쪽만 바뀌는 날 스케줄러가 만든 값을
                    // 잡이 거부하므로, 정의한 곳에서 그대로 가져온다.
                    // 진짜로 막아야 하는 것은 손으로 친 값이다.
                    if (asOf == null) {
                        // 파라미터 검증기가 막고 있어야 하는 자리다. 여기 오면 그쪽이 풀린 것이고,
                        // 그것을 "미래 asOf" 로 알리면 운영자가 자기가 친 시각을 의심한다.
                        throw new IllegalStateException(
                                "asOf 파라미터가 없습니다. DefaultJobParametersValidator 가 "
                                        + "막고 있어야 하는 자리입니다.");
                    }
                    if (isAsOfInFuture(asOf, committedAt)) {
                        throw new BusinessException(
                                ExpirationErrorCode.EXPIRE_ASOF_IN_FUTURE,
                                "asOf 가 현재보다 미래입니다. 기한이 남은 발급건까지 만료되고 "
                                        + "EXPIRED 는 되돌릴 수 없습니다. "
                                        + "asOf=" + asOf + " now=" + committedAt);
                    }

                    long generation = chunkContext.getStepContext().getStepExecution()
                            .getJobExecutionId();
                    List<Long> blocked = blockedCoupons(context, expirations, asOf, generation);

                    // ① 후보를 먼저 읽는다 — 락을 안 잡는 읽기다.
                    // ② 첫 회차의 연속부까지만 남긴다. 잠글 재고 행이 하나로 정해진다.
                    ExpireChunk chunk = ExpireChunk.from(
                            expirations.nextCandidates(asOf, afterId, chunkSize, blocked));
                    if (chunk.isEmpty()) {
                        return RepeatStatus.FINISHED;
                    }
                    // ③ 여기가 이 청크의 첫 쓰기 락이다. 발급·취소가 잠그는 그 행을
                    //    같은 방식으로 먼저 잡아야 순환이 안 생긴다.
                    if (!expirations.lockStock(chunk.couponId())) {
                        // blockedCoupons 가 재고 행 없는 회차를 이미 걸렀어야 한다.
                        // 여기 왔다는 것은 그 사이에 재고 행이 사라졌다는 뜻이고, 판정이
                        // 아니라 사고다.
                        throw new BusinessException(
                                ExpirationErrorCode.STOCK_ROW_MISSING,
                                "재고 행이 없는 회차입니다. 그 행을 만들어야 합니다 — "
                                        + "지금 누가 만들고 있는 중이라면 다음 주기가 알아서 "
                                        + "지나갑니다. 회차=" + chunk.couponId());
                    }

                    // ④ 후보를 다시 판단한다. 후보 수가 아니라 이 매치 건수가 만료 건수다 —
                    //    ①과 ③ 사이에 사용·취소된 건은 여기서 안 잡힌다.
                    long lastId = chunk.lastId();
                    int expired = expirations.expireBatch(
                            asOf, committedAt, afterId, lastId, chunk.couponId());

                    if (expired > 0) {
                        int histories = expirations.appendExpireHistories(
                                asOf, committedAt, afterId, lastId, chunk.couponId());
                        if (histories != expired) {
                            // 리플레이가 이력으로 상태를 재구성한다. 이력이 모자라면 검증이
                            // "이력 없는 발급건" 으로 잡는데, 원인이 이 잡이라는 것은 안 나온다.
                            throw new BusinessException(
                                    ExpirationErrorCode.EXPIRE_HISTORY_COUNT_MISMATCH,
                                    "만료 이력 수가 만료 건수와 다릅니다. 다시 돌려도 낫지 "
                                            + "않습니다 — 이력이 빠진 발급건을 찾아 손봐야 "
                                            + "합니다. 만료=" + expired + " 이력=" + histories);
                        }

                        // 재고 행은 ③에서 이미 우리 것이라, 여기서 실패하는 경우는 하나뿐이다:
                        // 뺄 재고가 모자란다. 재고 행이 없는 경우는 ③이 먼저 잡는다.
                        if (expirations.releaseStock(chunk.couponId(), expired, committedAt) != 1) {
                            throw new BusinessException(
                                    ExpirationErrorCode.STOCK_UNDERFLOW,
                                    "만료분을 빼면 재고가 음수가 되는 회차입니다. 재고가 이미 "
                                            + "어긋나 있어 다시 돌려도 낫지 않습니다. "
                                            + "회차=" + chunk.couponId() + " 만료=" + expired);
                        }
                    }

                    // 가드를 전부 지난 뒤에 센다. 메트릭은 롤백을 안 따라가므로 중간에서
                    // 부르면 죽은 청크의 표본이 남는다.
                    metrics.chunkFill(chunk.size(), chunkSize);
                    context.putLong(AFTER_ID_KEY, chunk.lastId());
                    contribution.incrementWriteCount(expired);
                    return RepeatStatus.CONTINUABLE;
                }, transactionManager)
                .transactionAttribute(stepTransaction)
                .build();
    }


    /**
     * 만료 Step 의 트랜잭션 속성. <b>타임아웃과 격리 수준 둘을 담는다.</b>
     *
     * 만료 Step 만 READ COMMITTED 로 내린다. 이 잡이 발급을 막지 않게 하는 유일한 수단이다.
     *
     * REPEATABLE READ 는 팬텀을 막으려고 gap 을 잠근다. 그래서 status='ISSUED' 를 찾다가
     * 다음 값('USED')에 next-key 락을 잡고, 그 gap 이 새 'ISSUED' 가 들어갈 자리라
     * **신규 발급 INSERT 가 오류 1205 로 죽는다.** 인덱스로도 안 풀린다 — 막는 것이
     * 스캔 범위가 아니라 잠금 위치이기 때문이다.
     *
     * 실측(5,000행·만료 대상 0건): 락 2 → 0, 발급 INSERT 1205 → 통과.
     * 전체 수치와 재는 방법은 docs/12-expire-lock-measurement.md.
     *
     * 정합성은 낮춰도 선다. 이 잡의 멱등성은 EXPIRE_BATCH 의 status='ISSUED' 조건이
     * 지키고, 뒤 문장들은 표식과 id 구간으로 집합을 되찾은 뒤 다섯 값을 서로 대조한다 —
     * 스냅샷 시점에 기대는 구조가 아니다. 전체 테스트로 확인했다.
     *
     * ⚠️ 전제: binlog_format 이 ROW 또는 MIXED 여야 한다. STATEMENT 면 MySQL 이
     *    READ COMMITTED 에서 InnoDB DML 을 오류 1665 로 거부한다. MySQL 8 기본값은 ROW 라
     *    대개 문제없지만, 레거시 my.cnf 를 가져오면 배포 후 첫 만료 주기에 터진다 —
     *    공용 테스트 컨테이너는 binlog 를 꺼 두어 이 조합을 재현하지 못하므로,
     *    {@code BinlogFormatGuard} 가 기동 때 막고 {@code BinlogFormatGuardTest} 가
     *    컨테이너를 따로 띄워 양방향으로 확인한다.
     *
     * ⚠️ verifyJob 에는 걸지 마라. 그쪽은 판정 시점을 얼려야 하므로 격리 수준이
     *    낮아지면 dataset_fingerprint 재료가 실행 중에 흔들린다.
     */
    private static TransactionAttribute expireStepTransaction(long millis) {
        DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
        attribute.setTimeout(Math.toIntExact(millis / 1_000));
        attribute.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        return attribute;
    }


    /**
     * <b>{@code asOf} 를 믿을 수 있나.</b> {@link CronSlot#EARLY_FIRE_TOLERANCE} 를 더해
     * 비교한다 — 크론이 슬롯 직전에 깨어나 {@code asOf} 가 최대 2초 미래인 것은 정상이다.
     *
     * <p>한때 이 판정이 두 곳에 있었고 폭이 달랐다. 태스클릿은 관용했는데 관측 리스너는
     * 엄격 비교라, 조기 발화한 정상 주기가 <b>만료는 다 하고 정상 종료했는데 지표 넷이
     * 통째로 {@code NaN}</b> 이 됐다 — 누락 알림의 {@code for} 가 리셋돼 감시가 조용히 꺼졌다.
     * CY-421 이 관측을 되읽기로 옮기면서 <b>호출자가 여기 하나만 남았다.</b>
     *
     * <p>폭을 여기 다시 적지 않고 정의한 곳에서 가져오는 이유는 {@link CronSlot} 이 적어 뒀다.
     *
     * <p><b>{@code null} 은 여기서 안 다룬다.</b> 이 자리에서 {@code null} 은 파라미터
     * 검증기가 풀렸다는 <b>배선 사고</b>이고, 그것을 <i>"미래 asOf"</i> 로 접으면 운영자를
     * 자기가 친 시각으로 보낸다. 호출부가 그 갈래를 따로 진다.
     */
    private static boolean isAsOfInFuture(LocalDateTime asOf, LocalDateTime now) {
        return asOf.isAfter(now.plus(CronSlot.EARLY_FIRE_TOLERANCE));
    }

    /**
     * 이 실행에서 손대지 않을 회차. <b>첫 청크에서 한 번 구하고 문맥에 실어 재사용한다.</b>
     *
     * <p><b>재고가 어긋난 회차를 애초에 창 밖으로 뺀다.</b> 예전에는 넘긴 뒤에 가드가 그것을
     * 발견하고 청크를 통째로 되돌렸다 — 오염 회차 하나가 같은 청크의 남의 회차까지 되돌리고,
     * 진도가 실행 사이로 안 넘어가니 다음 주기도 같은 자리에서 죽어 <b>그 뒤 id 의 만료가
     * 영구히 밀렸다.</b> 설계는 <i>"데이터가 틀렸다는 판정이 나와도 배치는 정상 종료"</i> 로
     * 정했는데 그 반대였다.
     *
     * <p><b>왜 청크마다 다시 안 구하나.</b> 그러면 남은 후보 전체를 매번 훑는다. 그리고
     * 청크 기준으로 막힘을 정의하면 <b>제외한 만큼 {@code LIMIT} 자리가 비어 다른 행이
     * 창 안으로 들어오는데</b>, 그 회차는 판정한 적이 없어 또 막혀 있을 수 있다 —
     * 재고 없이 만료된 상태가 커밋된다. 남은 대기 전체와 견주면 제외 대상이 창 구성과
     * 무관해져서, 밀려 들어오는 것은 언제나 성한 회차뿐이다.
     *
     * <p><b>그래서 차감이 반드시 성공한다.</b> 성한 회차는
     * {@code Σ(청크별 만료 수) ≤ 대기 전체 ≤ active_count} 다. 오른쪽은 제외 조건이 준다.
     * <b>왼쪽이 서려면 "대기 전체" 가 그 실행이 넘길 수 있는 모든 행의 상계여야 한다</b> —
     * 그래서 {@link ExpirationRepository#blockedCoupons} 가 {@code committedAt} 창을
     * 안 건다. 창을 걸면 뒤 청크의 창이 더 넓어져 왼쪽이 오른쪽의 부분집합이 아니게 되고,
     * 차감 합계가 {@code active_count} 를 넘어 {@code STOCK_UNDERFLOW} 로 죽는다.
     *
     * <p><b>취소·사용 경로가 붙은 뒤에도 이 부등식은 선다.</b> 한때 이 자리에
     * <i>"지금은 취소·사용 경로가 없어 줄이는 주체가 이 잡뿐"</i> 이라고 적혀 있었는데,
     * 그 전제는 이제 거짓이다({@code CouponCancelService} · {@code CouponCancelUseService}).
     * 네 갈래를 따져 보면 좌변과 우변이 같은 방향으로 움직인다 —
     * {@code active_count} 는 <i>ISSUED + USED</i> 합계라는 것이 그 이유다.
     *
     * <table border="1">
     *   <caption>사용자 경로가 부등식의 두 항에 하는 일</caption>
     *   <tr><th></th><th>{@code active_count}</th><th>남은 대기</th><th></th></tr>
     *   <tr><td>취소</td><td>−1</td><td>−1</td><td>둘이 같이 준다</td></tr>
     *   <tr><td>사용</td><td>그대로</td><td>−1</td><td>여유가 는다</td></tr>
     *   <tr><td>사용취소 → EXPIRED</td><td>−1</td><td>그대로</td>
     *       <td>그 행이 USED 로 이미 세져 있었다</td></tr>
     *   <tr><td>사용취소 → ISSUED</td><td>그대로</td><td>+1</td>
     *       <td>같은 이유 — 합계 안에서 자리만 옮긴다</td></tr>
     * </table>
     *
     * <p>깨지는 것은 {@code active_count} 가 <b>실제 집계와 이미 어긋난</b> 회차뿐이고,
     * 그것은 {@link #blockedCoupons} 가 창 밖으로 뺀다.
     */
    private static List<Long> blockedCoupons(ExecutionContext context,
            ExpirationRepository expirations, LocalDateTime asOf, long generation) {
        String prefix = generation + GENERATION_SEPARATOR;
        Optional<List<Long>> cached =
                ExpireStepContext.blockedFor(context.getString(BLOCKED_COUPONS_KEY, ""), prefix);
        if (cached.isPresent()) {
            return cached.get();
        }

        List<Long> blocked = expirations.blockedCoupons(asOf);
        context.putString(BLOCKED_COUPONS_KEY, prefix
                + blocked.stream().map(String::valueOf).collect(Collectors.joining(",")));
        if (!blocked.isEmpty()) {
            // 배치는 정상 종료한다. 이것은 데이터를 봐야 하는 사건이지 서버를 볼 사건이 아니다.
            log.warn("재고가 어긋나 이번 실행에서 건너뛰는 회차가 있습니다. "
                    + "만료는 나머지 회차로 계속 진행합니다. 회차수={} 회차={}",
                    blocked.size(), blocked);
        }
        return blocked;
    }
}
