// 기한이 지난 발급건을 만료로 넘기는 잡의 배선입니다.
package com.kafkick.batch.job;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.DefaultJobParametersValidator;
import org.springframework.batch.core.listener.JobExecutionListener;
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
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.batch.config.BinlogFormatGuard;
import com.kafkick.batch.config.CleanSchemaGuard;
import com.kafkick.batch.config.ExpireMetrics;
import com.kafkick.batch.schedule.CronSlot;
import com.kafkick.core.expiration.ExpirationRepository;
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
 * <p><b>진도를 넘어간 건수로 판단하지 않는다.</b> 청크가 0 을 돌려주면 남은 대상이 없다는
 * 뜻이고 그때 끝낸다 — 거르는 조건이 {@code UPDATE} 안에 있어서 성립하는 성질이다.
 */
@Configuration(proxyBeanMethods = false)
public class ExpireJobConfig {

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

    /**
     * 이 실행에서 손대지 않을 회차. 쉼표로 이어 문맥에 싣는다.
     *
     * <p><b>실행당 한 번만 계산한다.</b> 이 질의는 남은 대기 <b>전체</b>를 회차별로 묶으므로
     * 비용이 대기 건수에 비례한다. 청크마다 부르면 그것이 청크 수만큼 곱해지고, 한 번이면
     * 스캔 하나가 늘 뿐이다 — <b>대기가 없는 날은 거의 공짜</b>이고 하루 288회 중 대부분이
     * 그 경로다. (실제 소요는 300만 건을 적재한 뒤에 잰다. 축소 픽스처에서 잰 값은 운영
     * 규모를 대변하지 못한다 — {@code docs/13-batch-follow-ups.md} 에 남겼다.)
     *
     * <p><b>값에 {@code JobExecution} 세대를 함께 싣는다.</b> Step 문맥은 청크 커밋마다
     * 영속되고 <b>재시작이 그대로 복원한다.</b> 세대를 안 보면 재시작이 이전 실행의 목록을
     * 쓰게 되어, 그 사이 새로 어긋난 회차를 못 보고 <b>같은 자리에서 영원히 죽는다</b> —
     * 이 티켓이 없앤 모양이 재시작 축에 그대로 남는 것이다.
     */
    static final String BLOCKED_COUPONS_KEY = "expire.blockedCoupons";

    /** 세대와 목록을 가르는 문자. 회차 id 에도 쉼표에도 안 나온다. */
    private static final String GENERATION_SEPARATOR = "|";

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final int chunkSize;
    private final TransactionAttribute stepTransaction;

    /**
     * <b>관측 질의의 데드라인.</b> {@code afterJob} 은 Step 밖이라 {@code stepTransaction} 의
     * 타임아웃이 안 걸린다 — 그리고 그것을 대신할 것이 <b>아무것도 없다.</b>
     *
     * <p>Spring Batch 는 {@code afterJob} 을 부르기 <b>전에</b> 관측을 멈춘다. 그래서
     * {@code spring_batch_job_seconds} 에도 이 시간이 안 들어가고,
     * {@code BatchJobRunningTooLong} 이 못 본다. {@code JobOperator.stop()} 은 청크 경계에서만
     * 반응하니 여기까지 안 온다. 상한이 없으면 <b>끊을 수단도 알 수단도 없다.</b>
     *
     * <p>그런데 {@code COUNT_PENDING} 은 상한도 {@code LIMIT} 도 없는 전수 집계이고 하루
     * 288번 돈다. 대량 만료가 밀린 날 이것이 길어지면, 스케줄러가 동기 호출이라
     * <b>다음 크론 슬롯이 통째로 사라진다</b> — 그것도 조용히.
     *
     * <p>읽기 전용이고, 터지면 {@code catch} 가 잡아 <i>"모름"</i> 으로 떨어진다.
     * 관측이 판정을 방해하지 않는다는 계약이 그대로 선다.
     */
    private final TransactionTemplate observeTransaction;

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
        this.observeTransaction = observeTransaction(transactionManager, stepTimeoutMillis);

    }

    /**
     * <b>{@code asOf} 가 없으면 조용히 성공한다.</b> 태스클릿이 그 값을 그대로 SQL 에 넘기는데,
     * {@code expires_at < NULL} 은 아무 행도 매치하지 않아 <i>"넘길 대상이 없다"</i> 와
     * 구분되지 않는다 — 5분마다 정상 완료로 보이면서 아무것도 안 하는 상태가 된다.
     * {@code verifyJob} 이 같은 이유로 검증기를 붙여 뒀다.
     */
    @Bean
    public Job expireJob(Step expireStep, BinlogFormatGuard binlogFormatGuard,
            CleanSchemaGuard cleanSchemaGuard, ExpirationRepository expirations,
            ExpireMetrics metrics, TimeProvider timeProvider) {
        return new JobBuilder("expireJob", jobRepository)
                .validator(new DefaultJobParametersValidator(
                        new String[] {"asOf"}, new String[0]))
                // 이 Step 의 READ COMMITTED DML 이 STATEMENT binlog 서버에서 오류 1665 로
                // 거부된다. 스케줄 실행이든 수동 트리거든 여기를 지나야 만료가 시작한다.
                .listener(binlogFormatGuard)
                // 만료는 원본을 쓰는 유일한 배치다. 오염셋을 보게 띄우면 정답지가 무너진다 —
                // 그것도 "검증기가 틀렸다" 로 보이는 모양으로. 시작 전에 자른다.
                .listener(cleanSchemaGuard)
                .listener(reportPending(expirations, metrics, timeProvider))
                .start(expireStep)
                .build();
    }

    /**
     * 청크마다 여섯 문장이 한 트랜잭션에서 돈다 — 넘기고 · 경계를 찾고 · 이력을 남기고 ·
     * 회차를 세고 · 재고 행을 세고 · 재고를 되돌린다. <b>첫 청크만 일곱이다</b> —
     * 그 앞에 제외 목록을 한 번 구한다.
     *
     * <p><b>나눠 담으면 중간 상태가 남는다.</b> 상태만 바뀌고 재고가 안 돌아온 채 죽으면
     * 검증이 그것을 재고 불일치로 잡는다. 실제로는 이 잡이 덜 끝난 것인데 데이터가 틀렸다고 나온다.
     */
    @Bean
    public Step expireStep(ExpirationRepository expirations, TimeProvider timeProvider) {
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

                    int expired = expirations.expireBatch(
                            asOf, committedAt, afterId, chunkSize, blocked);
                    if (expired == 0) {
                        return RepeatStatus.FINISHED;
                    }

                    // 경계를 먼저 읽는다. 아래 네 문장이 같은 창(id > afterId)을 보므로
                    // 진도를 먼저 옮기면 그 문장들이 방금 넘긴 것을 못 본다.
                    long lastId = expirations.lastExpiredId(asOf, committedAt, afterId);

                    int histories = expirations.appendExpireHistories(
                            asOf, committedAt, afterId, lastId);
                    if (histories != expired) {
                        // 리플레이가 이력으로 상태를 재구성한다. 이력이 모자라면 검증이
                        // "이력 없는 발급건" 으로 잡는데, 원인이 이 잡이라는 것은 안 나온다.
                        throw new BusinessException(
                                ExpirationErrorCode.EXPIRE_HISTORY_COUNT_MISMATCH,
                                "만료 이력 수가 만료 건수와 다릅니다. 다시 돌려도 낫지 않습니다 — "
                                        + "이력이 빠진 발급건을 찾아 손봐야 합니다. "
                                        + "만료=" + expired + " 이력=" + histories);
                    }

                    // 아래 둘은 이제 "오염 데이터를 만났다" 가 아니다 — 그것은 blockedCoupons 가
                    // 애초에 창 밖으로 뺀다. 여기까지 왔다는 것은 **제외 논리가 틀렸거나 재고가
                    // 발밑에서 움직였다** 는 뜻이고, 그건 판정이 아니라 사고라 실패가 맞다.
                    // (취소·사용 경로가 붙으면 실행 도중 active_count 가 줄 수 있다.
                    //  그 티켓에서 이 자리를 다시 본다.)
                    int coupons = expirations.expiredCouponCount(asOf, committedAt, afterId, lastId);
                    int stockRows = expirations.stockRowCount(asOf, committedAt, afterId, lastId);
                    if (stockRows != coupons) {
                        // JOIN 이 재고 행 없는 회차를 조용히 건너뛴다. 그 회차의 발급건은
                        // 만료로 넘어갔는데 되돌릴 재고가 없다 — 세지 않으면 아무도 모른다.
                        throw new BusinessException(
                                ExpirationErrorCode.STOCK_ROW_MISSING,
                                "재고를 되돌리지 못한 회차가 있습니다. 재고 행이 없는 회차가 섞였습니다. "
                                        + "그 행을 만들어야 합니다 — 지금 누가 만들고 있는 중이라면 "
                                        + "다음 주기가 알아서 지나갑니다. "
                                        + "회차=" + coupons + " 재고행=" + stockRows);
                    }

                    int released = expirations.releaseStock(asOf, committedAt, afterId, lastId);
                    // released > stockRows 는 위 가드를 통과한 뒤에는 나올 수 없다.
                    // coupon_stocks 의 PK 가 coupon_id 라 JOIN 이 1:1 이고, stockRows == coupons
                    // 를 이미 확인했으므로 되돌린 수가 그보다 커질 길이 없다. 청크 도중에 재고
                    // 행이 생기는 경우(RC 라 문장마다 스냅샷이 갱신된다)도 이 자리가 아니라
                    // 위 STOCK_ROW_MISSING 으로 나간다 — 그쪽 메시지가 그 사실을 적고 있다.
                    if (released != stockRows) {
                        // active_count >= 차감량 조건에 걸린 회차가 있다. 재고가 이미 어긋나
                        // 있어서, 그대로 뺐다면 음수가 됐을 자리다.
                        throw new BusinessException(
                                ExpirationErrorCode.STOCK_UNDERFLOW,
                                "만료분을 빼면 재고가 음수가 되는 회차가 있습니다. "
                                        + "재고가 이미 어긋나 있어 다시 돌려도 낫지 않습니다. "
                                        + "재고행=" + stockRows + " 되돌림=" + released);
                    }

                    context.putLong(AFTER_ID_KEY, lastId);
                    contribution.incrementWriteCount(expired);
                    return RepeatStatus.CONTINUABLE;
                }, transactionManager)
                .transactionAttribute(stepTransaction)
                .build();
    }

    /**
     * 관측 질의를 감싸는 읽기 전용 트랜잭션. <b>여기 타임아웃을 걸어야 질의에 심긴다</b> —
     * 트랜잭션 밖에서 부르면 {@code DataSourceUtils} 가 {@code queryTimeout} 을 안 붙인다.
     *
     * <p>청크와 같은 값을 쓴다. 이 질의가 청크 하나보다 오래 걸릴 이유가 없고, 값을 따로 두면
     * 한쪽만 바뀌는 날 그 사실이 안 드러난다.
     */
    private static TransactionTemplate observeTransaction(
            PlatformTransactionManager transactionManager, long millis) {
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.setReadOnly(true);
        template.setTimeout(Math.toIntExact(millis / 1_000));
        template.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        return template;
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
     * <b>끝난 뒤 남은 것을 센다.</b> 잡의 생사만으로는 <i>"성공했는데 아무것도 안 했다"</i> 를
     * 못 잡는다 — 기존 알림 셋이 전부 그 축이고, 셋 다 통과하면서 기한 지난 발급건이 계속
     * 쌓이는 상태가 있다.
     *
     * <p><b>{@code afterJob} 이라 실패로 끝나도 남는다.</b> 오히려 그때 더 필요하다.
     *
     * <p><b>세는 것 자체가 실패의 이유가 되면 안 된다.</b> 이것은 관측이지 판정이 아니다 —
     * 세다가 죽어서 잡이 실패하면, 정작 만료는 다 해 놓고 지표 때문에 빨간불이 난다.
     * 프레임워크도 {@code afterJob} 예외를 삼키지만({@code AbstractJob}), 그때 나가는 것은
     * 원인이 안 보이는 ERROR 라 여기서 먼저 잡아 뜻이 있는 문장을 남긴다.
     *
     * <p><b>믿을 수 없는 {@code asOf} 로는 세지 않는다.</b> 셋 다 <i>"모름"</i> 으로 되돌린다 —
     * 0 은 <i>"밀린 것이 없다"</i> 라서 누락 알림을 침묵시키고, 미래 {@code asOf} 로 센 값은
     * <i>"밀린 만료"</i> 가 아니라 <b>아직 기한이 남은 발급건</b>이라 수백만이 나온다.
     * 그것을 그대로 내보내면 <b>파라미터를 잘못 친 사건이 "서버를 봐라" critical 로 나간다</b> —
     * 이 잡이 세우려는 구분(서버 · 데이터 · 파라미터)이 바로 그 자리에서 뭉개진다.
     */
    private JobExecutionListener reportPending(ExpirationRepository expirations,
            ExpireMetrics metrics, TimeProvider timeProvider) {
        return new JobExecutionListener() {
            @Override
            public void afterJob(JobExecution jobExecution) {
                try {
                    LocalDateTime asOf = jobExecution.getJobParameters()
                            .getLocalDateTime("asOf");
                    if (asOf == null || isAsOfInFuture(asOf, timeProvider.now())) {
                        metrics.markUnknown();
                        return;
                    }
                    // 제외 목록을 못 읽었다는 것과 목록이 비었다는 것은 다르다.
                    // 앞은 "판정할 재료가 없다", 뒤는 "막힌 회차가 없다" 다.
                    Optional<List<Long>> blocked = blockedFrom(jobExecution);
                    if (blocked.isEmpty()) {
                        metrics.markUnknown(asOf);
                        return;
                    }
                    metrics.record(asOf, observeTransaction.execute(
                            status -> expirations.countPending(asOf, blocked.get())),
                            blocked.get().size());
                } catch (RuntimeException e) {
                    // 직전 실행 값을 들고 있으면 관제가 그것을 이번 결과로 읽는다.
                    // asOf 를 못 읽었을 수도 있어 순서 없는 쪽으로 간다.
                    metrics.markUnknown();
                    log.warn("남은 만료 대기를 세지 못했습니다. 지표를 '모름' 으로 되돌립니다.", e);
                }
            }
        };
    }

    /**
     * Step 문맥에 실린 이번 실행의 제외 목록. <b>빈 {@code Optional} 은 "모른다" 다.</b>
     *
     * <p><b>못 읽은 것과 비어 있는 것을 가르는 것이 이 반환 타입의 전부다.</b> 예전에는 둘 다
     * 빈 목록이었다. 그러면 Step 을 시작도 못 한 실행 — 오염 스키마 가드나 binlog 가드가
     * 세웠거나 DB 가 안 붙은 경우 — 에서 <b>막힌 회차가 없다</b>고 판정하게 되고, 남은 대기가
     * 전부 <i>"배치가 처리했어야 하는 몫"</i> 으로 나간다. 그 알림은 critical 이고
     * <i>"서버를 봐야 한다"</i> 고 안내하는데, 실제로 고칠 곳은 접속 설정이다 —
     * 이 잡이 세우려는 구분(서버 · 데이터 · 파라미터 · 설정)이 그 자리에서 뭉개진다.
     *
     * <p><b>세대가 다른 값은 안 읽는다.</b> 재시작한 Step 의 문맥에는 이전 실행이 남긴 값이
     * 복원돼 있다. 태스클릿이 첫 청크에서 덮어쓰지만, 그 전에 죽으면 낡은 목록이 남는다 —
     * 그것을 이번 실행의 판정으로 세면 <b>이미 고쳐진 회차가 계속 제외된 것처럼</b> 보여
     * {@code blocked_pending} 이 부풀고 누락 알림이 침묵한다. 첫 청크가 <b>커밋 시점에</b>
     * 롤백되는 경우({@code TaskletStep} 이 문맥을 청크 이전으로 되돌린다)도 여기로 떨어진다.
     */
    private static Optional<List<Long>> blockedFrom(JobExecution jobExecution) {
        String prefix = jobExecution.getId() + GENERATION_SEPARATOR;
        return jobExecution.getStepExecutions().stream()
                .map(step -> blockedFor(
                        step.getExecutionContext().getString(BLOCKED_COUPONS_KEY, ""), prefix))
                .flatMap(Optional::stream)
                .findFirst();
    }

    /**
     * 문맥 값에서 <b>이 세대의</b> 목록만 꺼낸다. 세대가 다르면 빈 {@code Optional} 이다.
     *
     * <p><b>한 곳에 모으는 이유가 있다.</b> 이 포맷을 판정({@code blockedCoupons})과
     * 관측({@code blockedFrom})이 함께 읽는다. 두 벌로 두면 포맷을 바꾸는 날 한쪽만 고쳐지고,
     * 그 어긋남은 <b>지표만 조용히 틀리게</b> 만든다 — 잡은 멀쩡히 돈다.
     *
     * <p><b>두 가지 "빈 것" 을 가른다.</b> 이 메서드에 오는 것은 문맥 값 전체(`raw`)이지
     * id 목록이 아니다.
     *
     * <pre>
     *   raw = ""        접두사가 안 맞는다 → Optional.empty()  <b>모른다</b>
     *   raw = "7|"      이 세대가 판정했고 목록이 비었다        <b>막힌 회차가 없다</b>
     *   raw = "6|3,9"   남의 세대다 → Optional.empty()          <b>모른다</b>
     * </pre>
     *
     * 그 구분이 관측의 전부다 — <i>"못 읽었다"</i> 를 <i>"막힌 회차가 없다"</i> 로 읽으면
     * 남은 대기가 전부 <i>"배치가 처리했어야 하는 몫"</i> 으로 나가고, 그 알림은
     * <b>서버를 보라</b>고 안내한다.
     */
    private static Optional<List<Long>> blockedFor(String raw, String prefix) {
        if (!raw.startsWith(prefix)) {
            return Optional.empty();
        }
        String ids = raw.substring(prefix.length());
        return Optional.of(ids.isEmpty() ? List.of()
                : Arrays.stream(ids.split(",")).map(Long::valueOf).toList());
    }

    /**
     * <b>{@code asOf} 를 믿을 수 있나.</b> 태스클릿과 {@code afterJob} 이 <b>같은 것을 봐야
     * 한다</b> — 한쪽만 관용 폭을 빼면 정상 주기가 한쪽에서만 통과한다.
     *
     * <p>실제로 그랬다. 태스클릿은 {@link CronSlot#EARLY_FIRE_TOLERANCE} 를 더해 비교하는데
     * 리스너는 엄격 비교라, 조기 발화로 {@code asOf} 가 1초 미래인 주기가 <b>만료는 다 하고
     * 정상 종료했는데 지표 넷이 통째로 {@code NaN}</b> 이 됐다. 그러면 누락 알림의
     * {@code for} 타이머가 리셋되어 감시가 조용히 꺼진다.
     *
     * <p>폭을 여기 다시 적지 않고 정의한 곳에서 가져오는 이유는 {@link CronSlot} 이 적어 뒀다.
     *
     * <p><b>{@code null} 은 여기서 안 다룬다.</b> 두 호출자에게 뜻이 다르기 때문이다 —
     * 태스클릿에서는 파라미터 검증기가 풀렸다는 <b>배선 사고</b>이고, {@code afterJob} 에서는
     * 검증에 걸려 Step 이 안 돈 <b>정상적인 실패</b>다. 하나로 뭉치면 앞의 경우가
     * <i>"미래 asOf"</i> 로 나가 운영자를 자기가 친 시각으로 보낸다.
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
     * <p><b>단서: {@code active_count} 가 실행 도중 줄면 이 부등식도 흔들린다.</b> 지금은
     * 취소·사용 경로가 없어 줄이는 주체가 이 잡뿐이다. 그 경로가 붙는 티켓에서 다시 본다.
     */
    private static List<Long> blockedCoupons(ExecutionContext context,
            ExpirationRepository expirations, LocalDateTime asOf, long generation) {
        String prefix = generation + GENERATION_SEPARATOR;
        Optional<List<Long>> cached =
                blockedFor(context.getString(BLOCKED_COUPONS_KEY, ""), prefix);
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
