// 기한이 지난 발급건을 만료로 넘기는 잡의 배선입니다.
package com.kafkick.batch.job;

import java.time.LocalDateTime;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.DefaultJobParametersValidator;
import org.springframework.batch.core.job.builder.JobBuilder;
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
import com.kafkick.core.expiration.ExpirationRepository;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.expiration.exception.ExpirationErrorCode;
import com.kafkick.core.support.TimeProvider;

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
    public Job expireJob(Step expireStep, BinlogFormatGuard binlogFormatGuard) {
        return new JobBuilder("expireJob", jobRepository)
                .validator(new DefaultJobParametersValidator(
                        new String[] {"asOf"}, new String[0]))
                // 이 Step 의 READ COMMITTED DML 이 STATEMENT binlog 서버에서 오류 1665 로
                // 거부된다. 스케줄 실행이든 수동 트리거든 여기를 지나야 만료가 시작한다.
                .listener(binlogFormatGuard)
                .start(expireStep)
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
     * 청크마다 여섯 문장이 한 트랜잭션에서 돈다 — 넘기고 · 경계를 찾고 · 이력을 남기고 ·
     * 회차를 세고 · 재고 행을 세고 · 재고를 되돌린다.
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
                    // 발화가 슬롯 직전에 깨어나는 경우를 CronSlot 이 관용하므로 몇 초 미래일
                    // 수 있다 — 그 폭(CronSlot.EARLY_FIRE_TOLERANCE)만 열어 둔다.
                    // 진짜로 막아야 하는 것은 손으로 친 값이다.
                    if (asOf.isAfter(committedAt.plusSeconds(2))) {
                        throw new BusinessException(
                                ExpirationErrorCode.EXPIRE_ASOF_IN_FUTURE,
                                "asOf 가 현재보다 미래입니다. 기한이 남은 발급건까지 만료되고 "
                                        + "EXPIRED 는 되돌릴 수 없습니다. "
                                        + "asOf=" + asOf + " now=" + committedAt);
                    }

                    int expired = expirations.expireBatch(asOf, committedAt, afterId, chunkSize);
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

                    // 셋을 함께 봐야 두 실패가 갈린다. 하나로 뭉치면 원인이 섞인 메시지가
                    // 나가고, 운영자가 없는 재고 행을 찾다가 실제로는 수량이 모자란 것을 놓친다.
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
}
