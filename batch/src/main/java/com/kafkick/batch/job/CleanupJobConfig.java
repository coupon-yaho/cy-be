// 검증이 남긴 파생 행과 배치 메타를 걷는 잡의 배선입니다.
package com.kafkick.batch.job;

import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepExecution;
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

import com.kafkick.batch.config.BatchMetadataWindow;
import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.verification.CleanupRepository;
import com.kafkick.core.verification.CleanupRepository.PurgedMetadata;
import com.kafkick.core.verification.StatsRepository;

/**
 * <b>{@code @Scheduled} 가 아니라 Spring Batch 잡이다 — 규칙의 입력이 바뀌었다.</b>
 *
 * <p>{@code docs/11} 은 정리를 계층 3({@code @Scheduled})으로 분류했고, 그 규칙은
 * <i>"청크 재시작 · 실행 이력이 판정 근거 · 파라미터 재실행 증명 중 하나도 없으면
 * {@code @Scheduled}"</i> 다. 정리는 셋 다 해당 없다.
 *
 * <p><b>그런데 CY-392 가 감시를 전부 배치 메타 위에 세웠다.</b>
 * {@code BatchRunMetrics} 는 {@code List<Job>} 빈에서 이름을 모아 잡마다 마지막 성공 시각
 * 게이지를 <b>자동으로</b> 만들고, {@code BatchJobFailed}·{@code BatchStuckExecution} 은
 * 셀렉터에 잡 이름이 없어 새 잡을 그날 바로 덮는다. {@code @Scheduled} 로 만들면
 * {@code BATCH_JOB_EXECUTION} 행이 안 남아 <b>그 감시가 전부 비껴간다.</b>
 *
 * <p>정리는 300만 행을 지우는 일이라 <b>조용히 안 도는 것</b>이 가장 나쁜 결말이다.
 * 그래서 잡으로 만든다 — 규칙을 어긴 것이 아니라, 규칙이 세워질 때 없던 축이 생긴 것이다.
 *
 * <p><b>무엇을 지우나.</b> 검증이 남긴 파생 행 셋이다 — {@code asof_state} · 통계 셋 ·
 * {@code verification_findings}. {@code verification_runs} 행 자체는 남긴다 — 그것이 판정
 * 이력이고 관제와 지표가 그 위에 선다. {@code FAIL} 의 검출 행도 남긴다({@code deleteFindings}
 * 의 근거). <b>{@code BATCH_*} 메타는 {@code purgeBatchMetadataStep} 이 걷는다</b>
 * ({@code batch.cleanup.metadata-keep-days}, 최소 8 = 되읽기 창 7일 초과).
 *
 * <p><b>멱등성 레코드와 토큰은 안 지운다.</b> {@code idempotency_records} 는 읽고 쓰는 코드가
 * 저장소에 <b>하나도 없고</b>, 토큰은 애초에 이 저장소의 테이블이 아니다(Redis, 영역 ③).
 * {@code docs/11} 의 목록이 그 둘을 적은 것은 <b>계획</b>이지 현재 상태가 아니다.
 */
@Configuration(proxyBeanMethods = false)
public class CleanupJobConfig {

    /**
     * <b>{@code BatchRunMetrics} 가 이 이름으로 게이지를 만든다.</b> 잡 이름은 그 축의
     * 라벨 값이 되므로 리터럴을 두 곳에 적지 않는다.
     */
    public static final String JOB_NAME = "cleanupJob";

    /**
     * <b>어디까지 끝냈나.</b> 대상은 id 오름차순이라 <i>"이 id 까지는 셋 다 걷었다"</i> 하나로
     * 진도가 표현된다.
     *
     * <p><b>실행 컨텍스트에 두는 이유는 재시작이 아니라 격리다.</b> 태스클릿 람다는 싱글턴
     * 빈 안에 있어서 필드에 두면 <b>실행끼리 값이 샌다.</b> 재시작으로 이어받지는 못한다 —
     * {@code CleanupScheduler} 가 매 발화에 {@code firedAt} 을 실어 <b>매번 새 JobInstance</b>
     * 를 만들고, 실패한 실행을 다시 시작하는 경로도 없다. 실패하면 다음 발화가 처음부터
     * 다시 훑는다. 지우는 일은 멱등이라 손해는 재스캔뿐이다.
     */
    private static final String DONE_UP_TO_KEY = "cleanup.doneUpToRunId";

    /** 로그용 누계. 청크마다 트랜잭션이 갈리므로 지역 변수로는 살아남지 못한다. */
    private static final String ASOF_STATE_ROWS_KEY = "cleanup.asOfStateRows";

    /** 지금 실행에서 걷은 행 수. "재방문" 과 "실제로 걷었다" 를 가르는 값이다. */
    private static final String CURRENT_RUN_ROWS_KEY = "cleanup.currentRunRows";

    private static final String FINDING_ROWS_KEY = "cleanup.findingRows";

    private static final String RUNS_PURGED_KEY = "cleanup.runsPurged";

    /** 지운 배치 메타 실행 수. Step 이 갈려 있어 커서도 따로 둔다. */
    private static final String META_EXECUTIONS_KEY = "cleanup.metaExecutions";

    /** 지운 고아 인스턴스 수. */
    private static final String META_INSTANCES_KEY = "cleanup.metaInstances";

    /** 첫 청크가 잡은 컷오프. 청크마다 다시 잡으면 한 실행 안에서 기준이 앞으로 민다. */
    private static final String META_CUTOFF_KEY = "cleanup.metaCutoff";

    /**
     * <b>Step 1 의 버려진-실행 컷오프.</b> {@link #META_CUTOFF_KEY} 와 같은 이유로 첫 호출이
     * 잡은 값을 끝까지 쓴다 — 다만 이쪽이 더 위험하다. Step 2 가 미는 것은 "지울 배치 메타"
     * 인데 이쪽이 미는 것은 <b>도는 검증의 입력({@code asof_state})</b> 이다.
     */
    private static final String ABANDONED_CUTOFF_KEY = "cleanup.abandonedCutoff";

    /**
     * <b>배치 메타 보존 하한 — 되읽기 창보다 하루 크다.</b> 검사가
     * {@code < MIN_METADATA_KEEP_DAYS} 라 창 값(7) 자체는 거절된다. 두 값을 따로 적으면
     * "하한 7" 처럼 부등호가 등호로 새는데, 실제로 그렇게 새서 다섯 자리가 틀렸었다.
     *
     * <p><b>왜 창보다 커야 하나.</b> {@code BatchRunMetricsRefresher} 와
     * {@code ExpirePendingRefresher} 가 마지막 성공 실행을
     * {@link BatchMetadataWindow#LOOKBACK_DAYS} 창에서 찾는다. 보존이 그 창보다 길어야
     * <b>삭제가 게이지에 영향을 줄 수 없다</b> — 창이 언제나 구속 조건이 된다.
     * 두 값이 <b>한 상수에서 나오므로</b> 창을 넓히면 이 하한이 자동으로 따라 올라간다(CY-470).
     *
     * <p>⚠️ <b>"안쪽으로 내리면 곧 {@code NaN}" 은 아니다.</b> 두 잡은 매일 도니까 오늘치
     * 성공이 남아 평소에는 멀쩡하다. 무너지는 것은 <b>잡이 {@code metadata-keep-days} 일 넘게
     * 연속 실패한 날</b>이다 — 마지막 성공이 컷오프 밖으로 밀려 지워지고, 게이지가
     * {@code NaN} 이 되어 {@code ExpireNeverSucceeded}(critical)가 뜬다. 실제 상태는
     * <i>"며칠 실패"</i> 인데 관제는 <i>"한 번도 성공한 적 없음"</i> 을 읽는다 — 사고 등급이 바뀐다.
     *
     * <p><b>컷오프의 시간대 — 여기는 안 어긋난다.</b> 컷오프는 {@code TimeProvider}(UTC)로
     * 잡아 <b>원시 {@code LocalDateTime} 으로</b> 바인딩되는데, 원시 바인딩은 드라이버의 존
     * 변환을 <b>안 타서</b> 그 UTC 벽시계가 그대로 서버에 간다. 비교 대상인
     * {@code CREATE_TIME}·{@code END_TIME} 은 프레임워크가 {@code Timestamp.valueOf} 로 써서
     * <b>세션 존(UTC)으로 렌더링된</b> 값이다 — 둘 다 UTC 라 JVM 기본 존과 무관하게 만난다.
     * ({@code TimestampBindingAxisTest} 가 서버가 본 값을 단언한다.)
     *
     * <p>⚠️ <b>그러니 여기에 {@code Timestamp.valueOf} 를 씌우지 마라.</b> 씌우는 순간 UTC
     * 벽시계가 <i>JVM 기본 존의 벽시계</i>로 재해석돼, 컷오프가 <b>그 존의 오프셋만큼</b>
     * 움직인다. 방향이 부호를 따라 갈린다:
     * <ul>
     *   <li><b>UTC 동쪽</b>(KST, +09) — 컷오프가 아홉 시간 <b>이르게</b> 간다
     *       ({@code 16:42:55} 로 보낸 값을 서버가 {@code 07:42:55} 로 받는다). 조건이
     *       {@code CREATE_TIME < :olderThan} 이라 <b>덜 지운다</b> — 보존이 실질
     *       30일+9시간이 된다. 성가시지만 데이터를 잃지는 않는다
     *   <li><b>UTC 서쪽</b>(음수 오프셋) — 반대로 <b>늦게</b> 가서 <b>더 지운다.</b> 이쪽이
     *       위험하다 — 위에 적은 대로 마지막 성공이 컷오프 밖으로 밀리면 게이지가
     *       {@code NaN} 이 되고 {@code ExpireNeverSucceeded}(critical)가 뜬다
     * </ul>
     * 한때 {@code DefaultZoneGuard} 가 이 자리를 <i>"깨지는 자리"</i> 로 잘못 적었고,
     * 그 단정이 여기서 시작했다.
     */
    static final int MIN_METADATA_KEEP_DAYS = BatchMetadataWindow.LOOKBACK_DAYS + 1;

    /**
     * <b>걷을 것이 남았는데 검증에 자리를 내주고 멈춘 상태.</b> 잡 상태는 {@code COMPLETED}
     * 로 둔다 — 실패가 아니고, 실패로 두면 {@code BatchJobFailed} 가 검증 때문에 운다.
     * 대신 종료 코드로 사실을 남겨 {@code BatchRunMetricsRefresher} 가 성공 집계에서 뺀다.
     *
     * <p><b>"한 행도 안 걷었다" 가 아니다.</b> 검사는 청크마다 도므로 <b>커밋된 진도까지는
     * 남는다</b> — 몇 행에서 멈췄는지는 종료 <i>설명</i>({@code purgedRows})이 진다.
     * {@code ExitStatus.and()} 의 severity 순서상 미지 코드가 {@code COMPLETED} 를 이기므로,
     * 마지막 청크에서 한 번만 세워도 Step·Job 종료 코드가 이것이 된다.
     *
     * <p><b>걷을 것이 없던 밤은 여기 해당하지 않는다.</b> 대상 조회가 이 검사보다 앞에
     * 있어서, 할 일이 없으면 검증이 떠 있어도 그냥 {@code COMPLETED} 로 닫힌다 —
     * 순서가 반대였을 때는 정상 상태에서 {@code CleanupNotSucceeding} 이 울었다.
     *
     * <p><b>물러나는 횟수에는 상한이 없다.</b> 만료의 {@code max-expire-skips} 같은 장치를
     * 안 뒀다 — 대신 반복되는 yield 는 {@code CleanupNotSucceeding} 이 25시간째에 잡는다.
     * 마지막 성공이 안 갱신되기 때문이다.
     */
    public static final String YIELDED_EXIT_CODE = "YIELDED";

    private static final Logger log = LoggerFactory.getLogger(CleanupJobConfig.class);

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final TransactionAttribute timeout;
    private final int keepRuns;
    private final int chunkSize;

    private final int metadataKeepDays;

    /**
     * <b>{@code batch.cleanup.chunk-size} 와 나눈다.</b> 그 값은 <i>"{@code asof_state} 행
     * 몇 개"</i> 이고 기본이 10,000 인데, 여기서는 <b>잡 실행 몇 개</b>다 — 실행 하나에
     * 딸린 행이 잡마다 크게 다르다({@code verifyJob} 은 Step 열하나 + 문맥 blob).
     * 같은 값을 쓰면 한 트랜잭션이 27만 행을 들고 {@code step-timeout-ms}(120초)에 걸리고,
     * 그러면 <b>여태 지운 것이 전부 롤백돼 진도가 0</b> 이라 다음 날도 같은 양을 처음부터
     * 시도한다 — 이 클래스가 청킹을 세운 이유가 정확히 그 상태를 막는 것이다.
     */
    private final int metadataChunkSize;
    private final long abandonedAfterHours;

    /**
     * <b>값 검사를 생성자에 모은다.</b> Step 빈 메서드 파라미터로 두면 컨테이너를 띄워야만
     * 재지는데, 이 검사들은 DB 와 아무 상관이 없다 — {@code ExpireJobConfig} 가 같은 이유로
     * 같은 모양이고 {@code ExpireJobSettingsTest} 가 컨테이너 없이 그것을 잰다.
     */
    public CleanupJobConfig(JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Value("${batch.cleanup.step-timeout-ms:120000}") long stepTimeoutMillis,
            @Value("${batch.verify.asof-state-keep-runs:5}") int keepRuns,
            @Value("${batch.cleanup.chunk-size:10000}") int chunkSize,
            @Value("${batch.cleanup.abandoned-after-hours:6}") long abandonedAfterHours,
            @Value("${batch.cleanup.metadata-keep-days:30}") int metadataKeepDays,
            @Value("${batch.cleanup.metadata-chunk-size:500}") int metadataChunkSize) {
        // 스프링의 트랜잭션 타임아웃은 초 단위라 999 는 0 으로 내려앉는데, 0 은 "무제한" 이
        // 아니라 **데드라인이 이미 지났음**이다 — 첫 문장에서 TransactionTimedOutException 이
        // 난다. 기동은 성공하고 04:30 만 매일 조용히 실패하는 모양이 되므로 여기서 거절한다.
        // VerifyJobConfig·ExpireJobConfig 가 각자 같은 가드를 갖고 있다.
        if (stepTimeoutMillis < 1_000 || stepTimeoutMillis % 1_000 != 0) {
            throw new IllegalArgumentException(
                    "batch.cleanup.step-timeout-ms 는 1000 이상이면서 1000 의 배수여야 합니다. "
                            + "초 단위로 내림하기 때문에 999 이하는 0 초가 되고, 그러면 첫 "
                            + "문장에서 트랜잭션이 만료됩니다. 받은 값=" + stepTimeoutMillis);
        }
        // chunk-size 0 은 DELETE … LIMIT 0 이라 **한 행도 안 지운 채 성공으로 닫힌다.**
        // 잡이 COMPLETED 라 CleanupNotSucceeding 도 안 울고, 가장 무거운 테이블만 안 걷히는
        // 상태가 감시망을 통째로 통과한다. 음수는 MySQL 문법 오류로 매일 실패한다.
        if (chunkSize < 1) {
            throw new IllegalArgumentException(
                    "batch.cleanup.chunk-size 는 1 이상이어야 합니다. 0 이면 asof_state 를 "
                            + "한 행도 안 지운 채 잡이 성공으로 닫혀 감시망을 통과합니다. "
                            + "받은 값=" + chunkSize);
        }
        // 0 이면 방금 시작한 검증까지 "버려진 것" 으로 보고 그 입력을 걷는다.
        if (abandonedAfterHours < 1) {
            throw new IllegalArgumentException(
                    "batch.cleanup.abandoned-after-hours 는 1 이상이어야 합니다. 0 이면 방금 "
                            + "시작한 검증의 asof_state 를 걷어 그 판정이 자기 입력을 잃습니다. "
                            + "받은 값=" + abandonedAfterHours);
        }
        // 0 이면 방금 끝난 판정의 파생 행까지 그날 밤에 사라진다.
        if (keepRuns < 1) {
            throw new IllegalArgumentException(
                    "batch.verify.asof-state-keep-runs 는 1 이상이어야 합니다. 0 이면 직전 "
                            + "판정의 파생 행이 그날 밤에 사라져 되짚을 근거가 없습니다. "
                            + "받은 값=" + keepRuns);
        }
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.keepRuns = keepRuns;
        if (metadataKeepDays < MIN_METADATA_KEEP_DAYS) {
            throw new IllegalArgumentException(
                    "batch.cleanup.metadata-keep-days 는 " + MIN_METADATA_KEEP_DAYS
                            + " 이상이어야 합니다(되읽기 창 " + BatchMetadataWindow.LOOKBACK_DAYS
                            + "일 초과). BatchRunMetricsRefresher 와 "
                            + "ExpirePendingRefresher 가 마지막 성공 실행을 "
                            + "END_TIME > NOW() - INTERVAL " + BatchMetadataWindow.LOOKBACK_DAYS + " DAY "
                            + "창에서 찾습니다. 보존이 그 창보다 길어야 삭제가 게이지에 "
                            + "영향을 줄 수 없습니다. 안쪽으로 내려도 잡이 매일 성공하는 동안은 "
                            + "멀쩡해 보이지만, 연속 실패가 보존 기간을 넘긴 날 마지막 성공이 "
                            + "지워져 '며칠 실패' 가 '한 번도 성공한 적 없음'(NaN)으로 보고되고 "
                            + "ExpireNeverSucceeded·CleanupNeverSucceeded 가 영구 발화합니다. "
                            + "그 창을 바꾸려면 두 되읽기의 조회도 함께 고치십시오. "
                            + "받은 값=" + metadataKeepDays);
        }
        // **보존 기간 상한.** 오타 하나가 chunk-size=0 과 **관측상 같은 상태**를 만든다 —
        // 배치 메타가 사실상 안 걷히는데 잡은 매일 COMPLETED 라 CleanupNotSucceeding 도
        // 안 울고, 배치 메타 백로그에는 전용 알림이 없다. 이 클래스가 가드를 세운 근거가
        // 정확히 그 "조용한 통과" 다.
        if (metadataKeepDays > 365) {
            throw new IllegalArgumentException(
                    "batch.cleanup.metadata-keep-days 는 " + MIN_METADATA_KEEP_DAYS
                            + " 이상 365 이하여야 합니다. 너무 크면 BATCH_* 가 사실상 안 걷히는데 "
                            + "잡은 매일 COMPLETED 라 CleanupNotSucceeding 도 안 울고 배치 메타 "
                            + "백로그에는 전용 알림이 없습니다 — chunk-size=0 과 관측상 같은 "
                            + "상태입니다. 받은 값=" + metadataKeepDays);
        }
        // **청크 크기.** 0 이면 LIMIT 0 이라 MySQL 이 오류 없이 0건을 돌려주고, 첫 청크가
        // 곧 종료 신호가 되어 잡이 COMPLETED 로 닫힌다 — 형제 키와 같은 실패 모양이다.
        // 상한도 건다. 삭제는 id 하나씩 나가므로(CleanupJdbcAdapter 에 근거를 적었다)
        // 이 값이 곧 **한 트랜잭션의 문장 수 ÷ 6** 이다. 잠그는 행은 이 값이 아니라 그
        // 실행들에 딸린 행 전부다 — verifyJob 이면 실행 하나에 26행이라 5000 이면 13만 행.
        // 키울수록 한 청크가 무거워지고 step-timeout-ms 안에 못 들어올 위험이 커지는데,
        // 걸리면 여태 지운 것이 전부 롤백돼 진도가 0 이라 다음 날도 처음부터 시도한다.
        if (metadataChunkSize < 1 || metadataChunkSize > 5_000) {
            throw new IllegalArgumentException(
                    "batch.cleanup.metadata-chunk-size 는 1 이상 5000 이하여야 합니다. "
                            + "0 이면 LIMIT 0 이라 MySQL 이 오류 없이 0건을 돌려주고, 첫 청크가 "
                            + "곧 종료 신호가 되어 잡이 COMPLETED 로 닫힙니다 — "
                            + "CleanupNotSucceeding 도 안 울어서 배치 메타만 영원히 안 걷히는 "
                            + "상태가 감시망을 통과합니다. 반대로 너무 크면 한 트랜잭션이 실행 "
                            + "수천 개에 딸린 수십만 행을 들고 step-timeout-ms 에 걸려 전량 "
                            + "롤백되고, 진도가 0 이라 다음 날도 처음부터 시도합니다. "
                            + "받은 값=" + metadataChunkSize);
        }
        this.metadataKeepDays = metadataKeepDays;
        this.metadataChunkSize = metadataChunkSize;
        this.chunkSize = chunkSize;
        this.abandonedAfterHours = abandonedAfterHours;
        DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
        attribute.setTimeout(Math.toIntExact(stepTimeoutMillis / 1_000));
        attribute.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.timeout = attribute;
    }

    @Bean
    public Job cleanupJob(Step purgeVerificationRunsStep, Step purgeBatchMetadataStep) {
        // **물러났으면 통째로 물러난다.** 그 판정은 뒤 Step 안에서 한다 —
        // 흐름(.on(YIELDED).end(...))으로 끊으면 잡 종료 코드는 남지만 **종료 메시지가
        // 날아간다.** 그 메시지가 "몇 건 걷고 멈췄나" 를 지고 있어서, 없으면 코드 하나로는
        // "한 행도 못 걷었다" 와 "200만 걷고 멈췄다" 가 같은 값이 된다.
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(purgeVerificationRunsStep)
                .next(purgeBatchMetadataStep)
                .build();
    }

    /**
     * <b>파라미터가 없다.</b> 정리는 <i>"지금 남아 있는 것"</i> 을 보는 일이라 기준 시각을
     * 실을 이유가 없고, 실으면 같은 값으로 두 번 못 도는 제약만 생긴다 — 만료가 {@code asOf}
     * 를 식별 파라미터로 쓰는 것과 반대 방향이다.
     *
     * <p>대신 {@code JobParametersIncrementer} 도 안 쓴다. 스케줄러가 매 실행에 시각을
     * 실어 인스턴스를 가른다({@code CleanupScheduler}).
     *
     * <p><b>태스클릿 한 번이 청크 하나다.</b> {@code RepeatStatus.CONTINUABLE} 로 돌아오면
     * Spring Batch 가 <b>새 트랜잭션</b>에서 다시 부르므로 앞 청크는 커밋된 채 남는다.
     * 한 번에 다 돌고 {@code FINISHED} 를 돌려주면 <b>청킹이 아무것도 안 나눈다</b> —
     * {@code LIMIT} 을 붙여 삭제해도 커밋이 마지막에 한 번뿐이라 언두 로그와 잠금은 전량을
     * 통째로 들고 있고, 데드라인에 걸리면 <b>여태 지운 것이 전부 롤백</b>된다. 그러면 진도가
     * 0 이라 다음 날도 같은 양을 처음부터 시도해 또 실패한다 — 영원히 성공 못 하는 잡이 된다.
     * {@code ExpireJobConfig} 가 같은 이유로 같은 모양을 쓴다.
     *
     * <p><b>진도는 "어디까지 끝냈나" 하나로 충분하다.</b> 대상 목록을 컨텍스트에 실어 두지
     * 않고 매 호출 다시 고른다 — {@code verification_runs} 는 수백 행이라 값이 싸고,
     * 목록을 얼려 두면 그 사이 판정이 난 실행을 <b>옛 판단으로</b> 지우게 된다.
     *
     * <p><b>대상 집합은 "이미 걷었나" 를 안 본다 — 매 밤 지난 이력을 다시 들른다.</b>
     * 이미 빈 실행 하나마다 태스클릿이 한 번 더 돌고 그 한 번이 독립 트랜잭션이다.
     * 3주 시연 규모(수백 행)에서는 수백 번의 빈 왕복이라 초 단위이고,
     * {@code CleanupRunningTooLong}(900초) 근처에도 못 간다 — 그래서 지금은 술어를 안 더한다.
     * 이력이 만 단위로 가면 {@code verification_runs.purged_at} 을 세워 잘라야 한다.
     * 대신 <b>로그가 재방문을 "걷었다" 로 세지 않게</b> 했다.
     */
    @Bean
    public Step purgeVerificationRunsStep(CleanupRepository cleanup, StatsRepository stats,
            RunningJobProbe runningJobs, TimeProvider timeProvider) {
        return new StepBuilder("purgeVerificationRunsStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    ExecutionContext context =
                            chunkContext.getStepContext().getStepExecution().getExecutionContext();

                    // **"걷을 것이 있나" 를 먼저 묻는다.** 순서가 반대면 할 일이 없던 밤에도
                    // 검증이 떠 있다는 이유만으로 YIELDED 가 되고, 그 실행이 마지막 성공에서
                    // 빠져 **정상 상태에서 CleanupNotSucceeding 이 운다.** 이 두 질의는
                    // SELECT 뿐이라 검증과 부딪히지 않는다.
                    // **컷오프는 첫 호출이 잡은 것을 끝까지 쓴다(CY-686).** 태스클릿이
                    // RepeatStatus.CONTINUABLE 로 실행 하나마다 다시 도는데, 청크마다 시각을
                    // 다시 잡으면 드레인이 길어질수록 기준이 앞으로 밀린다. 그러면
                    // **시작 때 대상이 아니던 검증이 컷오프 안으로 들어와** 그 입력
                    // (asof_state)이 걷힌다 — 그 실행은 V1·V3·V5 에서 빈 상태를 읽고
                    // 예외 없이 조용히 틀린 답을 낸다. 지금까지는 verifyJob 프로브가
                    // 가려 줬을 뿐이고, 그것은 손 트리거 검증에는 안 걸린다.
                    LocalDateTime abandonedBefore = frozenCutoff(context, ABANDONED_CUTOFF_KEY,
                            () -> timeProvider.now().minusHours(abandonedAfterHours));
                    long doneUpTo = context.getLong(DONE_UP_TO_KEY, 0);
                    Optional<Long> next =
                            nextTarget(cleanup, keepRuns, abandonedBefore, doneUpTo);
                    if (next.isEmpty()) {
                        log.info("검증 파생 행을 걷었습니다. 실행={}개 asof_state={}행 findings={}행",
                                context.getLong(RUNS_PURGED_KEY, 0),
                                context.getLong(ASOF_STATE_ROWS_KEY, 0),
                                context.getLong(FINDING_ROWS_KEY, 0));
                        return RepeatStatus.FINISHED;
                    }

                    // **시각 창 하나에 파괴적 삭제를 맡기지 않는다.** abandoned-after-hours 의
                    // 근거는 "300만 전수의 소요를 아직 안 쟀다"(docs/13 §6 의 D) 뿐이라,
                    // 실제 소요가 그 값을 넘는 날 **도는 검증의 입력**이 걷힌다. 그때
                    // V1·V3·V5 는 빈 상태를 읽고 예외 없이 **조용히 틀린 답**을 낸다.
                    // 배치 메타에 같은 질문을 하는 수단이 이미 있으므로 그쪽을 먼저 본다.
                    //
                    // 매 청크마다 본다. 도중에 검증이 뜨면 거기서 멈추는 편이 맞고, 커밋된
                    // 데까지는 남아 다음 날이 이어받는다 — 지우는 일은 멱등이다.
                    List<Long> verifying = runningJobs.blockingExecutions(VerifyJobConfig.JOB_NAME);
                    if (!verifying.isEmpty()) {
                        long purged = context.getLong(ASOF_STATE_ROWS_KEY, 0)
                                + context.getLong(FINDING_ROWS_KEY, 0);
                        log.warn("검증이 도는 중이라 정리를 여기서 멈춥니다. 디스크는 하루 더 "
                                        + "기다릴 수 있지만 도는 검증의 입력은 되돌릴 수 없습니다. "
                                        + "verifyExecutionIds={} 이번에 걷은 행={}",
                                verifying, purged);
                        // **로그는 감시 수단이 아니다.** 그냥 FINISHED 로 물러나면 잡이
                        // COMPLETED 로 닫혀 cy_batch_last_success_seconds 가 갱신되고,
                        // 걷을 것이 남았는데 CleanupNotSucceeding 이 영원히 조용하다 —
                        // chunk-size=0 을 기동 때 거절한 근거와 관측상 똑같은 상태다.
                        // 그래서 배치 메타에 사실을 남긴다: 상태는 COMPLETED(실패가 아니다),
                        // 종료 코드는 YIELDED. BatchRunMetricsRefresher 가 그 코드를 성공에서
                        // 뺀다. statsAggregateStep 이 같은 이유로 ExitStatus("SKIPPED") 를 쓴다.
                        //
                        // **얼마나 걷고 멈췄는지를 설명에 싣는다.** 코드 하나로는 "한 행도 못
                        // 걷었다" 와 "200만 걷고 멈췄다" 가 같은 값이라, 그 구분을 배치 메타가
                        // 지게 한다 — 로그에만 두면 되짚을 때 남아 있지 않다.
                        contribution.setExitStatus(new ExitStatus(YIELDED_EXIT_CODE,
                                "verifyExecutionIds=" + verifying + " purgedRows=" + purged));
                        return RepeatStatus.FINISHED;
                    }

                    long runId = next.get();
                    int deleted = cleanup.deleteAsOfStateChunk(runId, chunkSize);
                    if (deleted > 0) {
                        add(context, ASOF_STATE_ROWS_KEY, deleted);
                        add(context, CURRENT_RUN_ROWS_KEY, deleted);
                        contribution.incrementWriteCount(deleted);
                        return RepeatStatus.CONTINUABLE;
                    }

                    // 이 실행의 asof_state 가 다 걷혔다. 나머지 둘은 건수가 작아 한 번에 지우고,
                    // 그때 비로소 진도를 옮긴다 — 셋이 같은 트랜잭션에서 끝나야 "걷은 실행" 의
                    // 뜻이 셋 모두를 덮는다.
                    int findings = cleanup.deleteFindings(runId);
                    // 통계 셋은 이미 있는 계약을 쓴다 — 세 테이블의 FK 순서를 아는 것이
                    // 그쪽이고, 여기서 다시 적으면 한쪽만 고치는 실수가 열린다.
                    stats.clear(runId);

                    add(context, FINDING_ROWS_KEY, findings);
                    // **재방문은 "걷었다" 로 세지 않는다.** 대상 집합은 파생 행이 남아 있는지를
                    // 안 보므로 이미 빈 실행도 매 밤 한 번씩 다시 들른다(아래 javadoc).
                    // 그때까지 세면 아무것도 안 지운 밤에 "실행 40개를 걷었습니다" 가 찍히는데,
                    // 사고를 되짚을 때 유일한 단서라 사실이어야 한다.
                    if (context.getLong(CURRENT_RUN_ROWS_KEY, 0) > 0 || findings > 0) {
                        add(context, RUNS_PURGED_KEY, 1);
                    }
                    context.putLong(CURRENT_RUN_ROWS_KEY, 0);
                    context.putLong(DONE_UP_TO_KEY, runId);
                    contribution.incrementWriteCount(findings);
                    return RepeatStatus.CONTINUABLE;
                }, transactionManager)
                .transactionAttribute(timeout)
                .build();
    }

    /**
     * <b>컷오프를 첫 호출 값에 얼린다.</b> 두 Step 이 같은 구현을 쓴다 — 한때 각자 인라인으로
     * 갖고 있었고, 그러다 Step 1 만 얼리는 것을 빠뜨렸다(CY-686 이 그 자리를 닫았다).
     *
     * <p><b>왜 뽑았나.</b> 인라인이면 <b>{@code if} 만 지우는 회귀가 통합 테스트를 통과한다</b> —
     * 매 호출 {@code put} 해도 키는 있고, 고정 시계 하네스에서는 값도 같기 때문이다. 실제로
     * 그 돌연변이가 살아남는 것을 확인했다. 여기로 뽑으면 {@code compute} 호출 횟수를 셀 수
     * 있어 그 회귀가 유닛 테스트에서 죽는다.
     *
     * <p><b>범위는 한 {@code StepExecution} 이다.</b> 두 호출부 모두 Step 문맥을 넘기므로
     * 두 Step 은 칸이 갈려 있다 — <b>키가 같아도 안 섞인다.</b> 키를 다르게 둔 것은 읽는
     * 사람을 위해서지 충돌 방지가 아니다. {@code ExecutionContextPromotionListener} 로 Job
     * 문맥에 올리지 마라 — 그 순간 둘이 한 칸을 다툰다.
     *
     * <p><b>재시작은 없다.</b> {@link #DONE_UP_TO_KEY} 가 적은 대로 {@code CleanupScheduler}
     * 가 매 발화에 {@code firedAt} 을 실어 새 인스턴스를 만든다. 설령 재시작이 일어나도
     * 얼린 컷오프는 나중 값보다 항상 일러서 두 질의 모두 <b>덜 지우는</b> 쪽이다
     * (대상 축소 · 보호 확대).
     */
    static LocalDateTime frozenCutoff(ExecutionContext context, String key,
            Supplier<LocalDateTime> compute) {
        if (!context.containsKey(key)) {
            context.put(key, compute.get().toString());
        }
        return LocalDateTime.parse(context.getString(key));
    }

    /**
     * 아직 안 끝낸 것 중 가장 오래된 실행. <b>id 오름차순</b>이라 진도 하나로 표현된다.
     *
     * <p>두 집합을 합친다. 겹칠 수 있고(오래된 버려진 실행), 합집합이라 두 번 지우지 않는다.
     */
    private static Optional<Long> nextTarget(CleanupRepository cleanup, int keepRuns,
            LocalDateTime abandonedBefore, long doneUpTo) {
        Set<Long> targets =
                new LinkedHashSet<>(cleanup.purgeableRunIds(keepRuns, abandonedBefore));
        targets.addAll(cleanup.abandonedRunIds(abandonedBefore));
        return targets.stream().filter(id -> id > doneUpTo).min(Long::compareTo);
    }

    private static void add(ExecutionContext context, String key, long delta) {
        context.putLong(key, context.getLong(key, 0) + delta);
    }

    /**
     * <b>배치 메타를 걷는다.</b> {@code BATCH_JOB_EXECUTION} 은 정리 경로가 없어 상한 없이
     * 자랐다 — {@code BatchRunMetricsRefresher}·{@code ExpirePendingRefresher} 가 조회에
     * <b>7일 창을 건 이유가 그것</b>이다. (CY-338 의 관리 화면 이력 조회도 같은 압력을 받게
     * 되는데 <b>그 코드는 아직 저장소에 없다</b> — {@code V15} 헤더가 그 사실을 적어 뒀다.)
     *
     * <p><b>검증 정리 뒤에 와야 한다. 근거는 하나다</b> — {@code SimpleJob} 이 잡 종료
     * 상태를 <b>마지막 Step 의 것으로 덮는다.</b> 그래서 뒤 Step 이 앞 Step 의
     * {@code YIELDED} 를 이어받지 않으면 <b>아무것도 안 한 주기가 {@code COMPLETED} 로
     * 닫혀</b> {@code BatchRunMetricsRefresher} 의 마지막 성공 시각을 민다.
     * {@code .split()} 으로 병렬화하면 그 이어받기가 깨진다.
     *
     * <p>⚠️ 한때 여기 <i>"뒤 Step 이 그 순간 도는 검증의 실행 이력을 지운다"</i> 라고
     * 적었는데 <b>거짓이다</b> — 도는 실행은 {@code END_TIME} 이 {@code NULL} 이라 애초에
     * 대상이 아니다. <b>도는 검증을 지키는 것은 이 순서가 아니라 그 술어다.</b>
     *
     * <p>실패하면 잡이 {@code FAILED} 라 {@code asof_state} 를 다 걷었어도 마지막 성공
     * 시각이 안 갱신된다 — 두 축을 한 잡에 묶은 대가이고, 그래서
     * {@code CleanupNotSucceeding} 의 runbook 이 Step 을 갈라 보라고 안내한다.
     *
     * <p><b>끝난 실행만 지운다.</b> {@code END_TIME} 이 {@code NULL} 인 행 — 실행 중이거나
     * 종료 표시를 못 남기고 죽은 행 — 은 안 건드린다. 시체를 지우면
     * {@code BatchStuckExecution} 이 조용해지는데 그건 고친 게 아니라 <b>증거를 지운
     * 것</b>이고, 그 행은 CY-429 의 복구 API 가 사람의 판단으로 닫는다.
     *
     * <p><b>인스턴스는 실행을 다 지운 뒤에 고아만 지운다.</b> 인스턴스가 먼저 없어지면
     * 남은 실행의 잡 이름을 잃어 되읽기 조회가 통째로 못 찾는다.
     *
     * <p>청크 모양은 앞 Step 과 같다 — {@code CONTINUABLE} 한 번이 한 트랜잭션이다.
     * 근거는 {@code cleanupJob} 의 javadoc 에 적었다. 격리수준도 같다 — 기본값이다.
     *
     * <p><b>앞 Step 과 달리 청크마다 검증을 다시 보지 않는다.</b> 앞 Step 의 양보는 <i>도는
     * 검증의 입력({@code asof_state})을 지키려는 것</i>이지 자원 양보가 아니고, 이 Step 은
     * 검증 데이터를 아예 안 건드린다. 자원 쪽 걱정(04:10 만료와 04:30 정리가 겹치는 밤)은
     * <b>삭제를 id 하나씩 보내 대상 밖 행을 아예 안 잠그는 것</b>으로 갚았다 — 그 근거와
     * 실측은 {@code CleanupJdbcAdapter#deleteBatchMetadataChunk} 에 있다.
     * <b>남은 구멍</b>: 앞 Step 의 프로브가 {@code verifyJob} 만 보고 {@code expireJob} 은
     * 안 본다. 그건 앞 Step 의 정책이라 이 티켓에서 안 바꾸고 {@code docs/13} 에 남겼다.
     */
    @Bean
    Step purgeBatchMetadataStep(CleanupRepository cleanup, TimeProvider timeProvider) {
        return new StepBuilder("purgeBatchMetadataStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    StepExecution self = chunkContext.getStepContext().getStepExecution();

                    ExecutionContext context = self.getExecutionContext();
                    // **컷오프는 첫 청크가 잡은 것을 끝까지 쓴다.** 청크마다 다시 잡으면
                    // 드레인이 길어질수록 기준이 앞으로 밀려, 한 실행 안에서 "보존 기간" 의
                    // 뜻이 달라진다.
                    // ⚠️ **이 축은 지금 하네스로 못 잰다.** 통합 테스트가 Clock 을 AS_OF 로
                    // 고정해서 청크가 몇 번을 돌든 now() 가 같다 — 이 분기를 지워도 초록이다.
                    // 30일 창에서는 드레인이 몇 분 늘어도 대상이 거의 안 바뀌지만,
                    // metadata-keep-days 를 최소 가까이 내리고 metadata-chunk-size 를
                    // 줄이는 날 뜻이 갈린다.
                    // **형제 Step 도 얼렸다(CY-686).** 앞 Step 의 abandonedBefore 는
                    // ABANDONED_CUTOFF_KEY 로 첫 호출 값을 끝까지 쓴다 — 그쪽이 미는 것은
                    // "지울 배치 메타" 가 아니라 **도는 검증의 입력(asof_state)** 이라
                    // 축이 더 위험했다.
                    LocalDateTime olderThan = frozenCutoff(context, META_CUTOFF_KEY,
                            () -> timeProvider.now().minusDays(metadataKeepDays));

                    PurgedMetadata purged =
                            cleanup.deleteBatchMetadataChunk(olderThan, metadataChunkSize);
                    if (!purged.isEmpty()) {
                        // CleanupRunningTooLong 의 runbook 이 WRITE_COUNT 로 진도를 본다.
                        // 안 올리면 이 Step 이 수만 건을 갈고 있어도 0 으로 보여, 운영자를
                        // "새 Step 이 아무것도 안 한다" 는 정반대 결론으로 보낸다.
                        //
                        // **단위는 잡 실행 수다 — 지운 행 수가 아니다.** 그래야
                        // metadata-chunk-size 와 같은 단위라 나누면 청크 수가 나온다.
                        // 고아 인스턴스를 여기 더하면 청크당 최대 2배가 되어 runbook 의
                        // 나눗셈이 진도를 두 배로 읽는다 — 그 값은 종료 설명이 따로 진다.
                        contribution.incrementWriteCount(purged.executions());
                        context.putLong(META_EXECUTIONS_KEY,
                                context.getLong(META_EXECUTIONS_KEY, 0) + purged.executions());
                        context.putLong(META_INSTANCES_KEY,
                                context.getLong(META_INSTANCES_KEY, 0) + purged.instances());
                        return RepeatStatus.CONTINUABLE;
                    }

                    long purgedExecutions = context.getLong(META_EXECUTIONS_KEY, 0);
                    long purgedInstances = context.getLong(META_INSTANCES_KEY, 0);
                    if (purgedExecutions > 0 || purgedInstances > 0) {
                        log.info("배치 메타를 걷었습니다. 실행={} 고아인스턴스={} 보존={}일",
                                purgedExecutions, purgedInstances, metadataKeepDays);
                    }
                    // **로그는 감시 수단이 아니다.** 앞 Step 이 같은 판단을 했다 —
                    // 되짚을 때 컨테이너 로그는 롤오버돼 있을 수 있고 배치 메타는 남는다.
                    String meta = "metaExecutions=" + purgedExecutions
                            + " metaInstances=" + purgedInstances;

                    // **앞 Step 이 물러났어도 배치 메타는 걷고 나서 물러난다.**
                    // 앞 Step 의 양보는 도는 검증의 입력(asof_state)을 지키려는 것이고,
                    // 이 Step 은 그 데이터를 아예 안 건드린다 — 여기서 같이 멈추면
                    // **손 트리거 검증이 13:30 KST 에 걸친 날마다 그날치 BATCH_* 가 통째로
                    // 안 걷힌다.** 하필 그 조건(앞 Step 에 백로그가 남음)과 메타 백로그가
                    // 큰 시기가 겹치고, 배치 메타 백로그에는 전용 알림이 없다.
                    //
                    // 이어받아야 하는 것은 **작업이 아니라 종료 상태**다 — SimpleJob 이 잡
                    // 종료 상태를 마지막 Step 의 것으로 덮으므로(6.0.4), 여기서 COMPLETED 로
                    // 끝내면 앞 Step 이 물러난 주기가 BatchRunMetricsRefresher 의 마지막
                    // 성공 시각을 밀어 버린다. 그래서 코드는 YIELDED 를 잇고 설명만 합친다.
                    ExitStatus yielded = yieldedFrom(self.getJobExecution());
                    contribution.setExitStatus(yielded != null
                            ? new ExitStatus(yielded.getExitCode(),
                                    yielded.getExitDescription() + " " + meta)
                            // 종료 **코드**는 COMPLETED 그대로 둔다. 바꾸면 SimpleJob 이
                            // 그것을 잡 EXIT_CODE 로 덮어 BatchRunMetricsRefresher 의
                            // YIELDED 필터가 흔들린다.
                            : new ExitStatus(ExitStatus.COMPLETED.getExitCode(), meta));
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .transactionAttribute(timeout)
                .build();
    }

    /**
     * <b>앞 Step 이 {@code YIELDED} 로 끝났나.</b> 그렇다면 그 종료 상태를 <b>메시지까지</b>
     * 그대로 돌려준다 — 코드만 옮기면 <i>"몇 건 걷고 멈췄나"</i> 를 잃는다.
     */
    private static ExitStatus yieldedFrom(JobExecution jobExecution) {
        return jobExecution.getStepExecutions().stream()
                .map(StepExecution::getExitStatus)
                .filter(status -> YIELDED_EXIT_CODE.equals(status.getExitCode()))
                // **가장 마지막 것을 집는다.** findFirst 면 Step 이 셋이 되는 날 3단계가
                // 2단계가 아니라 1단계의 메시지를 보고한다 — 그 메시지가 "몇 행에서
                // 멈췄나" 를 지고 있어서 값이 조용히 틀린다.
                .reduce((earlier, later) -> later)
                .orElse(null);
    }
}
