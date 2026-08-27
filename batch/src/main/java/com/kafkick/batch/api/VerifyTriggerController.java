// 검증 배치를 사람이 돌리고 결과를 조회합니다.
package com.kafkick.batch.api;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import static java.util.stream.Collectors.joining;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.batch.config.ExpireStepContext;
import com.kafkick.batch.config.VerifyExecutorConfig;
import com.kafkick.batch.job.ExpireJobConfig;
import com.kafkick.batch.job.VerifyJobConfig;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.response.ResponseEnvelope;
import com.kafkick.core.verification.exception.VerificationErrorCode;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationRunRepository;
import com.kafkick.core.verification.VerificationRuleRepository;

/**
 * <b>지금까지 검증을 손으로 시작할 방법이 테스트 말고 없었다.</b>
 *
 * <p>경로는 {@code docs/05-design-handoff.md} 가 프론트에 인수인계한 것을 그대로 쓴다.
 * 관리 포트로 옮기면 {@code /actuator/verify} 가 되어 그 계약이 깨진다 — 노출은 compose 의
 * 포트 매핑으로 줄인다. 근거는 {@code docs/15}.
 *
 * <p><b>인증이 없다.</b> PRD 보안 ①이 {@code /api/v1/admin/**} 에 {@code ADMIN} 역할을
 * 요구하지만 batch 에 Spring Security 가 없고, 토큰 규약은 영역 ③의 몫이라 여기서 혼자
 * 정하면 두 벌이 된다. <b>지금 이 API 를 지키는 것은 "밖에서 못 닿는다" 하나뿐이다.</b>
 */
@RestController
@RequestMapping("/api/v1/admin/verify")
public class VerifyTriggerController {

    private static final Logger log = LoggerFactory.getLogger(VerifyTriggerController.class);

    /**
     * <b>버릴 수 있는 상태.</b> {@code STOPPING} 은 신호를 받고 아직 안 멈춘 것,
     * {@code STOPPED} 는 멈춘 것이다 — 하드킬로 잡이 이미 없으면 {@code stop} 한 번에
     * 곧바로 뒤쪽이 된다. {@code FAILED}·{@code COMPLETED} 는 <b>버릴 것이 없고</b>,
     * 덮어쓰면 실행 이력이라는 판정 근거를 바꾸는 일이 된다.
     */
    private static final java.util.Set<BatchStatus> ABANDONABLE =
            java.util.EnumSet.of(BatchStatus.STOPPING, BatchStatus.STOPPED);

    private final JobOperator verifyJobOperator;
    private final Job verifyJob;
    private final JobRepository jobRepository;
    private final VerificationRunRepository runs;
    private final VerificationRuleRepository rules;
    private final TimeProvider timeProvider;
    private final RunningJobProbe runningJobs;
    private final VerifyStopService stopService;

    /**
     * <b>곧 뜰 만료를 보기 위한 것이다.</b> {@code rejectRunningExpire} 는 <i>이미 도는</i>
     * 만료만 보고 <i>곧 뜰</i> 만료는 못 본다 — 그 창이 CY-470 에서 위험해졌다.
     */
    private final CronExpression expireCron;

    /**
     * <b>검증이 정상 상태에서 넘지 않아야 할 시간.</b> 그 안에 만료가 뜨면 겹친다고 본다.
     * {@code VerifyRunningTooLong} 임계와 같은 값을 쓴다 — 그 값이 이미 <i>"정상 상태에서
     * 이보다 오래 걸리면 사람이 본다"</i> 는 선언이라, 두 자리가 한 축 위에 선다.
     *
     * <p>⚠️ <b>보장이 아니라 어림이다.</b> 이 저장소에는 <b>실행 전체를 끊는 수단이 없다</b> —
     * {@code batch.verify.step-timeout-ms} 는 청크 하나의 데드라인이라 잡 전체는
     * {@code 청크 수 × 그 값}만큼 돌 수 있다({@code .example} 의 {@code expire.step-timeout-ms}
     * 주석이 같은 사실을 적는다). 그래서 실측 472초의 2.5배를 넘겨 도는 실행은 이 가드를
     * 통과한 뒤에도 만료와 겹칠 수 있고, 그때는 {@code max-expire-skips=0} 이라 만료가
     * 지나간다.
     *
     * <p><b>그래도 안 하는 것보다 낫다.</b> 이 가드가 없으면 <b>시연 시간대에 누른 검증이
     * 반드시 버려지는데</b>(만료 04:10 UTC = 13:10 KST), 있으면 그 경우가 접수 단계에서
     * 걸러진다. 남은 창 — 정상 상한을 넘겨 도는 실행 — 은 잡 전체 데드라인이 생기는 날
     * 닫힌다. {@code docs/13} 에 그 자리를 남겨 뒀다.
     */
    private final Duration verifyWorstCase;

    public VerifyTriggerController(
            @Qualifier(VerifyExecutorConfig.OPERATOR) JobOperator verifyJobOperator,
            @Qualifier("verifyJob") Job verifyJob,
            JobRepository jobRepository,
            VerificationRunRepository runs,
            VerificationRuleRepository rules,
            TimeProvider timeProvider,
            RunningJobProbe runningJobs,
            VerifyStopService stopService,
            @Value(ExpireStepContext.CRON) String expireCron,
            @Value("${batch.scheduling.enabled:false}") boolean schedulingEnabled,
            @Value("${batch.metrics.verify-running-too-long-seconds:1200}")
            long verifyWorstCaseSeconds) {
        // **스케줄러가 꺼져 있으면 만료가 아예 안 뜬다.** ExpireScheduler 는
        // @ConditionalOnProperty 라 빈 자체가 안 만들어지므로, 크론 값이 유효해도 그 시각에
        // 아무 일도 안 일어난다 — 그 상태에서 이 가드가 409 를 내면 **부하 측정이나 손 검증
        // 때 API 가 통째로 막힌다.**
        //
        // "-" 로 끈 경우도 함께 가른다. 그때는 CronExpression 이 파싱을 못 한다.
        //
        // ⚠️ 문자열 비교가 아니라 boolean 바인딩이다. @Value 의 변환은 1·yes·on 도 참으로
        //    받는데, @ConditionalOnProperty 는 havingValue 와 문자열 비교라 그 셋을 거절한다 —
        //    즉 BATCH_SCHEDULING_ENABLED=1 이면 **스케줄러는 없는데 여기는 켜졌다고 읽는다.**
        //    그 방향(가드가 더 보수적)은 안전하다: 안 뜰 만료 때문에 거절할 뿐 반대는 아니다.
        this.expireCron = schedulingEnabled && CronExpression.isValidExpression(expireCron)
                ? CronExpression.parse(expireCron)
                : null;
        this.verifyWorstCase = Duration.ofSeconds(verifyWorstCaseSeconds);
        this.verifyJobOperator = verifyJobOperator;
        this.verifyJob = verifyJob;
        this.jobRepository = jobRepository;
        this.runs = runs;
        this.rules = rules;
        this.timeProvider = timeProvider;
        this.runningJobs = runningJobs;
        this.stopService = stopService;
    }

    /**
     * <b>202 에 싣는 것은 {@code executionId} 지 {@code runId} 가 아니다.</b>
     * 후자는 {@code startRunStep} 이 가드 여덟을 통과한 뒤에야 만들므로 이 시점에 없다.
     *
     * <p>{@code asOf} 만 필수다. 나머지는 틀려도 <b>잡이 거절</b>하지만, {@code asOf} 는
     * 아무 값이나 성립해서 <b>조용히 다른 것을 판정한다.</b>
     */
    @PostMapping
    public ResponseEntity<ResponseEnvelope<TriggerAccepted>> trigger(
            // ISO.DATE_TIME 을 쓰면 안 된다. 그 포맷터는 오프셋이 붙은 문자열을 파싱에
            // 성공시킨 뒤 지역 부분만 뽑는다 — 2026-03-01T09:00:00Z 가 조용히 09:00 이 된다.
            // JS 의 toISOString() 은 항상 Z 를 붙이므로 프론트가 보내면 KST 기준으로
            // 아홉 시간이 밀리고, 데이터가 조용한 시각이면 가드를 다 통과해 **틀린 시점으로
            // PASS 를 남긴다.** 스키마 시각은 지역시각이라 오프셋을 아예 안 받는다.
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm[:ss][.SSS]")
            LocalDateTime asOf,
            @RequestParam(required = false) DatasetType dataset,
            @RequestParam(required = false) ScopeType scope,
            @RequestParam(required = false) Integer attempt,
            @RequestParam(required = false) Long seedRunId,
            @RequestParam(required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm[:ss][.SSS]") LocalDateTime fromTs) {

        // 잡 안의 rejectRunningExpire 가 어차피 막는다. 여기서 먼저 답하는 것은
        // 클라이언트가 202 를 받아 놓고 폴링해야 원인을 아는 상황을 없애기 위해서다.
        // 진실은 여전히 잡 안에 있다 — 이 검사를 지워도 잡은 거절한다.
        //
        // 예전에는 batch.scheduling.enabled 를 봤다. 그 플래그는 "만료 스케줄러 빈이
        // 만들어졌는가" 라서, 운영처럼 늘 켜 두는 환경에서는 이 API 가 **항상 409** 였다.
        List<Long> runningExpire = runningJobs.blockingExecutions(ExpireJobConfig.JOB_NAME);
        if (!runningExpire.isEmpty()) {
            throw new BusinessException(VerificationErrorCode.VERIFY_EXPIRE_RUNNING,
                    "만료 배치가 실행 중입니다. 그 동안 검증하면 판정 근거가 검증 중에 "
                            + "바뀝니다 — 끝난 뒤 다시 부르십시오. executionIds=" + runningExpire);
        }

        rejectIfExpireIsAboutToFire();

        // 이미 도는 것이 있으면 여기서 끝낸다. 아래 start() 의 catch 로는 못 잡는다 —
        // TaskExecutorJobLauncher 가 TaskRejectedException 을 자기가 삼키고 잡을 FAILED 로
        // 표시한 뒤 **정상 반환**하기 때문이다(바이트코드로 확인). 그러면 클라이언트는
        // 202 를 받고 폴링하다 status:"FAILED" 만 보는데, 진짜 원인은 "앞 실행이 돌고 있다"다.
        rejectIfAlreadyRunning();

        DatasetType resolvedDataset = dataset != null ? dataset : attachedDataset();
        ScopeType resolvedScope = scope != null ? scope : ScopeType.FULL;

        // CORRUPT 는 정답 묶음을 명시해야 한다. 안 주면 startRunStep 이 거절하는데,
        // 그때는 이미 202 를 준 뒤라 클라이언트가 원인을 폴링으로 찾아야 한다.
        // 그리고 그 실패가 attempt 를 태운다 — 접수 단계에서 끝내는 편이 훨씬 싸다.
        if (resolvedDataset == DatasetType.CORRUPT && seedRunId == null) {
            throw new BusinessException(VerificationErrorCode.INVALID_RUN_PARAMS,
                    "dataset=CORRUPT 에는 seedRunId 가 필요합니다. 정답 묶음이 둘 이상인 DB 에서 "
                            + "기본값을 두면 낡은 묶음과 조용히 대조합니다.");
        }

        // 미래 asOf 는 잡 안의 validateAsOfNotInFuture 가 막는다. 여기서 먼저 막는 것은
        // **그 실패가 attempt 를 태우기 때문**이다 — 배치 메타에 인스턴스가 남아 같은
        // 파라미터로는 두 번 다시 못 부른다. fromTs·seedRunId 와 같은 정책이다.
        //
        // 컨트롤러가 현재 시각을 보는 것은 결정론을 안 깬다. 잡에 실리는 asOf 는 요청값
        // 그대로이고, 잡 안에서는 여전히 실행 시작 시각으로 판정한다.
        //
        // **잡 안의 가드와 같은 좌표계다.** 잡 쪽 기준인 verification_runs.started_at 을
        // CY-397 이 TimeProvider(Clock.systemUTC)로 옮겨서, 이 검사와
        // validateAsOfNotInFuture 의 엄격도가 같아졌다. 그전에는 잡 쪽이 JVM 기본 존이라
        // KST 기기에서 아홉 시간 느슨했고, 그때는 여기가 유일한 실질 방어였다.
        //
        // 그래도 여기서 먼저 막는 이유는 정확성이 아니라 **실패가 attempt 를 태우기 때문**이다.
        // 배치 메타 자신의 시각(START_TIME 등)은 여전히 JVM 기본 존이지만 이 판정에 안 쓰이고,
        // 그 존은 batch.yml 의 TZ=UTC 와 batch/build.gradle 의 bootRun user.timezone 이 고정한다.
        if (asOf.isAfter(timeProvider.now())) {
            throw new BusinessException(VerificationErrorCode.INVALID_AS_OF,
                    "검증 기준 시각은 미래일 수 없습니다. asOf=" + asOf);
        }

        // 같은 논거다. attempt 는 잡 안의 validateAttempt 가 1 미만을 거절하는데,
        // 열린 입력은 "쓸 수 있다" 는 신호라 운영자가 0 을 시도하고 그 시도가 번호를 태운다.
        if (attempt != null && attempt < 1) {
            throw new BusinessException(VerificationErrorCode.INVALID_RUN_PARAMS,
                    "attempt 는 1 이상이어야 합니다. 시드가 CLEAN 1·2 · CORRUPT 1 을 "
                            + "점유하므로 배치는 그 뒤 번호를 씁니다. attempt=" + attempt);
        }

        // fromTs 는 성공 조합이 없다. FULL 이면 VerificationRun 생성자가 거부하고,
        // 유일한 대안인 INCREMENTAL 은 rejectUnsupportedScope 가 막는다. 열린 입력은
        // "쓸 수 있다" 는 신호라 운영자가 시도하고, 그 시도가 attempt 를 태운다.
        if (fromTs != null) {
            throw new BusinessException(VerificationErrorCode.INVALID_RUN_PARAMS,
                    "fromTs 는 증분 검증 전용인데 증분은 아직 지원하지 않습니다. "
                            + "전수 검증에는 시작 시각을 지정할 수 없습니다.");
        }

        int resolvedAttempt = attempt != null
                ? attempt
                : runs.nextAttempt(asOf, resolvedDataset, resolvedScope);

        JobParametersBuilder parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", asOf)
                .addString("dataset", resolvedDataset.name())
                .addString("scope", resolvedScope.name())
                .addLong("attempt", (long) resolvedAttempt);
        if (seedRunId != null) {
            // **비식별**이어야 한다. 식별로 넣으면 uk_run_params 의 네 축과 어긋나
            // 엉뚱한 이유로 죽는다 — VerifyJobConfig 가 그것을 따로 거절한다.
            parameters.addLong("seedRunId", seedRunId, false);
        }

        JobExecution execution = start(parameters.toJobParameters());
        log.info("검증 배치를 접수했습니다. executionId={} asOf={} dataset={} scope={} attempt={}",
                execution.getId(), asOf, resolvedDataset, resolvedScope, resolvedAttempt);

        return ResponseEntity.accepted()
                .body(ResponseEnvelope.success(new TriggerAccepted(execution.getId(), asOf,
                        resolvedDataset, resolvedScope, resolvedAttempt)));
    }

    /**
     * <b>지금 도는 검증 실행.</b> 429 를 받은 클라이언트가 <b>무엇이 막고 있는지</b> 알아야
     * {@code stop}·{@code abandon} 을 부를 수 있다.
     *
     * <p>그 id 를 429 응답 본문에 실을 수는 없다 — 저장소 규약이 <i>"클라이언트에 나가는
     * 문구는 {@code errorCode.getMessage()}"</i> 로 못 박았고, 이 API 에는 인증이 없어
     * 자유 문장에 내부 값을 담지 않는 편이 맞다. 그래서 <b>조회 경로를 따로 연다.</b>
     *
     * <p>하드킬로 남은 실행도 여기 보인다 — 그것이 이 엔드포인트의 주 사용처다.
     */
    @GetMapping("/runs/running")
    public ResponseEnvelope<List<Long>> running() {
        return ResponseEnvelope.success(runningExecutions().stream().sorted().toList());
    }

    /**
     * <b>{@code runId} 가 {@code null} 인 것도 정보다.</b> 아직 판정 단계에 못 갔거나,
     * 가드에 걸려 <b>끝까지 못 가는</b> 실행이라는 뜻이다.
     */
    @GetMapping("/runs/{executionId}")
    public ResponseEnvelope<VerifyRunView> find(@PathVariable long executionId) {
        JobExecution execution = requireVerifyExecution(executionId);

        Long runId = execution.getExecutionContext().containsKey(VerifyJobConfig.RUN_ID_KEY)
                ? execution.getExecutionContext().getLong(VerifyJobConfig.RUN_ID_KEY)
                : null;

        return ResponseEnvelope.success(VerifyRunView.of(executionId, execution, runId,
                runId == null ? Optional.empty() : runs.findById(runId)));
    }

    /**
     * <b>중단은 요청이지 완료가 아니다.</b> Spring Batch 는 플래그를 세우고, 청크 경계에서
     * 잡이 그것을 보고 멈춘다 — 그래서 200 이 아니라 202 다. 실제로 멈췄는지는 조회로 본다.
     */
    @PostMapping("/runs/{executionId}/stop")
    public ResponseEntity<ResponseEnvelope<StopRequested>> stop(@PathVariable long executionId) {
        // **진도가 멈춘 실행만 멈춘다.** 아래 requireStuck 참고 — 도는 검증을 여기서 끊으면
        // 472초가 버려지는 데서 끝나지 않고, 만료·정리와의 상호 배제가 그 자리에서 꺼진다.
        // 이 실행이 verifyJob 인지 먼저 본다. 중단 신호는 **공유 JobRepository** 에 쓰이므로
        // 어느 operator 로 시작했든 멈춘다 — 검사가 없으면 /verify/ 경로가 expireJob 을
        // 멈출 수 있다. 만료는 재고를 쓰는 유일한 잡이고, 중간에 멈추면 다음 검증의
        // 판정 근거가 흔들린다. 둘은 같은 BATCH_JOB_EXECUTION 시퀀스를 공유해 번호가 섞인다.
        // 판정과 쓰기를 한 트랜잭션으로 묶어야 해서 서비스로 내렸다. 그 사이에 실행이
        // 되살아나면(락 대기가 풀리는 경우) 살아 있는 검증을 STOPPED 로 올리게 된다 —
        // ExpireRecoveryService 가 만료 쪽에서 같은 이유로 선점문을 쓴다.
        VerifyStopService.Outcome outcome = stopService.stop(executionId);
        return ResponseEntity.accepted()
                .body(ResponseEnvelope.success(
                        new StopRequested(outcome.executionId(), outcome.signalled())));
    }



    /**
     * <b>없는 실행과 남의 잡을 같은 404 로 접는다.</b>
     *
     * <p>{@code getJobExecution} 은 없는 id 에 {@code null} 이 아니라
     * {@code EmptyResultDataAccessException} 을 던진다 — 그 조회의 첫 줄
     * ({@code getJobInstanceId} 의 {@code queryForObject})이 try 블록 밖이라
     * DAO 가 안 잡는다. {@code null} 검사만 두면 그것이 500 으로 새어 나가
     * <i>"없다"</i> 와 <i>"서버가 깨졌다"</i> 가 클라이언트에게 같아진다.
     *
     * <p>잡 이름이 다르면 <b>있어도 없다고 답한다.</b> {@code expireJob} 의 실행을
     * 이 경로로 조회하면 {@code runId: null} 이 나와 <i>"판정 단계에 못 간 검증"</i> 처럼
     * 보이는데, 그건 거짓이다.
     */
    private JobExecution requireVerifyExecution(long executionId) {
        JobExecution execution;
        try {
            execution = jobRepository.getJobExecution(executionId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            throw notFound(executionId);
        }
        if (execution == null
                || !VerifyJobConfig.JOB_NAME.equals(execution.getJobInstance().getJobName())) {
            throw notFound(executionId);
        }
        return execution;
    }

    /**
     * <b>도는 것이 있으면 접수 자체를 거절한다.</b> 실행기의 거절 예외는 컨트롤러까지
     * 안 온다 — {@code TaskExecutorJobLauncher} 가 {@code TaskRejectedException} 을 잡아
     * 잡을 {@code FAILED} 로 표시하고 정상 반환한다. 그러면 429 가 죽은 코드가 되고,
     * 클라이언트는 202 를 받아 놓고 <i>왜 실패했는지 모르는 채</i> 폴링한다.
     */
    /**
     * <b>곧 뜰 만료와 겹칠 접수를 거절한다.</b>
     *
     * <p>{@code max-expire-skips} 가 <b>0</b> 이 된 뒤로(CY-470) 만료는 검증을 <b>건너뛰지
     * 않고 지나간다.</b> 그때 찍히는 {@code issuances.updated_at} 을 {@code assertFrozenStep} 의
     * {@code rejectIssuancesUpdatedAfterAsOf} 가 보고 실행을 죽이는데, <b>그 {@code asOf} 는
     * 영구히 못 쓴다</b> — 만료가 찍은 시각은 지워지지 않는다.
     *
     * <p>그 손잡이를 0 으로 내린 근거는 <i>"일정 분리가 겹침을 막는다"</i> 였고, 그것은
     * <b>크론끼리만</b> 참이다. 손 트리거 경로는 시각을 안 가리므로 방어가 통째로 사라졌다 —
     * 하필 만료 04:10 UTC 가 <b>13:10 KST</b> 라 시연 준비 시간대와 겹친다.
     * 규약을 문서에만 두면 그 시각에 누르는 사람을 아무것도 막지 못한다.
     *
     * <p><b>{@code rejectRunningExpire} 와 다른 축이다.</b> 그쪽은 <i>이미 도는</i> 만료를
     * 배치 메타에서 보고, 이쪽은 <i>곧 뜰</i> 만료를 크론에서 본다. 둘 다 있어야 창이 닫힌다.
     *
     * <p>거절 대신 기다리지 않는다 — 최악 8분을 HTTP 요청이 붙잡고 있을 수 없고,
     * <b>언제 다시 부르면 되는지</b>를 응답에 실어 주는 편이 낫다.
     */
    private void rejectIfExpireIsAboutToFire() {
        if (expireCron == null) {
            // 스케줄러가 꺼졌거나 만료 크론이 "-" 다. 어느 쪽이든 만료가 안 떠서 충돌 자체가
            // 없다 — 생성자가 그 판정을 이미 했다.
            return;
        }
        LocalDateTime now = timeProvider.now();
        LocalDateTime nextExpire = expireCron.next(now);
        if (nextExpire == null) {
            // 만료 크론이 앞으로 안 돈다. 겹칠 것이 없다.
            return;
        }
        Duration untilExpire = Duration.between(now, nextExpire);
        if (untilExpire.compareTo(verifyWorstCase) >= 0) {
            return;
        }
        // 전용 코드를 쓴다. VERIFY_EXPIRE_RUNNING 과 상태 코드는 같지만 **처방이 다르다** —
        // 그쪽은 "만료가 끝나길 기다려라" 이고 이쪽은 "배치 창을 지난 뒤에 불러라" 다.
        // 응답에 나가는 것은 고정 문구뿐이라(핸들러가 detail 을 로그에만 남긴다) 코드를
        // 안 가르면 운영자가 같은 시각에 또 누른다.
        throw new BusinessException(VerificationErrorCode.VERIFY_EXPIRE_ABOUT_TO_FIRE,
                "만료 크론이 " + untilExpire.toSeconds() + "초 뒤(" + nextExpire
                        + " UTC)에 뜹니다. max-expire-skips=0 이라 만료는 이 검증을 "
                        + "건너뛰지 않고 지나가며, 그때 찍히는 updated_at 때문에 이 asOf 로는 "
                        + "다시 못 잽니다(재시딩 말고 복구가 없습니다). "
                        + nextExpire.plus(verifyWorstCase) + " UTC 이후로 다시 부르십시오.");
    }

    private void rejectIfAlreadyRunning() {
        Set<Long> running = runningExecutions();
        if (!running.isEmpty()) {
            // 막고 있는 id 는 detail(로그용)에만 남긴다. 클라이언트는 GET /runs/running 으로
            // 얻는다 — 규약이 응답 문구를 카탈로그 메시지로 제한한다.
            throw new BusinessException(VerificationErrorCode.VERIFY_ALREADY_RUNNING,
                    "앞 실행이 아직 돌고 있습니다: executionId="
                            + running.stream().map(String::valueOf).collect(joining(",")));
        }
    }

    /** 지금 도는 검증 실행의 {@code executionId}. 하드킬로 남은 행도 여기 잡힌다. */
    private Set<Long> runningExecutions() {
        // JobOperator.getRunningExecutions(String) 은 Spring Batch 6 에서 제거 예정이다.
        // JobRepository 쪽이 대체이고, 레지스트리에 이름이 없어도 안 던져서 갈래가 하나 준다.
        //
        // 이것이 보는 것은 **DB 의 STATUS IN ('STARTING','STARTED','STOPPING')** 이다 —
        // 그래서 하드킬로 남은 행이 여기 잡히고, abandon 이 그 해제 경로가 된다.
        return jobRepository.findRunningJobExecutions(VerifyJobConfig.JOB_NAME).stream()
                .map(JobExecution::getId)
                .collect(java.util.stream.Collectors.toSet());
    }

    /**
     * <b>"Step 0개 FAILED" 는 실행기 거절의 필요조건이지 충분조건이 아니다.</b>
     * 첫 Step 진입 전에 죽는 다른 경로가 있다 — {@code beforeJob} 리스너가 던지거나,
     * {@code JobRepository} 갱신이 커넥션 문제로 실패하는 경우다.
     *
     * <p>그때도 429 를 주면 <b>도는 것이 없는데 "돌고 있다" 고 답하게 된다.</b> 429 는
     * 재시도 가능이라는 뜻이라 운영자는 기다리고, 기다릴수록 상태는 안 바뀐다. 그래서
     * 종료 설명에 거절 흔적이 있는지 확인한다 — 없으면 202 로 넘겨 조회가 원인을 말하게 한다.
     */
    private static boolean wasRejectedByExecutor(JobExecution execution) {
        String description = execution.getExitStatus().getExitDescription();
        return description != null
                && (description.contains("TaskRejectedException")
                        || description.contains("RejectedExecutionException"));
    }

    private static BusinessException notFound(long executionId) {
        return new BusinessException(VerificationErrorCode.VERIFY_EXECUTION_NOT_FOUND,
                "executionId=" + executionId);
    }

    /**
     * <b>하드킬로 남은 실행을 걷어낸다.</b>
     *
     * <p>{@code getRunningExecutions} 는 인메모리가 아니라 <b>DB 의
     * {@code STATUS IN ('STARTING','STARTED','STOPPING')}</b> 을 본다. 그래서 SIGKILL·OOM 으로
     * 프로세스가 죽으면 {@code STARTED} 행이 {@code END_TIME} 이 {@code NULL} 인 채 영구히
     * 남고, <b>그 뒤 모든 트리거가 429 가 된다.</b> 재기동으로도 안 풀린다.
     *
     * <p>⚠️ <b>여기 한때 <i>"{@code stop} 으로는 못 푼다 — {@code STOPPING} 이 되어 목록에
     * 그대로 있다"</i> 고 적혀 있었다. 틀렸다.</b> Spring Batch 6.0.4 바이트코드로 확인했다 —
     * {@code SimpleJobOperator.stop} 이 {@code setEndTime(now())} 까지 하고,
     * {@code SimpleJobRepository.update} 가 <i>"Upgrading job execution status from STOPPING
     * to STOPPED since it has already ended"</i> 로 <b>즉시 {@code STOPPED} 로 올린다.</b>
     * 그래서 {@code stop} 한 번으로 위 목록에서 빠지고 429 도 풀린다.
     *
     * <p>그럼에도 <b>{@code stop} → {@code abandon} 2단계</b>다. 둘의 뜻이 다르다 —
     * {@code STOPPED} 는 <i>"멈췄다"</i> 이고 {@code ABANDONED} 는 <i>"이 실행은 없던 것으로
     * 한다"</i> 이다. {@code abandon} 은 {@code STARTED} 를 거부하므로(살아 있을지도 모르는
     * 것을 함부로 버리지 않는다) 먼저 {@code stop} 을 거친다.
     *
     * <p><b>이것은 정상 절차가 아니라 복구다.</b> 도는 잡을 버리면 그 실행이 남긴
     * {@code verification_runs} 행은 {@code verdict} 없이 열린 채 남는다 — 되읽기가 그것을
     * 안 집는 것이 맞고({@code verdict IS NOT NULL}), 판정으로도 안 센다.
     */
    @PostMapping("/runs/{executionId}/abandon")
    public ResponseEntity<ResponseEnvelope<Abandoned>> abandon(@PathVariable long executionId) {
        JobExecution execution = requireVerifyExecution(executionId);

        // **중단된 것만 버린다.** Spring Batch 는 status.isLessThan(STOPPING) 일 때만
        // 거부한다. BatchStatus 순서가 COMPLETED(0)·STARTING(1)·STARTED(2)·STOPPING(3)·
        // STOPPED(4)·FAILED(5)·ABANDONED(6) 이라, 통과하는 것은 **STOPPING·STOPPED·FAILED·
        // ABANDONED 넷**이고 우리는 뒤의 둘을 막는다 — COMPLETED 는 프레임워크가 이미 막는다(6.0.4 에서 직접 찍었다. 한때 여기
        // 주석이 COMPLETED 도 통과한다고 적었는데 거짓이었고, CY-429 가 같은 문장을
        // 복제한 뒤 돌연변이 테스트가 잡았다). 그대로 두면 실패 이력을 ABANDONED 로
        // 덮어쓰고 END_TIME 을 현재로 다시 쓴다. 이 저장소는
        // 실행 이력을 판정 근거로 삼으므로(docs/11) 그것은 증거를 조용히 바꾸는 일이다.
        //
        // STOPPING 과 STOPPED 를 둘 다 받는다. stop 은 도는 잡이 있으면 STOPPING 을
        // 남기고 청크 경계에서 STOPPED 로 가지만, **하드킬로 잡이 이미 없으면 곧바로
        // STOPPED** 가 된다(실측). 후자가 이 엔드포인트의 주 사용처다.
        //
        // 끝난 실행에는 버릴 것도 없다 — 그것은 트리거를 막지 않는다.
        if (!ABANDONABLE.contains(execution.getStatus())) {
            throw new BusinessException(VerificationErrorCode.VERIFY_NOT_ABANDONABLE,
                    "중단된 실행만 버릴 수 있습니다. 지금=" + execution.getStatus()
                            + ". 도는 실행이면 먼저 stop 을 부르고, 이미 끝난 실행이면 "
                            + "버릴 것이 없습니다(트리거를 막지 않습니다). executionId=" + executionId);
        }
        try {
            verifyJobOperator.abandon(execution);
            log.warn("검증 실행을 버렸습니다. 하드킬로 남은 행을 걷어내는 복구 절차입니다. "
                    + "executionId={}", executionId);
            // stop 과 달리 **완료 동작**이라 200 이다. 202 + StopRequested 를 재사용하면
            // "신호만 보냈다" 는 그쪽 뜻이 여기서는 거짓이 된다.
            return ResponseEntity.ok(ResponseEnvelope.success(
                    new Abandoned(executionId, BatchStatus.ABANDONED.name())));
        } catch (org.springframework.batch.core.launch.JobExecutionAlreadyRunningException e) {
            // 위 검사와 이 호출 사이에 상태가 바뀐 경우다. 같은 답을 준다.
            throw new BusinessException(VerificationErrorCode.VERIFY_NOT_ABANDONABLE,
                    "먼저 stop 으로 중단 신호를 보내십시오. executionId=" + executionId, e);
        }
    }

    /**
     * <b>붙어 있는 스키마가 곧 데이터셋이다.</b> {@code VerificationMetrics}·
     * {@code CleanSchemaGuard}·{@code rejectDatasetMismatch} 가 쓰는 <b>같은 사실</b>을 본다 —
     * 넷이 각자 판정하면 어긋나는 날 어느 쪽이 맞는지 아무도 모른다.
     */
    private DatasetType attachedDataset() {
        return rules.hasCleanOnlyConstraints() ? DatasetType.CLEAN : DatasetType.CORRUPT;
    }

    private JobExecution start(JobParameters parameters) {
        try {
            JobExecution execution = verifyJobOperator.start(verifyJob, parameters);
            // rejectIfAlreadyRunning 과 여기 사이에 다른 요청이 끼면 실행기가 거절하는데,
            // 그 예외는 런처가 삼키고 Step 을 하나도 안 만든 채 FAILED 를 준다.
            // 그 모양으로 그것을 알아본다 — 202 를 주면 클라이언트가 헛폴링한다.
            if (execution.getStatus() == BatchStatus.FAILED
                    && execution.getStepExecutions().isEmpty()
                    && wasRejectedByExecutor(execution)) {
                throw new BusinessException(VerificationErrorCode.VERIFY_ALREADY_RUNNING,
                        "실행기가 거절했습니다. 앞 실행이 아직 돌고 있습니다.");
            }
            return execution;
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // TaskRejectedException 이 이 타입을 상속하므로 하나로 잡힌다.
            // 실행기가 스레드 하나 · 큐 없음이다. 미루지 않고 거절하는 것이 설계다 —
            // 큐에 넣으면 그 사이 asOf 가 지나가 접수 시점과 다른 데이터를 본다.
            throw new BusinessException(VerificationErrorCode.VERIFY_ALREADY_RUNNING,
                    "실행기는 스레드 하나이고 큐가 없습니다. 앞 실행이 끝난 뒤 다시 부르십시오.");
        } catch (org.springframework.batch.core.launch.JobExecutionAlreadyRunningException
                | org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException e) {
            // 같은 파라미터로 이미 돌았거나 돌고 있다. attempt 를 올리면 새 인스턴스다.
            throw new BusinessException(VerificationErrorCode.INVALID_RUN_PARAMS,
                    e.getMessage(), e);
        } catch (org.springframework.batch.core.job.parameters.InvalidJobParametersException
                | org.springframework.batch.core.launch.JobRestartException e) {
            throw new BusinessException(VerificationErrorCode.INVALID_RUN_PARAMS,
                    e.getMessage(), e);
        }
        // BusinessException 은 위 어느 catch 타입에도 안 걸려 그대로 전파된다 —
        // 핸들러가 도메인 에러코드로 옮긴다. 다시 던지는 절은 죽은 코드라 두지 않는다.
    }
}
