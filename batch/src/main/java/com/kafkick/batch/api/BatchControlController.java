// 배치 실행을 사람이 다시 돌리거나 멈춥니다.
package com.kafkick.batch.api;

import java.time.Duration;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.batch.config.BatchJobRepositoryConfig;
import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.core.batch.exception.BatchControlErrorCode;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.response.ResponseEnvelope;

/**
 * 실패한 배치를 <b>다시 돌리고</b>, 오래 도는 배치를 <b>멈춘다.</b>
 *
 * <h2>왜 api 가 아니라 여기인가</h2>
 *
 * <p>{@code api} 모듈에는 <b>spring-batch 가 없다</b>(실측: {@code api/build.gradle} 에
 * 그 의존이 없다). {@code JobOperator} 도 잡 정의도 <b>이 프로세스에만</b> 있으므로,
 * 관제 화면이 이 동작을 부르려면 여기로 온다. 읽기(이력·스텝·파라미터)는 관측 풀로
 * 나가므로 api 에서도 되지만, <b>쓰기는 잡이 사는 곳에서만 된다.</b>
 *
 * <h2>가드를 새로 짜지 않았다</h2>
 *
 * <p>{@code JobOperator} 시그니처를 실측하니 위험한 전이를 <b>대부분</b> 이미 거절한다 —
 * 안 도는 실행 중단은 {@code JobExecutionNotRunningException}, 못 돌리는 재시작은
 * {@code JobRestartException}. <b>같은 판정을 우리가 다시 쓰면 프레임워크가 조건을
 * 바꾸는 날 둘이 갈리고 우리 쪽이 틀린 답을 낸다.</b>
 *
 * <p><b>딱 하나가 우리에게 남았다 — {@code COMPLETED} 재시작.</b> 그 판정은 id 받는
 * {@code restart(long)} 이 {@code JobInstanceAlreadyCompleteException} 으로 냈는데,
 * 그 오버로드가 <b>제거 예정</b>이라 {@code JobExecution} 을 받는 쪽에는 없다.
 * <b>프레임워크가 안 하는 것만 우리가 한다</b>는 규칙은 그대로이고, 그 하나를 여기 적어 둔다.
 *
 * <h2>잡 이름을 안 받는다</h2>
 *
 * <p>실행 id 하나면 충분하다. 받으면 <b>이 통로가 특정 잡을 알게 되고</b>, 그러면 새 잡을
 * 만들 때마다 여기를 고쳐야 한다 — 범용 관제인 이유가 사라진다.
 *
 * <h2>⚠️ 앞단이 얇다</h2>
 *
 * <p>이 경로 앞에 서는 것은 <b>공유 비밀 관문 하나</b>이고(CY-742) 그것도 기본이 꺼져 있다.
 * 소지만 묻고 <b>누가</b> 불렀는지는 안 가른다. 그런데 이 모듈의 관리자 API 는 이미
 * 쓰기를 열어 두고 있다 — {@code ExpireAdminController}·{@code CleanupAdminController} 의
 * {@code recover}, {@code VerifyTriggerController} 의 손 트리거. <b>이 티켓이 그 자세를
 * 바꾸지는 않는다</b>: 같은 관문 뒤에 동작 둘을 더한다.
 *
 * <p>대신 <b>더 위험해지지 않게</b> 한다 — 판정을 프레임워크에 맡기고, 되돌릴 수 없는
 * 동작({@code abandon})은 <b>열지 않는다.</b> 그것은 실행을 죽은 것으로 표시해 회수 경로를
 * 바꾸므로, 사람이 화면에서 누를 것이 아니라 잡별 {@code recover} 가 근거를 갖고 할 일이다.
 */
@RestController
@RequestMapping("/api/v1/admin/batch")
public class BatchControlController {

    /**
     * <b>공용(동기) 빈이다.</b> 비동기 빈으로 재시작하면 응답이 "받았다" 까지만 말하고
     * 거절 예외가 이 스레드에 안 온다 — 그러면 아래 매핑이 통째로 죽는다.
     * {@code CleanupScheduler} 가 같은 이유로 같은 빈을 문다.
     */
    private final JobOperator jobOperator;

    /**
     * <b>실행을 id 로 찾는 자리.</b> {@code JobOperator} 의 id 받는 오버로드는 6.0 에서
     * 전부 <b>제거 예정</b>이라(실측: {@code restart(long)}·{@code stop(long)}·
     * {@code abandon(long)} 이 {@code @Deprecated(forRemoval)}) 남는 것은
     * {@code JobExecution} 을 받는 쪽이다. 그래서 여기서 먼저 실행을 꺼낸다.
     */
    private final JobRepository jobRepository;

    /**
     * <b>시체 판정을 여기서 새로 만들지 않는다.</b> 잡별 관제가 이미 같은 판정을 이 빈으로
     * 하고 있어(CY-429·CY-678·CY-697), 두 벌로 두면 <b>통로에 따라 죽었는지 여부가
     * 갈린다.</b>
     */
    private final RunningJobProbe runningJobProbe;

    public BatchControlController(
            @Qualifier(BatchJobRepositoryConfig.SHARED_OPERATOR) JobOperator jobOperator,
            JobRepository jobRepository,
            RunningJobProbe runningJobProbe) {
        this.jobOperator = jobOperator;
        this.jobRepository = jobRepository;
        this.runningJobProbe = runningJobProbe;
    }

    /**
     * <b>끝나지 않았거나 실패했거나 멈춘</b> 실행을 같은 파라미터로 다시 돌린다.
     *
     * <p><b>우리가 막는 것은 {@code COMPLETED} 하나다.</b> {@code STOPPED}(사람이 멈춘 것)와
     * {@code FAILED} 는 정상적인 재시작 대상이고, 한때 이 문장이 "실패한 실행" 만 말해서
     * {@code STOPPED} 가 대상인지 읽을 수 없었다.
     *
     * <p><b>{@code ABANDONED} 는 우리가 안 막지만 재시작되지도 않는다</b> — 그것은
     * "이 실행은 없던 것으로 친다" 는 표시라 프레임워크가 거절한다
     * ({@code JobRestartException} → {@code 409 BATCH-003}). 우리가 막는 목록에 넣지 않는
     * 이유는 <b>같은 판정을 두 벌로 두지 않기 위해서</b>이지 대상이라서가 아니다.
     *
     * <p><b>성공한 실행은 거절된다</b>({@code 409 BATCH-002}). 그것이 이 동작의 핵심
     * 안전장치다 — 성공한 실행을 다시 돌리면 같은 일이 두 번 처리된다. 같은 조건으로 또
     * 돌리고 싶으면 재시작이 아니라 <b>다른 파라미터로 새 인스턴스</b>다.
     *
     * <p><b>멱등이 아니다.</b> 두 번 누르면 두 번째는 첫 번째가 만든 실행 때문에 거절되거나
     * (도는 중) 또 하나를 만든다(첫 번째가 이미 실패로 끝났다면). 그래서 응답에 <b>새로
     * 만들어진 실행 id</b> 를 실어, 사람이 무엇이 생겼는지 보고 판단하게 한다.
     *
     * @param executionId 다시 돌릴 실행
     * @return 새로 만들어진 실행 id
     */
    @PostMapping("/runs/{executionId}/restart")
    public ResponseEnvelope<Restarted> restart(@PathVariable long executionId) {
        JobExecution execution = require(executionId);
        // **막는 것은 COMPLETED 하나다.** id 받는 restart 가 이 판정을 예외로 냈는데
        // 그 오버로드가 제거 예정이라, JobExecution 을 받는 쪽으로 옮기면서 판정 하나가
        // 우리에게 남았다 — 프레임워크가 안 하는 것만 우리가 한다.
        //
        // ⚠️ 첫 판은 `isUnsuccessful() || isRunning()` 이었고 **틀렸다.**
        //    BatchStatus 를 실측하니 STOPPED 는 둘 다 false 다 —
        //    사람이 방금 멈춘 잡을 다시 못 돌리게 막고, 그것도 "이미 성공했다"(BATCH-002)는
        //    거짓 이유로 거절했다. 리뷰가 잡았다.
        //
        //    running=false unsuccessful=false : COMPLETED · STOPPED
        //    running=true                     : STARTING · STARTED · STOPPING
        //    unsuccessful=true                : FAILED · ABANDONED · UNKNOWN
        //
        //    다시 시작하면 안 되는 것은 **COMPLETED 뿐**이다. 나머지는 프레임워크가
        //    판단한다(못 돌리면 JobRestartException 을 던진다).
        if (execution.getStatus() == BatchStatus.COMPLETED) {
            throw new BusinessException(BatchControlErrorCode.ALREADY_COMPLETED,
                    "jobExecutionId=" + executionId + " status=" + execution.getStatus());
        }
        try {
            return ResponseEnvelope.success(new Restarted(executionId,
                    jobOperator.restart(execution).getId()));
        } catch (JobRestartException refused) {
            throw new BusinessException(BatchControlErrorCode.RESTART_REFUSED,
                    "jobExecutionId=" + executionId
                            + " cause=" + refused.getClass().getSimpleName());
        }
    }

    /**
     * 도는 실행에 <b>멈추라고 신호를 보낸다.</b>
     *
     * <p><b>즉시 멈추지 않는다.</b> Spring Batch 의 중단은 협조적이라, 스텝이 다음 청크
     * 경계에서 그 신호를 확인해야 멈춘다 — 긴 tasklet 하나가 도는 중이면 그것이 끝날 때까지
     * 안 멈춘다. 그래서 응답이 <b>{@code stopping}</b> 이지 {@code stopped} 가 아니다.
     * 화면이 "눌렀는데 안 멈춘다" 를 정상으로 읽어야 한다.
     *
     * <p><b>⚠️ 죽은 실행에도 신호가 받아들여진다.</b> 배치 JVM 이 죽으면 그 행이
     * {@code STARTED} 로 영원히 남는데, {@code BatchStatus.STARTED.isRunning()} 이 참이라
     * {@code JobOperator} 는 <b>거절하지 않고 {@code true} 를 돌려준다</b>(실측).
     * 신호는 DB 에 적히지만 <b>그것을 읽을 프로세스가 없어 영영 안 멈춘다.</b>
     *
     * <p>그래서 응답에 {@code status} 를 함께 싣는다 — 오래된 {@code STARTED} 를 누른
     * 것이면 필요한 것은 중단이 아니라 <b>회수</b>({@code /runs/stuck} 목록과
     * 잡별 {@code recover})다. 이 통로가 그 판단까지 하지는 않는다: 정말 죽었는지를
     * 가르는 것은 <b>마지막 진도 시각</b>이고, 그 판정은 이미 잡별 관제가 갖고 있다.
     *
     * @param executionId 멈출 실행
     * @return 신호를 받아들였는지와 그 시점의 상태. <b>멈췄다는 뜻이 아니다</b>
     */
    @PostMapping("/runs/{executionId}/stop")
    public ResponseEnvelope<Stopping> stop(@PathVariable long executionId) {
        JobExecution execution = require(executionId);
        // **누르기 전 상태를 먼저 잡는다.** stop() 이 이 객체의 상태를 STOPPING 으로 바꿔
        // 놓으므로, 뒤에서 읽으면 "무엇을 눌렀는지" 가 아니라 "누른 결과" 가 나간다 —
        // 사람이 알고 싶은 것은 앞쪽이다(실측으로 STOPPED 가 나와서 잡혔다).
        String statusWhenSignalled = execution.getStatus().name();
        try {
            return ResponseEnvelope.success(new Stopping(executionId,
                    jobOperator.stop(execution), statusWhenSignalled));
        } catch (JobExecutionNotRunningException idle) {
            throw new BusinessException(BatchControlErrorCode.NOT_RUNNING,
                    "jobExecutionId=" + executionId);
        }
    }

    /**
     * <b>죽은 실행을 걷는다.</b> {@code stop} 이 신호만 남기고 못 하는 일이다.
     *
     * <h2>왜 범용 관제에 있어야 하나</h2>
     *
     * <p>{@code expireJob}·{@code verifyJob}·{@code cleanupJob} 은 각자 회수 API 를 갖고
     * 있다. 그런데 <b>이 관제가 범용인 이유</b>는 Spring Batch 메타데이터만 읽어 잡 이름을
     * 모른다는 것인데, 조치만 잡별 API 에 기대면 <b>주제가 바뀌는 순간 걷을 방법이
     * 사라진다.</b> 새 도메인의 잡에는 그 API 가 없다.
     *
     * <h2>두 단계다 — {@code abandon} 하나로는 안 된다</h2>
     *
     * <p>실측(Spring Batch 6.0.4): 죽은 {@code STARTED} 에 {@code abandon} 을 바로 부르면
     * <b>거부된다</b>({@code JobExecutionAlreadyRunningException: JobExecution is running or
     * complete and therefore cannot be aborted}) — {@code STARTED.isRunning()} 이 참이라
     * 프레임워크가 "도는 중" 으로 본다. {@code stop} 을 먼저 부르면 {@code STOPPED} 가 되고,
     * 그때 {@code abandon} 이 {@code ABANDONED} 로 내린다.
     *
     * <h2>⚠️ 시체 판정을 지난다</h2>
     *
     * <p>회수는 <b>되돌릴 수 없다.</b> 살아 있는 실행에 하면 그 잡이 통째로 죽는다.
     * 그래서 마지막 진도가 {@code batch.stuck-job-after-ms} 넘게 안 움직였을 때만 받고,
     * 아니면 <b>남은 시간을 실어 409</b>({@code BATCH-005})로 거절한다. 잡별 API 가 거는
     * 것과 <b>같은 판정</b>이다({@link RunningJobProbe}).
     *
     * <p><b>{@code batch.stuck-job-after-ms} 를 내리는 변경이 이 API 의 안전을 직접 깎는다</b>
     * — {@code ExpireAdminController} 가 같은 문장을 적어 뒀다.
     *
     * <h2>정리해도 같은 파라미터로 다시 돌지는 않는다</h2>
     *
     * <p>실측: 회수 뒤 같은 파라미터로 시작하면 {@code JobInstanceAlreadyCompleteException}
     * 이다. 운영에는 영향이 없다 — 세 스케줄러가 전부 {@code firedAt}·{@code asOf} 를 식별
     * 파라미터로 넘겨 회차마다 새 인스턴스다. <b>회수의 값은 "다시 돌리기" 가 아니라
     * "관제가 거짓말을 멈추는 것"</b> 이다({@code cy_batch_stuck_executions} 가 내려간다).
     *
     * @param executionId 걷을 실행
     * @return 누르기 전 상태와 최종 상태
     */
    @PostMapping("/runs/{executionId}/abandon")
    public ResponseEnvelope<Abandoned> abandon(@PathVariable long executionId) {
        JobExecution execution = require(executionId);
        if (!execution.getStatus().isRunning()) {
            // 회수는 시체용이다. 이미 끝난 실행에 하면 끝난 결과를 ABANDONED 로 덮는다.
            throw new BusinessException(BatchControlErrorCode.NOT_RUNNING,
                    "jobExecutionId=" + executionId + " status=" + execution.getStatus());
        }
        Duration remaining = runningJobProbe.untilStuck(execution);
        if (!remaining.isNegative() && !remaining.isZero()) {
            throw new BusinessException(BatchControlErrorCode.NOT_STUCK_YET,
                    "jobExecutionId=" + executionId + " remainingMs=" + remaining.toMillis());
        }
        // **누르기 전 상태를 먼저 잡는다.** 아래 두 호출이 이 객체의 상태를 바꿔 놓으므로,
        // 뒤에서 읽으면 "무엇을 눌렀는지" 가 아니라 "누른 결과" 가 나간다 — stop 과 같다.
        String statusWhenAbandoned = execution.getStatus().name();
        try {
            jobOperator.stop(execution);
        } catch (JobExecutionNotRunningException raced) {
            // 위에서 isRunning 을 봤는데 여기서 아니라면 그 사이 진짜로 끝난 것이다.
            throw new BusinessException(BatchControlErrorCode.NOT_RUNNING,
                    "jobExecutionId=" + executionId + " status=" + execution.getStatus());
        }
        try {
            JobExecution abandoned = jobOperator.abandon(execution);
            return ResponseEnvelope.success(new Abandoned(executionId, statusWhenAbandoned,
                    abandoned.getStatus().name()));
        } catch (JobExecutionAlreadyRunningException stillRunning) {
            // stop 이 STOPPING 에서 멈춘 경우다 — 읽을 프로세스가 있어 협조 중이라는 뜻이라
            // 죽은 실행이 아니다. 여기서 억지로 내리지 않는다.
            throw new BusinessException(BatchControlErrorCode.NOT_STUCK_YET,
                    "jobExecutionId=" + executionId + " status=" + execution.getStatus());
        }
    }

    /**
     * 실행을 꺼내거나 404 로 끊는다.
     *
     * <p><b>{@code null} 이 아니라 예외가 온다</b> — {@code JobRepository.getJobExecution} 은
     * 없는 id 에 {@code EmptyResultDataAccessException} 을 던진다(실측). 처음에는
     * {@code null} 검사를 적어 뒀는데 <b>틀렸고</b>, 그 상태에서는 없는 실행이 500 으로
     * 나갔다 — 화면이 "없는 실행" 과 "서버가 깨졌다" 를 구분하지 못한다.
     */
    private JobExecution require(long executionId) {
        try {
            return jobRepository.getJobExecution(executionId);
        } catch (EmptyResultDataAccessException absent) {
            throw new BusinessException(BatchControlErrorCode.EXECUTION_NOT_FOUND,
                    "jobExecutionId=" + executionId);
        }
    }

    /**
     * @param jobExecutionId 다시 돌린 원래 실행
     * @param newJobExecutionId 새로 만들어진 실행. <b>이것을 이력에서 보면 결과를 안다</b>
     */
    public record Restarted(long jobExecutionId, Long newJobExecutionId) {
    }

    /**
     * @param jobExecutionId 걷은 실행
     * @param statusWhenAbandoned 누르기 전 상태. <b>무엇을 걷었는지</b>가 여기 있다
     * @param status 걷은 뒤 상태. 정상이면 {@code ABANDONED}
     */
    public record Abandoned(long jobExecutionId, String statusWhenAbandoned, String status) {
    }

    /**
     * @param jobExecutionId 멈추라고 신호한 실행
     * @param signalled 신호가 받아들여졌는지. <b>멈췄다는 뜻이 아니다</b> — 중단은 협조적이라
     *        스텝이 다음 경계에서 확인해야 실제로 멈춘다
     * @param status <b>신호를 보내기 직전</b>의 상태 — 보낸 뒤가 아니다. 오래된 {@code STARTED} 면 그 실행은 이미 죽었을 수
     *        있고, 그때는 <b>신호를 읽을 프로세스가 없어 영영 안 멈춘다</b> — 필요한 것은
     *        중단이 아니라 회수다
     */
    public record Stopping(long jobExecutionId, boolean signalled, String status) {
    }
}
