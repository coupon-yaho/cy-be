// 종료 표시를 못 남기고 죽은 만료 실행을 걷어내는 복구 API 입니다. 트리거는 없습니다.
package com.kafkick.batch.api;

import java.util.List;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.batch.config.ExpireStepContext;
import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.core.support.response.ResponseEnvelope;

/**
 * <b>{@code BatchStuckExecution} 이 우는데 처방이 손 SQL 뿐이었다.</b>
 *
 * <p>종료 표시를 못 남기고 죽은 실행은 {@code STATUS IN ('STARTING','STARTED','STOPPING')}
 * 조회에 {@code END_TIME} 검사도 시간 상한도 없어 <b>영원히</b> 남는다. {@code verifyJob} 은
 * {@code stop → abandon} 엔드포인트가 있는데 <b>{@code expireJob} 은 없어서</b>,
 * {@code docs/13} 이 적어 둔 {@code UPDATE BATCH_JOB_EXECUTION ...} 을 사람이 직접 쳐야 했다.
 *
 * <h2>안전은 판정 하나에서 온다 — {@code VERSION} 이 아니다</h2>
 *
 * <p><b>손 SQL 과 이 API 는 같은 성격의 쓰기를 한다.</b> 둘 다 {@code VERSION} 을 올린다
 * (API 는 선점 UPDATE 와 {@code recover} 의 갱신으로 JOB 행 기준 <b>+2</b>)
 * ({@code JdbcJobExecutionDao} 의 {@code UPDATE ... VERSION = VERSION + 1 WHERE ... AND
 * VERSION = ?}). 그러니 <i>"API 는 VERSION 을 안 올려서 안전하다"</i> 는 말은 거짓이다.
 *
 * <p>다른 것은 하나다 — <b>선행 조건이 코드에 있다.</b> 이 API 는
 * {@link RunningJobProbe#stuckExecutions} 가 시체로 판정한 실행에만 그 쓰기를 한다.
 * 손 SQL 은 그 임계를 사람이 SQL 에 직접 적어야 했고, <b>그 숫자가 코드와 갈리는 순간
 * 운영자가 살아 있는 만료를 걷어낸다</b> — 만료는 재고를 쓰는 유일한 잡이라 중간에 끊기면
 * 다음 검증의 판정 근거가 흔들린다.
 *
 * <p>그래서 <b>{@code batch.stuck-job-after-ms} 를 내리는 변경은 이 API 의 안전을 직접
 * 깎는다.</b> 그 값은 환경변수로 열려 있다.
 *
 * <p><b>절차 자체는 {@link ExpireRecoveryService} 가 진다.</b> 배치 메타에 대한 다단
 * 쓰기라 트랜잭션 경계가 필요한데, 컨트롤러에는 그것을 걸 자리가 없다.
 *
 * <h2>왜 {@code abandon} 이 아니라 {@code recover} 인가</h2>
 *
 * <p>{@code JobOperator#recover} 가 <b>정확히 이 용도로 있다</b>(Spring Batch 6.0.4).
 * 돌던 {@code StepExecution} 을 전부 {@code FAILED} + {@code END_TIME} 으로 닫고,
 * {@code JobExecution} 도 그렇게 닫고, 실행 문맥에 {@code batch.recovered} 를 남긴다(<b>인메모리 문맥에만</b> — 영속되지 않는다).
 * 처음에는 {@code verifyJob} 쪽 모양을 그대로 옮겨 {@code stop → abandon} 2단계로 지었는데,
 * 리뷰가 이 메서드를 짚었다. 셋이 낫다.
 *
 * <ol>
 *   <li><b>한 번에 끝난다.</b> 2단계는 중간에 끊기면 다음 호출이 서로 다른 이유로 409 를
 *       내고, 그 문구가 상황과 반대로 나가 운영자를 잘못 보낸다.</li>
     *   <li><b>실행 상태로 멱등하다.</b> {@code FAILED} + {@code END_TIME} 이면 다시 안
     *       쓴다 — 재시도가 안전해야 하고, 복구 API 에서 그것은 선택이 아니다.
     *       <b>{@code batch.recovered} 는 근거로 못 쓴다</b>({@link ExpireRecoveryService}
     *       가 이유를 적었다).</li>
 *   <li><b>{@code FAILED} 는 슬롯을 안 태운다.</b> {@code ABANDONED} 는
 *       {@code COMPLETED} 와 같은 취급이라({@code TaskExecutorJobLauncher} 가 둘을 같이
 *       막는다) 그 {@code JobInstance} 를 같은 파라미터로 영원히 못 돌린다. 만료는
 *       {@code asOf} 가 식별 파라미터다.</li>
 * </ol>
 *
 * <p>덤으로 {@code docs/13} 이 <i>"{@code BATCH_STEP_EXECUTION} 행은 {@code STARTED} 로
 * 남는다 — 그대로 둬도 된다"</i> 로 남겨 뒀던 찌꺼기까지 {@code recover} 가 닫는다.
 *
 * <h2>⚠️ 트리거는 열지 않는다</h2>
 *
 * <p>이 컨트롤러에 <b>만료를 띄우는 엔드포인트를 추가하면 안 된다.</b> CY-421 이
 * {@code ExpirePendingRefresher} 의 조회를 {@code asOf} 가 아니라 {@code END_TIME} 정렬로
 * 바꾼 <b>유일한 근거가 "만료 손 트리거가 이 저장소에 없다"</b> 이다. 트리거가 생기면 과거
 * {@code asOf} 실행이 나중에 끝날 수 있고, 그때 게이지가 <b>더 좁은 창의 더 작은 값</b>을
 * 내 관제가 그것을 <i>"밀린 것이 없다"</i> 로 읽는다.
 * {@code ExpireRecoveryTest.exposesExactlyTheRecoveryEndpoints} 가 이 컨트롤러의 매핑
 * 전체를 못 박으므로, 엔드포인트를 더하는 티켓은 그 목록을 고치며 이 결정을 함께 본다.
 *
 * <h2>노출</h2>
 *
 * <p>업무 포트(9090)에 열리고 <b>사용자 인증이 없다</b> — 누가 불렀는지는 못 가른다.
 * {@code batch.yml} 이 그 포트를 아예 안 내보내는 이유가 그것이고, 필요할 때만
 * {@code batch-expose.yml} 을 얹어 {@code 127.0.0.1} 에 묶는다({@code docs/13} §4).
 *
 * <p><b>그 오버레이가 토큰 관문을 함께 켠다</b>(CY-742) — 1차 방어선이 사라지는 자리가
 * 정확히 거기다. {@code AdminTokenFilter} 가 {@code /api/v1/admin/**} 전체를 덮으므로
 * 이 컨트롤러도 그 뒤에 선다. {@code VerifyTriggerController} 와 같은 축이다.
 */
@RestController
@RequestMapping("/api/v1/admin/expire")
public class ExpireAdminController {

    private final ExpireRecoveryService recovery;
    private final RunningJobProbe runningJobs;

    public ExpireAdminController(ExpireRecoveryService recovery, RunningJobProbe runningJobs) {
        this.recovery = recovery;
        this.runningJobs = runningJobs;
    }

    /**
     * <b>{@code BatchStuckExecution} 이 몇 건이라고만 말해 준다.</b> 그다음 질문인
     * <i>"어느 실행이고, 정말 죽었나"</i> 에 답하는 자리다.
     *
     * <p>도는 실행은 여기 안 나온다 — 나오면 운영자가 그것을 걷어낸다.
     */
    // **읽기에도 데드라인을 준다(CY-697).** 트랜잭션 밖이면 DataSourceUtils 가
    // queryTimeout 을 안 붙여 **끊을 수단이 없다** — 배치 메타가 잠긴 날 톰캣 스레드가
    // 그대로 붙잡힌다. 표준 스택은 관문이 꺼져 있고(batch.yml) 오버레이를 얹어도 공유 비밀
    // 하나라, 같은 번호로 요청이 몰리면 더 빨리 마른다 — 누구를 막을지 고를 수단이 없다.
    // 형제 BatchRunMetricsRefresher 가 같은 이유로 같은 값(5초)을 쓴다.
    @Transactional(readOnly = true, timeoutString = "${batch.admin.timeout-seconds:5}")
    @GetMapping("/runs/stuck")
    public ResponseEnvelope<List<StuckRunView>> stuck() {
        return ResponseEnvelope.success(
                runningJobs.stuckExecutions(ExpireStepContext.JOB_NAME).stream()
                        .map(StuckRunView::of)
                        .toList());
    }

    /**
     * <b>이것은 정상 절차가 아니라 복구다.</b> 걷어낸 실행이 남긴 것은 되돌리지 않는다 —
     * 만료는 청크마다 커밋하므로 <b>이미 커밋된 청크는 그대로 정당하고</b>(재고도 함께
     * 되돌아갔다) 안 끝난 청크는 프로세스가 죽을 때 롤백됐다. 남은 대상은 다음 04:10 이
     * 가져간다.
     *
     * <p><b>재시도해도 된다.</b> 이미 닫힌 실행이면 아무것도 안 쓰고
     * {@code alreadyRecovered=true} 로 200 을 낸다 — 복구 API 가 재시도에서 에러를 내면
     * 운영자는 첫 호출이 실패했다고 읽고 손 SQL 로 되돌아간다.
     */
    @PostMapping("/runs/{executionId}/recover")
    public ResponseEnvelope<Recovered> recover(@PathVariable long executionId) {
        ExpireRecoveryService.Outcome outcome = recovery.recover(executionId);
        return ResponseEnvelope.success(
                new Recovered(executionId, outcome.status(), outcome.alreadyRecovered()));
    }
}
