// 진도가 멈춘 정리 실행을 조회하고 걷어냅니다.
package com.kafkick.batch.api;

import java.util.List;

import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.batch.job.CleanupJobConfig;
import com.kafkick.core.support.response.ResponseEnvelope;

/**
 * <b>{@code cleanupJob} 의 시체를 손 SQL 없이 걷는다.</b> {@code BatchStuckExecution} 은
 * {@code Job} 빈 셋 모두에 뜨는데({@code BatchRunMetrics} 가 {@code List<Job>} 에서 이름을
 * 모은다) 복구 경로는 만료·검증 둘뿐이었다 — 알림이 <i>"cleanupJob 은 손 SQL 이 유일한
 * 길"</i> 이라고 명시하고 있었고, 그것이 이 컨트롤러가 생긴 이유다(CY-697).
 *
 * <p><b>{@link ExpireAdminController} 와 같은 모양이다.</b> 조회로 <i>"어느 실행이고 정말
 * 죽었나"</i> 에 답하고, 복구로 그것을 닫는다. 판단이 갈린 것은 <b>2단계가 아니라 한 방</b>
 * 이라는 것뿐이고 근거는 {@link CleanupRecoveryService} 에 적었다.
 *
 * <p><b>이 API 에는 사용자 인증이 없다.</b> 방어선은 둘인데(CY-742) 둘 다 <b>누가 불렀는지는
 * 안 가른다</b> — 1차는 업무 포트 미노출({@code batch.yml}), 2차는 공유 비밀 헤더
 * ({@code AdminTokenFilter}). 그래서 응답 문구는 카탈로그 것만 나가고 {@code detail} 은
 * 로그에만 남는다({@code BatchApiExceptionHandler}).
 */
@RestController
@RequestMapping("/api/v1/admin/cleanup")
public class CleanupAdminController {

    private final CleanupRecoveryService recovery;
    private final RunningJobProbe runningJobs;

    public CleanupAdminController(CleanupRecoveryService recovery, RunningJobProbe runningJobs) {
        this.recovery = recovery;
        this.runningJobs = runningJobs;
    }

    /**
     * <b>도는 실행은 여기 안 나온다</b> — 나오면 운영자가 그것을 걷어낸다.
     *
     * <p>읽기에도 데드라인을 준다(CY-697). 트랜잭션 밖이면 {@code DataSourceUtils} 가
     * {@code queryTimeout} 을 안 붙여 끊을 수단이 없다.
     */
    @Transactional(readOnly = true, timeoutString = "${batch.admin.timeout-seconds:5}")
    @GetMapping("/runs/stuck")
    public ResponseEnvelope<List<StuckRunView>> stuck() {
        return ResponseEnvelope.success(
                runningJobs.stuckExecutions(CleanupJobConfig.JOB_NAME).stream()
                        .map(StuckRunView::of)
                        .toList());
    }

    /**
     * <b>재시도해도 된다.</b> 이미 닫힌 실행이면 아무것도 안 쓰고
     * {@code alreadyRecovered=true} 로 200 을 낸다 — 복구 API 가 재시도에서 에러를 내면
     * 운영자는 첫 호출이 실패했다고 읽고 손 SQL 로 되돌아간다.
     */
    @PostMapping("/runs/{executionId}/recover")
    public ResponseEnvelope<Recovered> recover(@PathVariable long executionId) {
        CleanupRecoveryService.Outcome outcome = recovery.recover(executionId);
        return ResponseEnvelope.success(
                new Recovered(executionId, outcome.status(), outcome.alreadyRecovered()));
    }
}
