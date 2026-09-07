package com.kafkick.batch.api;

import java.util.List;

import org.springframework.batch.core.job.Job;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.core.batch.BatchRunRepository;
import com.kafkick.core.support.response.ResponseEnvelope;

/**
 * 배치 실행 이력. 세 잡(expire·verify·cleanup)을 함께 준다.
 *
 * <p>검증 이력과 가른 이유는 출처가 달라서다. 이쪽은 Spring Batch 메타의 실행 기록이고
 * 저쪽은 판정 기록이다 — verifyJob 은 두 곳에 다 있지만, 가드에 걸려 죽으면 실행만 남고
 * 검증 행은 안 생긴다.
 *
 * <p>회차 상태 전이는 여기 안 나온다. {@code CouponRoundScheduler} 는 Spring Batch 잡이
 * 아니라 {@code @Scheduled} 라 메타에 행이 안 남는다 — 그 축은 지표로 본다.
 */
@RestController
@RequestMapping("/api/v1/admin/batch")
public class BatchHistoryController {

    private final BatchRunRepository runs;
    private final RunningJobProbe runningJobs;
    private final List<String> jobNames;

    /**
     * <b>볼 잡을 {@code Job} 빈에서 받는다</b> — {@code BatchRunMetrics} 와 같은 근거다.
     * 이름을 리터럴로 적으면 CY-384 가 {@code JOB_NAME} 으로 없앤 이중화가 되살아나고,
     * 잡이 넷째로 늘어나는 날 이 조회만 조용히 셋을 본다.
     *
     * <p><b>{@code JobRepository.getJobNames()} 를 안 쓴다.</b> 6.0.4 바이트코드로 쟀더니
     * 그 인터페이스 <b>기본 구현이 {@code Collections.emptyList()} 를 돌려준다</b> —
     * 예외가 아니라 빈 목록이다.
     * <pre>
     * public default java.util.List&lt;java.lang.String&gt; getJobNames();
     *   0: invokestatic  // Collections.emptyList()
     *   3: areturn
     * </pre>
     * 지금 도는 것은 {@code SimpleJobRepository extends SimpleJobExplorer} 가 <b>상속으로</b>
     * 주는 구현이고({@code SimpleJobRepository} 자신은 이 메서드를 오버라이드하지 않는다),
     * 그 상속이 끊기는 날 이 API 는 <b>"시체 0건" 을 영원히 참으로</b> 낸다. 그것은 알림이
     * 우는데 목록이 비는 모양이라, 운영자가 손 SQL 로 되돌아가는 대신 <b>없다고 믿는다.</b>
     *
     * <p>대신 배치 메타에만 남은 <b>빈이 사라진 잡</b>의 실행은 이 목록에 안 나온다. 그 행은
     * 아무 잡도 안 막으므로({@code findRunningJobExecutions} 는 이름으로 조회한다) 걷어낼
     * 이유가 없다 — 다만 <b>영원히 안 지워진다.</b> {@code cleanupJob} 은 {@code END_TIME}
     * 이 {@code NULL} 인 행을 <b>일부러 안 건드리고</b>(그 근거는 {@code CleanupJobConfig}
     * 에 있다 — 시체를 지우는 것은 고치는 것이 아니라 증거를 지우는 것이다), 시체는 정의상
     * 그 행이다. 지표도 같은 {@code Job} 빈에서 라벨을 만들어 그 잡의 계열이 아예 없으므로,
     * <b>알림이 우는데 목록이 비는 모양은 안 생긴다</b> — 둘이 같은 것을 못 본다. 그 행을
     * 걷으려면 손 SQL 이다({@code docs/13}). <b>한때 여기 "정리 잡이 보존 기간이 지나면
     * 지운다" 고 적었는데 거짓이었다.</b>
     *
     * <p>이름 순으로 정렬한다 — 빈 순서는 보장이 없다.
     */
    public BatchHistoryController(BatchRunRepository runs, RunningJobProbe runningJobs,
            List<Job> jobs) {
        this.runs = runs;
        this.runningJobs = runningJobs;
        this.jobNames = jobs.stream().map(Job::getName).sorted().toList();
    }

    /**
     * 최근 실행부터 한 페이지. {@code jobName} 을 안 주면 세 잡을 다 준다.
     *
     * <p><b>{@code anchor} 로 페이지 경계를 얼린다.</b> 첫 요청은 안 보내고, 응답이 준 값을
     * 다음 요청부터 되돌려주면 그 사이에 새 실행이 생겨도 목록이 안 밀린다. 근거는
     * {@link HistoryPage} 에 있다.
     *
     * <p>⚠️ <b>한때 여기 "전체가 약 90행이라 한 요청에 다 들어온다" 고 적고 커서를 안 만들었다 —
     * 틀린 단정이었다(봇 리뷰가 두 번 짚었다).</b> 세 잡이 일 1회인 것은 <b>기본 크론</b>일 뿐이고
     * {@code EXPIRE_CRON}·{@code VERIFY_CRON}·{@code CLEANUP_METADATA_KEEP_DAYS} 가 전부
     * 환경변수다. 게다가 검증은 {@code POST /api/v1/admin/verify} 로 <b>손 트리거가 열려 있어</b>
     * 하루에도 여러 건이 쌓인다. 상한이 보장되지 않는데 보장된다고 적었다.
     */
    @GetMapping("/runs")
    @Transactional(readOnly = true, timeoutString = "${batch.admin.timeout-seconds:5}")
    public ResponseEnvelope<HistoryPage<BatchRunView>> history(
            @RequestParam(required = false) String jobName,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset,
            @RequestParam(required = false) Long anchor) {
        int size = HistoryPage.pageSize(limit);
        int from = HistoryPage.pageOffset(offset);
        List<BatchRunView> items = runs.findRecent(jobName, size, from, anchor).stream()
                .map(BatchRunView::of)
                .toList();
        Long boundary = anchor != null ? anchor : runs.latestExecutionId(jobName);
        return ResponseEnvelope.success(
                new HistoryPage<>(items, runs.countRecent(jobName, boundary), size, from,
                        boundary));
    }

    /**
     * <b>전 잡의 시체 목록.</b> 범용 관제에 조치({@code stop}·{@code abandon}·{@code restart})는
     * CY-927 까지 다 올라왔는데 <b>그 {@code executionId} 를 알아낼 조회가 이쪽에 없었다</b> —
     * 목록은 {@code /admin/expire}·{@code /admin/cleanup} 처럼 잡별 경로에만 있어서,
     * 새 잡을 붙이면 조치는 되고 대상은 못 찾는 상태였다. {@link BatchControlController#stop}
     * 의 주석이 이미 {@code /runs/stuck} 을 가리키고 있었는데, <b>이 경로에는 그것이
     * 없었다.</b>
     *
     * <h2>⚠️ 여기서 찾은 번호를 바로 {@code abandon} 에 넣지 말 것</h2>
     *
     * <p><b>만료·정리의 처방은 {@code abandon} 이 아니라 잡별 {@code recover} 다.</b>
     * {@code ABANDONED} 는 {@code COMPLETED} 와 같은 취급이라
     * ({@code TaskExecutorJobLauncher} 가 둘을 같이 막는다) <b>그 {@code JobInstance} 를 같은
     * 파라미터로 영원히 못 돌린다</b> — 만료는 {@code asOf} 가 식별 파라미터라 그 크론
     * 슬롯이 통째로 사라진다. 그래서 잡별 API 는 일부러 {@code FAILED} 로 닫는다
     * ({@link Recovered}). 범용 {@code abandon} 은 <b>잡별 회수가 없는 잡</b>을 위한
     * 마지막 통로다.
     *
     * <p>그리고 이 목록의 행은 대개 {@code STARTED} 인데
     * {@code BatchRunAbandonService} 가 받는 것은 {@code STOPPING}·{@code STOPPED} 뿐이라,
     * 바로 넣으면 409 다. 범용 경로를 쓸 때도 <b>{@code stop} 이 먼저</b>다.
     *
     * <p><b>판정을 여기서 다시 하지 않는다.</b> {@link RunningJobProbe#stuckExecutions} 를
     * 잡마다 부른다 — 임계와 규칙이 그 클래스에 한 벌만 있어야 하고, 그 이유는 거기
     * 적혀 있다. 여기가 하는 일은 <b>잡 이름을 대는 것</b>뿐이다.
     *
     * <p><b>비어 있는 잡도 낸다.</b> 시체가 없는 잡을 목록에서 빼면 "그 잡을 봤는데 없었다"
     * 와 "그 잡을 아예 안 봤다" 가 응답에서 같은 모양이 된다 — 이 API 가 답해야 하는 질문이
     * 정확히 그 둘의 차이다.
     *
     * <p><b>질의는 잡 수만큼이 아니다 — 시체 수와 그 Step 수로 는다.</b> 6.0.4 바이트코드로
     * 쟀다: {@code SimpleJobExplorer.findRunningJobExecutions} 는 DAO 조회 한 번 뒤에
     * <b>실행마다</b> {@code getJobInstance}·{@code getStepExecutions}·
     * {@code getExecutionContext} 셋을, <b>Step 마다</b> {@code getExecutionContext} 하나를
     * 더 한다. 잡 하나가 {@code 1 + 3R + S} 다(R=그 잡의 실행 중인 행, S=그 Step 수).
     *
     * <p>그래서 커서를 안 둔 근거는 "잡 수만큼이라 작다" 가 아니라 <b>목록의 길이가
     * 곧 사고의 크기</b>라는 것이다 — 시체가 스무 건 쌓였으면 그 자체가 봐야 할 상황이고,
     * 페이지를 나눠 뒤쪽을 안 보이게 하는 것이 더 나쁘다. 다만 그 구간에서 <b>예산을 먼저
     * 넘기는 것도 이 조회</b>이므로, 실제로 끊기는 것을 보는 날 아래 데드라인부터 다시 잰다.
     * {@code StuckRunView} 가 {@code ExecutionContext} 를 한 필드도 안 쓰는데 프레임워크가
     * 그것을 읽어 오는 것도 그때 함께 볼 자리다.
     *
     * <p><b>데드라인은 형제와 같은 키를 쓰는데 그 예산을 잡 수만큼 나눠 쓴다.</b>
     * {@code DataSourceUtils} 가 문장마다 <b>남은</b> 시간을 붙이므로, 배치 메타가 느린 날
     * <b>잡별 경로는 뜨는데 이 조회만 죽는</b> 구간이 생긴다 — 하필 "잡별 경로를 몰라도
     * 되게" 만든 통로가 먼저 죽는다. 별도 키를 두지 않은 것은 <b>그 값을 아직 안 쟀기
     * 때문이다</b>: 잡이 셋이고 질의가 인덱스 조회라 지금은 5초가 넉넉하고, 잡이 늘어
     * 여기가 먼저 끊기는 것을 실제로 보는 날 그 실측으로 키를 가른다.
     */
    @GetMapping("/runs/stuck")
    @Transactional(readOnly = true, timeoutString = "${batch.admin.timeout-seconds:5}")
    public ResponseEnvelope<List<StuckRunGroup>> stuck() {
        return ResponseEnvelope.success(jobNames.stream()
                .map(jobName -> new StuckRunGroup(jobName,
                        runningJobs.stuckExecutions(jobName).stream()
                                .map(StuckRunView::of)
                                .toList()))
                .toList());
    }
}
