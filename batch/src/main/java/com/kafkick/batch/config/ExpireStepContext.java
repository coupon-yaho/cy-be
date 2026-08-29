// 만료 잡이 Step 문맥에 남기는 값의 포맷입니다. 잡과 되읽기가 함께 씁니다.
package com.kafkick.batch.config;

import java.util.Arrays;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;

import org.springframework.batch.core.job.JobExecution;

/**
 * <b>만료 잡과 지표 되읽기가 함께 아는 계약.</b> 잡 이름 하나와 Step 문맥 값 하나뿐이다.
 *
 * <h2>왜 {@code ExpireJobConfig} 에 두지 않나</h2>
 *
 * <p>되읽기({@code ExpirePendingRefresher})가 이 둘을 쓴다. 그것을 잡 설정에서 직접 가져오면
 * <b>{@code batch.config → batch.job} 화살표가 생기는데, 그 방향은 이 저장소에 없었다</b> —
 * 잡 설정 셋이 전부 {@code config} 를 쓰는 단방향이었다. 한 번 순환이 생기면 다음 티켓이
 * <i>"이미 그렇게 하고 있다"</i> 를 근거로 삼고, {@code batch.job} 을 떼어 내려는 날 못 뗀다.
 *
 * <p>그리고 {@code @Configuration} 클래스에 {@code public static} 유틸을 매달면 그 클래스가
 * <b>잡 정의와 문맥 포맷 라이브러리 둘</b>이 된다. 계약은 계약이 있을 자리에 둔다.
 *
 * <p>형제 되읽기가 같은 규율을 이미 지킨다 — {@code BatchRunMetricsRefresher} 는 잡 이름을
 * 잡 설정이 아니라 {@code BatchRunMetrics.watchedJobNames()} 에서 받는다.
 */
public final class ExpireStepContext {

    /** 스케줄러·되읽기·검증 가드가 모두 이 이름으로 배치 메타를 조회한다. */
    public static final String JOB_NAME = "expireJob";

    /**
     * <b>만료 크론. 스케줄러와 검증 트리거 API 가 함께 읽는다.</b>
     *
     * <p>API 쪽이 이것을 보는 이유는 <b>곧 뜰 만료</b>와 겹칠 접수를 거절하기 위해서다 —
     * {@code max-expire-skips} 가 0 이 된 뒤로(CY-470) 만료가 검증을 건너뛰지 않고 지나가고,
     * 그때 찍히는 {@code updated_at} 때문에 그 {@code asOf} 를 영구히 못 쓴다.
     *
     * <p>리터럴을 두 곳에 적으면 한쪽만 고치는 실수를 아무것도 안 막는다 — 그러면 API 가
     * <b>실제로 안 뜨는 시각</b>을 근거로 거절하거나, 진짜 충돌을 통과시킨다.
     * {@code @Scheduled} 는 컴파일 상수만 받으므로 이 자리가 그것을 만족해야 한다.
     */
    public static final String CRON = "${batch.schedule.expire-cron:0 10 4 * * *}";

    /**
     * <b>이 실행에서 손대지 않기로 한 회차 목록.</b> 첫 청크가 한 번 구해 여기 싣고, 이후
     * 청크와 되읽기가 그것을 읽는다.
     *
     * <p><b>값에 {@code JobExecution} 세대를 함께 싣는다.</b> Step 문맥은 청크 커밋마다
     * 영속되고 <b>재시작이 그대로 복원한다.</b> 세대를 안 보면 재시작이 이전 실행의 목록을
     * 쓰게 되어, 그 사이 새로 어긋난 회차를 못 보고 <b>같은 자리에서 영원히 죽는다.</b>
     */
    public static final String BLOCKED_COUPONS_KEY = "expire.blockedCoupons";

    /**
     * <b>이 실행이 끝난 시점의 이력 id 상한.</b> 되읽기가 <i>"이 실행 이후의 변경"</i> 을 빼는
     * 창으로 쓴다.
     *
     * <p><b>시각이 아니라 id 다.</b> {@code created_at} 은 <b>멱등 선점 시각</b>이라
     * 백데이트되고, {@code committedAt} 은 <b>청크 시작</b> 시각이라 그 청크가 도는 동안
     * 붙은 이력을 <i>"실행 이후"</i> 로 잘못 뺀다 — 둘 다 봇 리뷰가 짚었다. id 는
     * {@code INSERT} 시점에 매겨지고 뒤로 안 간다. 검증도 같은 축을 쓴다
     * ({@code hasHistoriesAddedAbove(frozenMaxHistoryId, …)}).
     *
     * <p><b>잡이 끝나는 자리에서 한 번 찍는다</b> — 청크마다 찍으면 마지막 청크가 도는
     * 동안의 이력이 빠진다.
     *
     * <p><b>없으면 되읽기가 창을 못 건다.</b> {@code COUNT_PENDING} 이 창 없이 세면,
     * 실행이 끝난 <b>뒤</b> {@code CANCEL_USE}({@code USED → ISSUED})로 돌아온 행이 새로
     * 세어진다. 그 회차는 얼린 제외 목록에 없으니 {@code unexplained} 로 들어가고
     * {@code ExpireLeavesWorkBehind}(critical · channel server)가 뜬다 —
     * <b>배치는 안 틀렸는데 서버를 보라고 나가고, 만료가 일 1회라 최대 하루 간다.</b>
     *
     * <p>형제 {@link #BLOCKED_COUPONS_KEY} 와 같은 이유로 <b>세대를 함께 싣는다.</b>
     * Step 문맥은 재시작이 그대로 복원하므로, 세대를 안 보면 되읽기가 <b>이전 실행의 창</b>
     * 으로 지금 실행을 판정한다.
     */
    public static final String MAX_HISTORY_ID_KEY = "expire.maxHistoryId";

    /** 세대와 목록을 가르는 문자. 회차 id 에도 쉼표에도 안 나온다. */
    public static final String GENERATION_SEPARATOR = "|";

    private ExpireStepContext() {
    }

    /**
     * <b>이 실행이 건너뛴 회차 목록을 배치 메타에서 꺼낸다.</b>
     *
     * <p><b>재기동을 넘어 읽힌다.</b> 값은 {@code BATCH_STEP_EXECUTION_CONTEXT} 에 영속되고,
     * {@code JobRepository.getJobExecution(long)} 이 지난 실행을 불러올 때 Step 문맥을 함께
     * 채운다({@code fillStepExecutionDependencies}).
     */
    public static Optional<List<Long>> blockedFrom(JobExecution jobExecution) {
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
     * 관측({@link #blockedFrom})이 함께 읽는다. 두 벌로 두면 포맷을 바꾸는 날 한쪽만
     * 고쳐지고, 그 어긋남은 <b>지표만 조용히 틀리게</b> 만든다 — 잡은 멀쩡히 돈다.
     *
     * <p><b>두 가지 "빈 것" 을 가른다.</b> 여기 오는 것은 문맥 값 전체({@code raw})이지
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
    public static Optional<List<Long>> blockedFor(String raw, String prefix) {
        if (!raw.startsWith(prefix)) {
            return Optional.empty();
        }
        String ids = raw.substring(prefix.length());
        return Optional.of(ids.isEmpty() ? List.of()
                : Arrays.stream(ids.split(",")).map(Long::valueOf).toList());
    }

    /**
     * <b>이 실행이 끝난 시점의 이력 id 상한을 배치 메타에서 꺼낸다.</b> 형제
     * {@link #blockedFrom} 과 같은 규약이다 — 세대가 안 맞으면 빈 {@code Optional} 이다.
     *
     * <p>⚠️ <b>비어 있다고 게이지를 NaN 으로 두지 않는다.</b> 형제 쪽은 그렇게 하는데
     * 여기는 다르다 — 이 키는 <b>이 티켓이 새로 만든 것</b>이라, 배포 직후 마지막 성공
     * 실행에는 <b>반드시 없다.</b> NaN 으로 두면 다음 만료(하루 뒤)까지
     * {@code ExpireMetricsUnknown} 이 계속 울고, 그것은 <b>고치려던 오탐을 다른 오탐으로
     * 바꾸는 것</b>이다. 그래서 없으면 <b>창 없이</b> 센다 — 지금까지의 동작 그대로이고,
     * 새 실행이 한 번 돌면 바로 창이 걸린다.
     */
    public static Optional<Long> maxHistoryIdFrom(JobExecution jobExecution) {
        String prefix = jobExecution.getId() + GENERATION_SEPARATOR;
        return jobExecution.getStepExecutions().stream()
                .map(step -> maxHistoryIdFor(
                        step.getExecutionContext().getString(MAX_HISTORY_ID_KEY, ""), prefix))
                .flatMap(Optional::stream)
                .findFirst();
    }

    /** 세대 접두사를 떼고 id 를 읽는다. 형식이 깨졌으면 <b>모른다</b> 로 둔다. */
    static Optional<Long> maxHistoryIdFor(String raw, String prefix) {
        if (!raw.startsWith(prefix)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Long.parseLong(raw.substring(prefix.length())));
        } catch (NumberFormatException exception) {
            return Optional.empty();
        }
    }
}
