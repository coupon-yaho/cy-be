// 시체로 판정된 실행 하나를 보여주는 응답 조각입니다.
package com.kafkick.batch.api;

import java.time.LocalDateTime;

import com.kafkick.batch.config.BatchTimeAxis;
import com.kafkick.batch.config.RunningJobProbe.StuckRun;

/**
 * <b>운영자가 "정말 죽었나" 를 판단할 재료를 함께 낸다.</b>
 *
 * <p>실행 번호만 주면 그다음 질문이 <i>"이거 살아 있는 거 아냐?"</i> 인데, 그것을 배치 메타를
 * 직접 조회해서 답하게 하면 {@code docs/13} 의 손 SQL 로 되돌아간다.
 *
 * <p><b>값을 여기서 계산하지 않는다.</b> {@code lastProgress} 와 {@code stalledSeconds} 는
 * {@link com.kafkick.batch.config.RunningJobProbe} 가 <b>판정에 실제로 쓴 값</b> 그대로다 —
 * 한때 이 record 가 폴백을 다시 구현했는데 {@code startTime} 단계를 빠뜨려, Step 이 하나도
 * 없는 실행에서 <b>판정과 표시가 다른 컬럼을 가리켰다.</b>
 *
 * <p><b>잡 이름을 안 진다.</b> {@link StuckRun} 투영일 뿐이라 만료·정리가 같은 모양을 쓴다 —
 * 한때 {@code ExpireRunView} 였는데 정리 쪽 복구 경로를 열면서(CY-697) 이름만 잡에 묶여
 * 있었던 것이 드러났다. 응답 JSON 은 안 바뀐다.
 */
public record StuckRunView(
        long executionId,
        String status,
        LocalDateTime createTime,
        LocalDateTime startTime,
        LocalDateTime lastProgress,
        long stalledSeconds) {

    /**
     * <b>시각 셋을 도메인 축(UTC)으로 옮겨 내보낸다</b>(CY-743). 셋 다 배치 메타에서 와서
     * <b>JVM 기본 존</b> 벽시계인데({@code lastProgress} 도 {@code RunningJobProbe} 가
     * {@code LAST_UPDATED}·{@code START_TIME} 에서 만든다), 옮기지 않으면 같은 배치 API 안에서
     * {@code /verify/runs/{id}} 와 <b>좌표계가 갈린다</b> — 운영자가 두 조회를 나란히 열면
     * 시각이 존 오프셋만큼 어긋난 채 보인다.
     *
     * <p><b>{@code stalledSeconds} 는 안 건드린다.</b> 그것은 두 시각의 <b>차이</b>라 축과
     * 무관하다 — 옮기면 오히려 같은 값을 두 번 계산하는 셈이 된다.
     *
     * <p><b>{@code null} 을 각각 본다.</b> {@code START_TIME} 은 잡이 실제로 시작하기 전까지
     * 비어 있고({@code AbstractJob} 이 실행기 스레드에서 찍는다), {@code STOPPING} 이면 아예
     * 안 찍힌다. 시체 목록은 <b>바로 그런 행</b>을 보여 주는 API 라 여기서 던지면 목록 전체가
     * 500 이 된다.
     */
    public static StuckRunView of(StuckRun run) {
        return new StuckRunView(
                run.execution().getId(),
                run.execution().getStatus().name(),
                onDomainAxis(run.execution().getCreateTime()),
                onDomainAxis(run.execution().getStartTime()),
                onDomainAxis(run.lastProgress()),
                run.stalledSeconds());
    }

    private static LocalDateTime onDomainAxis(LocalDateTime batchMetaTime) {
        return batchMetaTime == null ? null : BatchTimeAxis.onDomainAxis(batchMetaTime);
    }
}
