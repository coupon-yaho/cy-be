// 시체로 판정된 만료 실행 하나를 보여주는 응답 조각입니다.
package com.kafkick.batch.api;

import java.time.LocalDateTime;

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
 */
public record ExpireRunView(
        long executionId,
        String status,
        LocalDateTime createTime,
        LocalDateTime startTime,
        LocalDateTime lastProgress,
        long stalledSeconds) {

    public static ExpireRunView of(StuckRun run) {
        return new ExpireRunView(
                run.execution().getId(),
                run.execution().getStatus().name(),
                run.execution().getCreateTime(),
                run.execution().getStartTime(),
                run.lastProgress(),
                run.stalledSeconds());
    }
}
