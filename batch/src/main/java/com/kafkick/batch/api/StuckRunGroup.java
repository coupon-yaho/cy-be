// 한 잡의 시체 목록입니다. 전 잡 조회가 잡 이름별로 묶어서 냅니다.
package com.kafkick.batch.api;

import java.util.List;

/**
 * <b>{@link StuckRunView} 에 잡 이름을 얹지 않고 바깥에서 묶는다.</b>
 *
 * <p>그 record 는 <i>"잡 이름을 안 진다"</i> 고 명시하고 있고, 만료·정리·검증 셋이 같은
 * 모양을 쓰는 근거가 그것이다. 여기서 필드를 하나 더하면 <b>세 잡별 API 의 응답 JSON 이
 * 같이 바뀐다</b> — 운영자가 쓰던 조회가 이유 없이 달라진다.
 *
 * <p><b>행마다 잡 이름을 반복하지 않는다.</b> 잡 이름은 <i>"어느 잡이 멈췄나"</i> 를 읽는
 * 문맥이고, 운영자가 그다음에 부르는 경로는 <b>어느 것이든 실행 번호만 받는다</b> —
 * 잡별 {@code recover} 도 범용 {@code stop}·{@code abandon} 도 그렇다. 반복해서 실어 보낼
 * 값이 아니라 <b>그 다음 경로를 고르는 근거</b>라, 그룹에 한 번 있으면 된다.
 *
 * <p><b>그 선택을 이 record 가 대신하지 않는다.</b> 잡마다 처방이 다르고
 * ({@code expireJob}·{@code cleanupJob} 은 잡별 {@code recover}, 그 밖은 범용
 * {@code stop} → {@code abandon}), 그 근거는 {@link BatchHistoryController#stuck()} 와
 * {@link Recovered} 에 있다. 한때 이 자리에 <i>"다음 행동은 범용 abandon"</i> 이라고
 * 적었는데 <b>만료·정리에는 틀린 처방이었다</b> — {@code ABANDONED} 는 그 {@code asOf}
 * 인스턴스를 영원히 못 돌게 한다.
 */
public record StuckRunGroup(String jobName, List<StuckRunView> runs) {
}
