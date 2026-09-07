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
 * <p>행마다 잡 이름을 반복하지 않는 이유도 있다. 운영자가 이 목록에서 하는 다음 행동은
 * {@code POST /api/v1/admin/batch/runs/{id}/abandon} 인데 <b>그 경로에 잡 이름이 안 들어간다</b>
 * — 이름은 "어느 잡이 멈췄나" 를 읽는 문맥이지 요청에 실어 보낼 값이 아니다.
 */
public record StuckRunGroup(String jobName, List<StuckRunView> runs) {
}
