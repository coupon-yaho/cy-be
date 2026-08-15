// asof_state 쓰기 계약입니다. Step 0 의 산출물이고 규칙 6개 중 4개가 이걸 읽습니다.
package com.kafkick.core.verification.replay;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 읽기는 여기에 없습니다. V1·V2·V3·V5 는 300만 행을 자바로 끌어올리지 않고
 * 집계 SQL 로 대조하므로 규칙 쪽이 직접 질의합니다.
 */
public interface AsOfStateRepository {

    /**
     * 접기 결과를 한 묶음으로 쌓는다. 사용 건수는 0 으로 두고 뒤에서 한 번에 채운다.
     *
     * <p>재시작해도 같은 행을 다시 써도 되게 만든다. 청크가 죽은 지점부터 다시 도는데
     * PK 가 {@code (run_id, coupon_id)} 라 그냥 INSERT 면 중복키로 죽는다.
     *
     * <p>반영 행 수를 돌려주지 않는다. 배치 재작성이 켜지면 드라이버가 행마다
     * {@code SUCCESS_NO_INFO} 를 돌려주어 셀 수 없다. 세는 척하면 그 숫자를 근거로 쓰게 된다.
     */
    void appendAll(long runId, List<ReplayResult> results);

    /**
     * asOf 기준 활성 사용 건수를 한 문장으로 채운다.
     *
     * <p>활성 = {@code used_at <= asOf AND (canceled_at IS NULL OR canceled_at > asOf)}.
     * 발급건마다 질의하면 300만 번이라 집계 조인 한 번으로 끝낸다.
     *
     * @return 값이 실제로 바뀐 행 수. 모든 행이 0 에서 시작하므로
     *         곧 <b>활성 사용이 하나 이상인 발급건 수</b>다
     */
    int applyActiveUsageCounts(long runId, LocalDateTime asOf);
}
