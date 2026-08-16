// 검출 결과 저장 계약입니다. 규칙 6종이 전부 이 계층으로 결과를 남깁니다.
package com.kafkick.core.verification;

import java.util.List;

/**
 * 읽기는 여기에 없습니다. 판정(양방향 MINUS·checksum)은 300만 행을 자바로 끌어올리지 않고
 * 집계 SQL 로 하므로 판정 쪽이 직접 질의합니다.
 */
public interface VerificationFindingRepository {

    /**
     * 검출 결과를 한 묶음으로 쌓는다.
     *
     * <p>같은 행을 다시 써도 되게 만든다. 청크가 죽은 지점부터 다시 도는데
     * {@code uk_run_finding(run_id, finding_type, target_key)} 가 걸려 있어
     * 그냥 INSERT 면 재시작이 중복키로 죽는다.
     *
     * <p>행 수를 돌려주지 않는다. 배치 재작성이 켜지면 드라이버가 행마다
     * {@code SUCCESS_NO_INFO} 를 준다. 검출 건수는 판정 단계가 집계 SQL 로 따로 센다.
     */
    void appendAll(long runId, List<VerificationFinding> findings);
}
