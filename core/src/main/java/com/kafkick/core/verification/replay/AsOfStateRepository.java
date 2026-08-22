// asof_state 쓰기 계약입니다. Step 0 의 산출물이고 규칙 6개 중 4개가 이걸 읽습니다.
package com.kafkick.core.verification.replay;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 읽기는 여기에 없습니다. V1·V2·V3·V5 는 300만 행을 자바로 끌어올리지 않고
 * 집계 SQL 로 대조하므로 규칙 쪽이 직접 질의합니다.
 *
 * <p><b>asOf 이하 이력이 하나도 없는 발급건은 행이 생기지 않습니다.</b> 그런 발급건은
 * 발급건 단위 규칙(V3·V5)에서 <b>통째로 빠집니다</b> — 이 테이블이 드라이빙이기 때문입니다.
 * 그래도 드라이빙을 바꾸지 않습니다. {@code issuances} 를 드라이빙으로 잡으면 접힌 상태가 없는 행이
 * {@code state IS NULL} 로 올라와, 기대 매트릭스에 없는 검출을 내기 때문입니다.
 * 그 몫은 <b>회차 단위 규칙이 재고 집계 차이로</b> 잡습니다.
 *
 * <p><b>계약 문면과 시드 구현이 다릅니다.</b> {@code docs/contract.json} 의 오염 유형 1 설명은
 * "재고는 줄었는데 history 에 ISSUE 기록 없음" 인데, 시드는 이력을 지우지 않고
 * 재고 카운터만 올려 같은 어긋남을 만듭니다(시드 저장소 {@code seedgen/corrupt.py} —
 * "재고만 +1. 고아 행이 없어 V1 하나만 울린다"). 그래서 실제 오염셋에는 이력이 없는 발급건이
 * 생기지 않습니다. <b>이 사실에 기대어 규칙을 짜지 마십시오</b> — 시드가 주입 방식을 바꾸면
 * 조용히 깨집니다. 위 문단의 이유(오탐 회피)만으로 드라이빙을 고정합니다.
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
     * <p>행 수를 돌려주지 않는다. 드라이버 기본값에서 UPDATE 는 <b>매칭된</b> 행을 세지
     * 바뀐 행을 세지 않는다. 재실행하면 값이 이미 채워져 있어 의미가 또 달라진다.
     * 판정 근거로 쓸 수 없는 숫자를 돌려주면 누군가 근거로 쓴다.
     */
    void applyActiveUsageCounts(long runId, LocalDateTime asOf);
}
