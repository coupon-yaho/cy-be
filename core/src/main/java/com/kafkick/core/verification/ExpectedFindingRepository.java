// 오염셋의 정답 매니페스트를 읽어 검출과 대조합니다.
package com.kafkick.core.verification;

import java.util.List;

/**
 * <b>합격 조건은 건수가 아니라 집합 일치입니다.</b> 오탐 400 + 누락 400 도 800 이라,
 * {@code finding_count} 만 보면 정확히 검출한 것과 구분되지 않습니다.
 *
 * <p>양방향으로 봅니다 — 정답에 있는데 안 잡힌 것(누락)과 잡았는데 정답에 없는 것(오탐).
 * 한 방향만 보면 그 둘이 상쇄돼 통과합니다.
 *
 * <h2>두 목록은 정렬돼 옵니다 — 계약입니다</h2>
 *
 * <p>{@link #missing}·{@link #unexpected} 는 {@code (finding_type, target_key)} 오름차순으로
 * 옵니다. <b>바이트 비교</b>여야 합니다 — 대소문자를 접는 collation 이면 서로 다른
 * {@code target_key} 가 같은 순위가 되어 순서가 실행마다 갈립니다.
 *
 * <p><b>동률이 없습니다 — 그래서 전순서입니다.</b> 정렬 키 둘이 각 테이블에서 유일합니다:
 * {@code uk_expected(seed_run_id, finding_type, target_key)} 와
 * {@code uk_run_finding(run_id, finding_type, target_key)}. 한 실행·한 시드 안에서 같은
 * 쌍이 두 행일 수 없으므로, 이 정렬은 <b>같은 순위가 생기지 않습니다.</b> 동률이 있으면
 * MySQL 이 그 사이 순서를 보장하지 않아 계약이 성립하지 않습니다.
 *
 * <p><b>이것은 성능이 아니라 제출물의 계약입니다.</b> 리포트가 커밋돼 diff 되므로 같은
 * 판정을 두 번 떠서 순서만 달라지면 <i>"결과가 바뀌었다"</i> 로 읽힙니다. 목록이 잘려서
 * 실릴 때는 더 셉니다 — 정렬이 없으면 <b>매번 다른 표본</b>이 실립니다.
 *
 * <p><b>기대 행수를 상수로 박지 마십시오.</b> 계약의 800 은 기본 설정일 때의 값이고,
 * 시드의 {@code --plant-v6} 를 켜면 {@code corrupt_type=8} 로 한 건이 더 붙어 801 이 됩니다.
 * 판정은 <b>이 테이블을 읽어서</b> 해야 합니다.
 */
public interface ExpectedFindingRepository {

    /**
     * 정답에 있는데 이 실행이 못 잡은 것. <b>규칙이 놓친 것</b>이다.
     *
     * <p>조인은 {@code (finding_type, target_key)} 두 컬럼으로만 한다 —
     * 이유는 {@link FindingKey} 에 있다. <b>같은 두 컬럼 오름차순으로 정렬해 돌려준다</b> —
     * 위 "두 목록은 정렬돼 옵니다" 참고.
     */
    List<FindingKey> missing(long runId, long seedRunId);

    /**
     * 이 실행이 잡았는데 정답에 없는 것. <b>규칙이 잘못 잡은 것</b>이다.
     *
     * <p>정답 묶음이 아예 없으면(오염 주입을 안 돌린 DB) 검출 전부가 여기로 온다.
     * 그 경우를 판정 전에 걸러야 "오탐 800" 이라는 엉뚱한 결론이 안 나온다.
     *
     * <p><b>정렬해 돌려준다</b> — 위 "두 목록은 정렬돼 옵니다" 참고. 그리고
     * <b>개수 제한이 없다</b> — 부르는 쪽이 응답에 실을 때 잘라야 한다.
     */
    List<FindingKey> unexpected(long runId, long seedRunId);

    /** 이 정답 묶음이 존재하는가. 없으면 대조 자체가 성립하지 않는다. */
    boolean exists(long seedRunId);

    /**
     * 정답 묶음의 총 행수. 실패 메시지에 검출 총수와 함께 실어 <b>실패 모양을 가른다</b> —
     * {@code 정답 800 / 검출 0} 은 규칙이 안 돈 것이고, {@code 정답 800 / 검출 800} 인데
     * 누락·오탐이 400씩이면 {@code target_key} 포맷이 어긋난 것이다.
     */
    int countOf(long seedRunId);

    /**
     * 정답 묶음을 한 값으로 접는다. <b>판정 입력도 얼려야 한다</b> — 데이터 네 축(발급건·재고·
     * 정책·이력)은 {@code assertFrozenStep} 이 얼리는데 매니페스트는 안 얼려, 실행 중에 주입을
     * 다시 돌리면 <b>같은 데이터·같은 asOf 인데 판정만 달라진다.</b>
     */
    String digestOf(long seedRunId);
}
