// 검증 규칙 6종입니다. verification_findings 와 expected_findings 가 같은 어휘를 공유해야 조인이 성립합니다.
package com.kafkick.core.verification;

/**
 * 규칙은 6종뿐입니다. V7 같은 규칙을 새로 만들지 않습니다.
 *
 * <ul>
 *   <li>발급코드 중복은 {@link #DUP_PER_MEMBER} 의 두 번째 케이스입니다
 *       ({@code GROUP BY coupon_id, code}, {@code MIN(id)} 제외)</li>
 *   <li>고아 이력은 {@link #ILLEGAL_TRANSITION} 이 전이 연쇄로 잡습니다</li>
 * </ul>
 *
 * 별도 규칙을 만들면 같은 행이 두 규칙에 잡혀 target_key 집합 비교가 어긋납니다.
 *
 * <p>오염 유형과 1:1 이 아닙니다. 주입 700건이 정답 800행이 되는 이유는
 * 유형 3(CANCEL_USE 이중 기록)이 {@link #STOCK_MISMATCH} 와 {@link #ILLEGAL_TRANSITION} 을
 * 동시에 울리기 때문입니다.
 *
 * <pre>
 * 규칙별 기대 행수   V1 200 · V2 200 · V3 100 · V4 200 · V5 100 · V6 0   합계 800
 * </pre>
 */
public enum FindingType {

    /** 재고 카운터 ↔ asof_state 활성 집계. 초과 발급 판정의 핵심 */
    STOCK_MISMATCH(Grain.COUPON, false),

    /** 1인 1매 위반 + 발급코드 중복. 케이스가 둘이고 target_key 형식은 같습니다 */
    DUP_PER_MEMBER(Grain.COUPON_MEMBER, true),

    /** 리플레이 결과 ↔ issuances.status */
    REPLAY_MISMATCH(Grain.ISSUANCE, false),

    /** 연쇄 불일치 + 전이표 위반 + 고아 이력. 가장 비쌉니다 */
    ILLEGAL_TRANSITION(Grain.HISTORY, true),

    /** asof_state.active_usage_count ↔ state = USED 여부 */
    USAGE_MISMATCH(Grain.ISSUANCE, true),

    /** issued_grade 스냅샷 ↔ eligible_grades_mask. members 를 조인하지 않습니다 */
    GRADE_VIOLATION(Grain.ISSUANCE, false);

    private final Grain grain;
    private final boolean deterministic;

    FindingType(Grain grain, boolean deterministic) {
        this.grain = grain;
        this.deterministic = deterministic;
    }

    public Grain grain() {
        return grain;
    }

    /**
     * 현재 행을 읽지 않고 asOf 로 완전히 재구성되는가.
     *
     * <p>Step 순서를 이것이 결정합니다 — 결정론 규칙이 먼저 돌아야 폭주로 중단돼도
     * 결정론적 부분은 이미 확보됩니다.
     *
     * <pre>
     * Step 1 V4   Step 2 V2   Step 3 V5     ← 완전 결정론
     * Step 4 V3   Step 5 V1   Step 6 V6     ← 현재 행을 읽음
     * </pre>
     *
     * <p>V2 가 결정론인 이유 — 세는 대상이 "행의 존재"와 code 인데 둘 다 변하지 않습니다.
     * 상태가 바뀌어도 행은 남고 코드는 발급 후 불변입니다.
     */
    public boolean isDeterministic() {
        return deterministic;
    }

    /** 위반이 발생하는 단위. target_key 형식을 결정합니다 */
    public enum Grain {
        COUPON,
        COUPON_MEMBER,
        ISSUANCE,
        HISTORY
    }
}
