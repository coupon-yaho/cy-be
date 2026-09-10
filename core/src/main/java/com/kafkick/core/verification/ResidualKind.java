// 두 실행 사이에서 한 대상이 어느 쪽에 있었는지입니다.
package com.kafkick.core.verification;

/**
 * <b>{@link ResidualCount} 의 세 칸을 대상 하나 단위로 뒤집은 것이다.</b>
 *
 * <p>집계는 <i>"이 유형이 몇 건 남았나"</i> 를 답하고, 이 값은 <i>"이 대상은 어느
 * 쪽인가"</i> 를 답한다. 같은 사실을 다른 그레인으로 보는 것이라 둘 중 하나만 있으면
 * 화면이 <b>"3건 남았다" 까지만 알고 어느 것인지 모른다.</b>
 *
 * <p><b>세 값이 닫혀 있다.</b> 두 실행 중 한쪽에만 있거나 양쪽에 있거나 셋뿐이고,
 * 어느 쪽에도 없으면 애초에 행이 없다.
 */
public enum ResidualKind {

    /** 앞뒤 실행 <b>양쪽</b>에 있다 — 아직 안 고쳐졌다. */
    PERSISTED,

    /** <b>뒤</b> 실행에만 있다 — 이번에 새로 생겼다. */
    INTRODUCED,

    /** <b>앞</b> 실행에만 있다 — 이번에 사라졌다. */
    RESOLVED;

    /**
     * 몇 쪽에 있었는지와 뒤 실행에 있었는지로 가른다.
     *
     * <p>질의가 세는 두 값({@code sides}, {@code hasAfter})을 그대로 받는다 —
     * 자바에서 다시 계산하면 SQL 과 두 벌이 되고, 갈리면 <b>화면과 집계가 다른 말을
     * 한다.</b>
     *
     * @throws IllegalArgumentException {@code sides} 가 1도 2도 아닐 때.
     *         {@code (finding_type, target_key)} 가 실행마다 유일하므로
     *         ({@code uk_run_finding}) 두 실행에서 최대 2다 — 3이 나오면 그 제약이
     *         깨진 것이고, 조용히 접으면 그 사실이 안 드러난다
     */
    public static ResidualKind of(int sides, boolean hasAfter) {
        if (sides == 2) {
            return PERSISTED;
        }
        if (sides != 1) {
            throw new IllegalArgumentException(
                    "한 대상은 두 실행에서 최대 두 번입니다(uk_run_finding). 받은 값=" + sides);
        }
        return hasAfter ? INTRODUCED : RESOLVED;
    }
}
