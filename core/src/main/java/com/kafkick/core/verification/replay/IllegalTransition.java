// 이력 한 행이 낸 V4 위반입니다. verification_findings 의 expected/actual 로 그대로 들어갑니다.
package com.kafkick.core.verification.replay;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;

/**
 * <b>이력 한 행은 위반을 하나만 냅니다.</b> target_key 가 {@code HISTORY:{이력id}} 인데
 * {@code uk_run_finding(run_id, finding_type, target_key)} 가 걸려 있어, 한 행이 둘을 내면
 * 두 번째 INSERT 가 중복키로 죽습니다. 집합 비교도 어긋납니다.
 *
 * <p>{@code expected}/{@code actual} 은 varchar(200) 컬럼에 들어갑니다.
 * checksum 은 {@code (finding_type, target_key)} 만 쓰므로 이 문자열은 판정에 영향이 없고,
 * 사람이 읽는 진단 정보입니다.
 */
public record IllegalTransition(long historyId, Reason reason, String expected, String actual) {

    private static final String NONE = "(없음)";

    public IllegalTransition {
        if (reason == null) {
            throw new IllegalArgumentException("위반 사유가 필요합니다.");
        }
        if (expected == null || actual == null) {
            throw new IllegalArgumentException("기대값과 실제값이 필요합니다.");
        }
    }

    /**
     * 추적 상태에서 그 사건이 일어날 수 없거나, 일어나도 결과 상태가 다르다.
     *
     * @param allowed 전이표가 허용하는 결과 상태. 전이 자체가 없으면 null
     */
    public static IllegalTransition notInTable(
            long historyId,
            IssuanceStatus tracked,
            IssuanceEventType event,
            IssuanceStatus allowed,
            IssuanceStatus claimed
    ) {
        return new IllegalTransition(
                historyId,
                Reason.NOT_IN_TABLE,
                render(tracked, event, allowed),
                render(tracked, event, claimed)
        );
    }

    /**
     * 전이 자체는 합법인데 행이 주장하는 출발 상태가 추적 상태와 다르다.
     *
     * <p>이력만 읽으면 합법으로 보이는 위조를 여기서 잡습니다.
     */
    public static IllegalTransition chainBroken(
            long historyId,
            IssuanceStatus tracked,
            IssuanceStatus claimed
    ) {
        return new IllegalTransition(
                historyId,
                Reason.CHAIN_BROKEN,
                "from=" + name(tracked),
                "from=" + name(claimed)
        );
    }

    private static String render(IssuanceStatus from, IssuanceEventType event, IssuanceStatus to) {
        return name(from) + "-" + event.name() + "->" + name(to);
    }

    private static String name(Enum<?> value) {
        return value == null ? NONE : value.name();
    }

    public enum Reason {

        /** 전이표에 없는 전이 */
        NOT_IN_TABLE,

        /** from_status 가 앞 행의 to_status 와 어긋남 */
        CHAIN_BROKEN
    }
}
