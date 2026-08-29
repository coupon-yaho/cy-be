// 이력 한 행이 낸 V4 위반입니다. verification_findings 의 expected/actual 로 그대로 들어갑니다.
package com.kafkick.core.verification.replay;

import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;

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
     * <p><b>기대값을 한 상태로 못 적는다.</b> {@code CANCEL_USE} 는 만료 여부에 따라
     * {@code ISSUED} 와 {@code EXPIRED} 둘 다로 갈 수 있어서다
     * ({@code CouponStateMachine.isLegal}). 그래서 expected 에는 <b>허용 여부를 묻는 삼중항의
     * 앞 두 자리</b>만 적고 결과 자리를 {@code ?} 로 남긴다 — <i>"이 상태에서 이 사건으로는
     * 그 결과에 갈 수 없다"</i> 가 이 발견의 내용이다.
     *
     * <p>예전에는 전이표가 {@code (from, event) → to} 1:1 이라 기대값을 한 값으로 적었다.
     * 그 표는 {@code CANCEL_USE → ISSUED} 만 알아서, 만료된 쿠폰의 사용 취소를
     * <b>정상인데 위반으로</b> 셌다.
     */
    public static IllegalTransition notInTable(
            long historyId,
            IssuanceStatus tracked,
            IssuanceEventType event,
            IssuanceStatus claimed
    ) {
        return new IllegalTransition(
                historyId,
                Reason.NOT_IN_TABLE,
                expectedOf(tracked, event),
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

    /**
     * <b>결과가 둘인 사건만 갈래를 적는다.</b> {@code CANCEL_USE} 는 만료 여부에 따라
     * {@code ISSUED}·{@code EXPIRED} 로 갈리지만 나머지 넷은 여전히 결과가 하나다 —
     * 전부 {@code ?} 로 뭉개면 오염 유형 4 가 내는 200행의 진단이 통째로 흐려진다.
     *
     * <p>판정에는 안 들어간다({@code checksum} 은 {@code (finding_type, target_key)} 만
     * 쓴다). 사람이 {@code verification_findings} 를 열어 원인을 가릴 때만 쓰인다.
     */
    private static String expectedOf(IssuanceStatus from, IssuanceEventType event) {
        return event == IssuanceEventType.CANCEL_USE
                ? name(from) + "-CANCEL_USE->ISSUED|EXPIRED"
                : name(from) + "-" + event.name() + "->?";
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
