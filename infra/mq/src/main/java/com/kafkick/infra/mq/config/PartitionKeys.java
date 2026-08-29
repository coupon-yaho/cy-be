package com.kafkick.infra.mq.config;

import com.kafkick.core.observation.IssuanceFlowEvent;

/**
 * 파티션 키를 한곳에 못박는다. 발행 지점마다 키를 고르면 분포가 조용히 갈린다.
 *
 * <pre>
 * persist  → issuanceCode   (issuances.code)
 * attempt  → memberId
 * notify   → memberId
 * </pre>
 *
 * <p><b>{@code couponId} 를 키로 쓰지 않는다.</b> 그건 캠페인 회차라 부하 테스트처럼 캠페인이
 * 하나뿐인 상황에서 모든 레코드가 한 파티션으로 몰린다. 파티션을 6으로 잡은 의미가 사라지고
 * 컨슈머를 몇 개 띄우든 하나만 일한다.
 *
 * <p>persist 만 {@code issuanceCode} 인 이유 — 이쪽은 발급 <b>한 건</b>의 영속화 순서가
 * 지켜져야 한다. 회원 단위로 묶으면 같은 회원의 서로 다른 발급이 같은 파티션에서 줄을 서고,
 * 순서 보장의 단위가 실제로 필요한 것보다 굵어진다.
 */
public final class PartitionKeys {

    private PartitionKeys() {}

    public static String forPersist(String issuanceCode) {
        if (issuanceCode == null || issuanceCode.isBlank()) {
            throw new IllegalArgumentException("issuanceCode 없이 persist 키를 만들 수 없다.");
        }
        return issuanceCode;
    }

    public static String forAttempt(long memberId) {
        return memberKey(memberId);
    }

    public static String forNotify(long memberId) {
        return memberKey(memberId);
    }

    /**
     * attempt 이벤트의 키. 발행 지점이 이벤트에서 값을 직접 꺼내다 {@code couponId} 로 잘못
     * 옮겨 적을 자리를 없앤다.
     */
    public static String forAttempt(IssuanceFlowEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event 없이 attempt 키를 만들 수 없다.");
        }
        return memberKey(event.memberId());
    }

    private static String memberKey(long memberId) {
        if (memberId <= 0) {
            throw new IllegalArgumentException("memberId 는 양수여야 한다: " + memberId);
        }
        return Long.toString(memberId);
    }
}
