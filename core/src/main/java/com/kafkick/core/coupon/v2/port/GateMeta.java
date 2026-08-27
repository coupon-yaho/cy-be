package com.kafkick.core.coupon.v2.port;

import java.util.Objects;

/**
 * 게이트 데이터 다섯 필드. <b>하나라도 빠지면 게이트가 아니다</b> — 선점과 복원이 같은 다섯을
 * 요구하므로(12 문서), 네 필드만 쓰면 발급은 도는데 만료 복원만 영구히 죽는 조합이 생긴다.
 * 그 상태는 복원의 "미준비" 로만 나타나 아무도 원인을 못 찾는다.
 *
 * <p>{@code totalQuantity} 를 선점은 쓰지 않는다. 그래도 이 타입이 다섯을 한 덩어리로 묶어
 * 부분 쓰기 자체를 불가능하게 한다.
 *
 * @param status 게이트 개폐
 * @param openAtEpochMillis 오픈 시각
 * @param closeAtEpochMillis 마감 시각
 * @param gradeMask 허용 등급 비트마스크. <b>음수를 금지한다</b> — Lua 의
 *     {@code bit.band(-1, x) == x} 라 전 등급이 통과하는 초과 발급 방향이다
 * @param totalQuantity 총재고. 복원의 상한이다
 */
public record GateMeta(
        GateStatus status,
        long openAtEpochMillis,
        long closeAtEpochMillis,
        int gradeMask,
        long totalQuantity
) {

    public GateMeta {
        Objects.requireNonNull(status, "status");
        if (openAtEpochMillis < 0 || closeAtEpochMillis < 0) {
            throw new IllegalArgumentException("게이트 시각은 음수일 수 없습니다.");
        }
        if (openAtEpochMillis > closeAtEpochMillis) {
            // 뒤집힌 창은 게이트 데이터로는 멀쩡해 보인다. 스크립트는 now >= closeAt 을 먼저 보므로
            // 모든 요청이 정상적인 -1(마감)로 나가고, 회차는 열린 적 없이 "마감된 회차" 로 보인다.
            throw new IllegalArgumentException("openAt은 closeAt보다 뒤일 수 없습니다.");
        }
        if (gradeMask < 0) {
            throw new IllegalArgumentException("gradeMask는 음수일 수 없습니다.");
        }
        if (totalQuantity < 0) {
            throw new IllegalArgumentException("totalQuantity는 음수일 수 없습니다.");
        }
    }
}
