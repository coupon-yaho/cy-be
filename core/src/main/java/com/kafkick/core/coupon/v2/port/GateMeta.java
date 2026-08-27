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
 * @throws IllegalArgumentException 게이트 데이터로 성립할 수 없는 값일 때. 셋 다
 *     <b>스크립트가 정상 코드로 거절</b>하는 값이라 여기서 안 막으면 잘못된 설정이 사고가
 *     아니라 "정상 마감"·"정상 미달" 로 보인다 — 경보가 뜨지 않는 실패다.
 *     <ul>
 *       <li>시각이 음수 — epochMillis 로 성립하지 않는다</li>
 *       <li>{@code openAt > closeAt} — 창이 뒤집히면 모든 요청이 {@code -1}(마감)로 나가
 *           회차가 열린 적 없이 "마감된 회차" 로 보인다</li>
 *       <li>{@code gradeMask} 가 음수 — Lua 의 {@code bit.band(-1, x) == x} 라 전 등급이
 *           통과한다. <b>초과 발급 방향</b>이다</li>
 *       <li>{@code totalQuantity} 가 음수 — 복원의 상한이 음수면 만료 복원이 전부
 *           {@code -2}(상한 초과)로 막힌다</li>
 *       <li>{@code totalQuantity} 가 {@link #TOTAL_QUANTITY_MAX} 초과 — 스크립트가 그 값을
 *           읽지 못해 선점이 영구히 {@code -9}, 복원이 영구히 {@code -1} 이다</li>
 *     </ul>
 */
public record GateMeta(
        GateStatus status,
        long openAtEpochMillis,
        long closeAtEpochMillis,
        int gradeMask,
        long totalQuantity
) {

    /**
     * 스크립트의 {@code isCanonicalInt} 가 받는 자릿수와 <b>같은 상한</b>이다(15자리).
     *
     * <p>Lua 5.1 의 수는 double 이라 2^53 위에서는 {@code left + n > total} 이 조용히 틀린다 —
     * 그래서 12 문서가 판정 집합을 일부러 15자리로 좁혀 뒀다. 여기서 넓게 받으면
     * <b>쓸 수는 있는데 아무도 못 읽는 meta</b> 가 만들어지고, 그 회차는 선점이 영구히
     * {@code -9}, 복원이 영구히 {@code -1} 이다. 두 축의 판정이 갈리면 안 된다.
     */
    public static final long TOTAL_QUANTITY_MAX = 999_999_999_999_999L;

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
        if (totalQuantity < 0 || totalQuantity > TOTAL_QUANTITY_MAX) {
            throw new IllegalArgumentException(
                    "totalQuantity는 0 이상 " + TOTAL_QUANTITY_MAX + " 이하여야 합니다.");
        }
    }
}
