package com.kafkick.core.coupon.v2.port;

import java.util.Objects;

/**
 * 선점 인자. <b>선점 시각은 여기 없다</b> — 시각의 원본은 Redis 하나다(12 문서).
 * api 가 여러 대라 호출 인스턴스의 시계를 넘기면 마감 경계의 답이 인스턴스마다 갈리고,
 * 시계가 앞선 인스턴스가 남긴 선점은 나이가 음수라 {@code stalePendingCount} 가 못 잡는다.
 *
 * @param couponRoundId 회차. 키 네 개의 해시태그가 된다
 * @param memberId 발급 회원
 * @param gradeBit 이 회원의 등급 비트. {@code meta.gradeMask} 와 {@code AND} 된다
 * @param idempotencyKey 클라이언트가 준 값. {@code '|'} 를 포함할 수 있다
 * @param requestToken 이 선점이 누구 것인가. {@code '|'} 금지
 */
public record ClaimCommand(
        long couponRoundId,
        long memberId,
        int gradeBit,
        String idempotencyKey,
        String requestToken
) {

    public ClaimCommand {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        Objects.requireNonNull(requestToken, "requestToken");
        if (gradeBit < 0) {
            // 스크립트의 등급 비트 가드는 '^%d+$' 라 음수를 -10(호출부 버그)으로 돌려준다.
            // 왕복을 돌고 경보를 울리기 전에 여기서 막는다.
            throw new IllegalArgumentException("gradeBit은 음수일 수 없습니다.");
        }
    }
}
