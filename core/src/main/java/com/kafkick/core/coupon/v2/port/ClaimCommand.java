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
 * @param idempotencyKey 클라이언트가 준 값. {@code '|'} 를 포함할 수 있다.
 *     <b>여기서 검증하지 않는다</b> — 빈 값은 호출부 버그가 아니라 잘못된 요청이라
 *     거부할 자리가 입력 검증(400)이다
 * @param requestToken 이 선점이 누구 것인가. 비어 있을 수 없고 {@code '|'} 를 포함할 수 없다 —
 *     <b>우리가 만든 값</b>이라 어기면 호출부 버그다
 * @throws IllegalArgumentException {@code gradeBit} 이 음수이거나, {@code requestToken} 이
 *     비었거나 {@code '|'} 를 포함할 때
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
        // 토큰은 우리가 만든 값이라 여기서 막는다. '|' 가 섞이면 필드 경계가 밀려 완료·보상의
        // 토큰 CAS 가 잘못 잘린 문자열을 비교한다 — 보상이 남의 선점을 되돌리는 초과 발급
        // 방향이다(12). 스크립트도 같은 것을 -10 으로 막지만, 그건 우리가 못 막았을 때의 그물이다.
        if (requestToken.isEmpty()) {
            throw new IllegalArgumentException("requestToken은 비어 있을 수 없습니다.");
        }
        if (requestToken.indexOf('|') >= 0) {
            throw new IllegalArgumentException("requestToken에는 '|'를 포함할 수 없습니다.");
        }
        // 멱등키는 여기서 막지 않는다. 클라이언트가 준 불투명 값이라 빈 값은 호출부 버그가
        // 아니라 잘못된 요청이고, 거부할 자리는 입력 검증(400)이다 — 값 객체에서 터뜨리면
        // 클라이언트 실수가 5xx 로 나간다. 그래도 새어 들어오면 스크립트가 -10 으로 막는다(01·12).
    }
}
