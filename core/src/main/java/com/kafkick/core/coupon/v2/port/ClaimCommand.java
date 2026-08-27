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
 *     <b>여기서 검증하지 않는다</b> — 빈 값은 호출부 버그가 아니라 잘못된 요청이다.
 *     지금 그것을 막는 것은 스크립트의 {@code -10} 하나이고, 요청 검증(400)은 v2 발급
 *     엔드포인트가 생기는 S4·S5 의 몫이다(생성자의 TODO)
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
        // 아니라 잘못된 요청이고, 값 객체에서 터뜨리면 클라이언트 실수가 5xx 로 나간다.
        // '|' 와 개행이 든 키는 아예 정상 입력이라(01) 여기서 막으면 Java 만 파손으로 세어
        // 두 축이 갈린다.
        //
        // 지금 실제로 막는 것은 스크립트의 -10(BAD_ARGUMENT) 하나뿐이다. v2 에는 아직 발급
        // 엔드포인트가 없어 입력 검증 계층 자체가 없다.
        // TODO(CY-646 S4·S5): v2 발급 엔드포인트가 생기면 빈 멱등키를 요청 검증에서 400 으로
        //   막는다. -10 은 "호출부 버그" 라는 뜻이고 이상 카운터(badArgument)를 올리므로,
        //   클라이언트가 만든 값이 그 카운터를 올리면 정상 운영에서 0 이어야 할 지표가 흐려진다.
    }
}
