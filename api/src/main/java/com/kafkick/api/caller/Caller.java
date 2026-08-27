package com.kafkick.api.caller;

/**
 * 요청을 보낸 회원. <b>인증 결과가 아니라 사용자 구분 수단이다.</b>
 *
 * <p>서명이 없으므로 클라이언트가 무엇이든 주장할 수 있다. 이 값을 권한 판정에 쓰지 않는다.
 *
 * @param memberId {@code members.id}
 */
public record Caller(long memberId) {
}
