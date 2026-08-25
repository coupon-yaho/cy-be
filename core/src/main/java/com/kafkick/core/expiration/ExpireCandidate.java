// 만료 후보 한 건입니다. 청크 범위를 정하는 데만 쓰입니다.
package com.kafkick.core.expiration;

/**
 * 만료 후보 한 건. <b>{@code id} 와 회차뿐이다</b> — 청크의 범위를 정하는 데 그 둘만 쓰이고,
 * 실제로 넘길지는 {@link ExpirationRepository#expireBatch} 의 조건부 {@code UPDATE} 가 다시
 * 판단한다.
 *
 * <p><b>여기 담긴 것은 "만료된다" 가 아니라 "만료 대상이었다" 다.</b> 후보 질의는 락을 안
 * 잡으므로, 읽은 뒤 {@code UPDATE} 전에 그 건이 사용·취소될 수 있다. 그래서 이 목록의
 * 건수를 만료 건수로 쓰면 안 된다 — 그 수는 {@code UPDATE} 의 매치 건수로만 온다.
 */
public record ExpireCandidate(long id, long couponId) {
}
