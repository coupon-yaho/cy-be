// 검증 실행을 버린 결과입니다.
package com.kafkick.batch.api;

/**
 * <b>{@code stop} 과 다른 뜻이라 봉투도 다르다.</b> 저쪽은 <i>"신호를 보냈다"</i> 이고
 * 실제로 멈췄는지는 조회로 확인해야 한다. 이쪽은 <b>DB 를 즉시 최종 상태로 바꾸는 완료
 * 동작</b>이라 재확인할 것이 없다 — 같은 봉투·같은 202 로 답하면 클라이언트가 둘을
 * 구분할 수 없고, {@code StopRequested} 의 javadoc 이 여기서는 거짓이 된다.
 */
public record Abandoned(long executionId, String status) {
}
