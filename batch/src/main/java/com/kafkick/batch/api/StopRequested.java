// 검증 배치 중단 요청 응답입니다.
package com.kafkick.batch.api;

/**
 * <b>중단은 요청이지 완료가 아니다.</b> Spring Batch 는 플래그를 세우고, 잡이 청크 경계에서
 * 그것을 본다. 그래서 {@code signalled} 는 <i>"신호를 받았다"</i> 지 <i>"멈췄다"</i> 가 아니다 —
 * 실제로 멈췄는지는 조회로 확인한다.
 */
public record StopRequested(long executionId, boolean signalled) {
}
