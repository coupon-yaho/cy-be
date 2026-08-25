package com.kafkick.core.coupon.service.idempotency;

/**
 * 멱등 실행 값과 완료 결과 재사용 여부를 함께 반환합니다.
 *
 * @param value 실행 또는 저장 응답 복원으로 얻은 값
 * @param replayed 이미 완료된 응답을 복원했으면 {@code true}
 * @param <R> 멱등 실행 결과 타입
 */
public record IdempotentExecutionResult<R>(R value, boolean replayed) {
}
