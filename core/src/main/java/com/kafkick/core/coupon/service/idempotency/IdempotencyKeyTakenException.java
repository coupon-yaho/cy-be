package com.kafkick.core.coupon.service.idempotency;

/**
 * 같은 멱등키가 이미 기록돼 있어 권위 작업을 확정하지 못했음을 알립니다.
 *
 * <p>동시에 같은 키로 들어온 두 요청 중 진 쪽에서 발생합니다. 업무 실패가 아니라 재사용 신호이므로
 * {@code BusinessException}이 아니고, 호출부는 이 예외를 잡아 저장된 응답 복원으로 넘어갑니다.
 * 트랜잭션은 이 예외로 롤백되며, IN_PROGRESS 선점이 없으므로 별도 정리가 필요하지 않습니다.
 */
public class IdempotencyKeyTakenException extends RuntimeException {

    private final String idempotencyKey;

    public IdempotencyKeyTakenException(String idempotencyKey) {
        super("idempotencyKey=" + idempotencyKey);
        this.idempotencyKey = idempotencyKey;
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }
}
