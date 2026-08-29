package com.kafkick.core.admin.stock;

/** 관리자 지표 계산이 사용하는 발급 엔진 중립 재고 스냅샷입니다. */
public record AdminStockSnapshot(long totalQuantity, long remainingQuantity) {

    /** 계산기가 잘못된 재고를 정상값으로 소비하지 않도록 생성 시 수량 범위를 검증합니다. */
    public AdminStockSnapshot {
        if (totalQuantity <= 0L) {
            throw new IllegalArgumentException("totalQuantity는 양수여야 합니다.");
        }
        if (remainingQuantity < 0L || remainingQuantity > totalQuantity) {
            throw new IllegalArgumentException("remainingQuantity는 0 이상 totalQuantity 이하여야 합니다.");
        }
    }

    /** 전체 수량에서 권위 있는 잔여 수량을 빼 현재까지 발급된 수량을 반환합니다. */
    public long issuedQuantity() {
        return totalQuantity - remainingQuantity;
    }
}
