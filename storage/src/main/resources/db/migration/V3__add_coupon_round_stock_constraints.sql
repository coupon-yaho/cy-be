-- 쿠폰 회차 시간 범위와 재고 점유 불변식을 DB 최종 방어선으로 보장합니다.
ALTER TABLE `coupons`
    ADD CONSTRAINT `ck_coupon_round_time_range`
        CHECK (`close_at` > `open_at`);

ALTER TABLE `coupon_stocks`
    ADD CONSTRAINT `ck_coupon_stock_total_positive`
        CHECK (`total_quantity` > 0),
    ADD CONSTRAINT `ck_coupon_stock_active_range`
        CHECK (
            `active_count` >= 0
            AND `active_count` <= `total_quantity`
        );
