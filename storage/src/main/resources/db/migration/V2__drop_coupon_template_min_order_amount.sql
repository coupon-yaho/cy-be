-- 주문 도메인이 없기에 최소주문금액이 필요 없음
ALTER TABLE coupon_templates drop column min_order_amount;
