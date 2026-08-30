-- 쿠폰 사용 요청에서 서버가 발급할 주문번호를 DB AUTO_INCREMENT로 생성합니다.
CREATE TABLE coupon_order_numbers (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    created_at DATETIME(6) NOT NULL
);

-- 기존에 클라이언트가 전달한 주문번호를 예약해 신규 번호와 충돌하지 않게 합니다.
INSERT INTO coupon_order_numbers (id, created_at)
SELECT order_id, MIN(created_at)
FROM issuance_usages
GROUP BY order_id;
