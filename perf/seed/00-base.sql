-- 회차 무관 기반 데이터. 여러 번 돌려도 같은 상태가 된다.
--
-- ⚠️ policy_type 은 'PERCENT_CAPPED' | 'FIXED_AMOUNT' | 'DATA_GRANT' 다.
--    'RATE' 같은 값을 넣으면 발급이 500 으로 죽는다 — enum 변환 실패이고,
--    실패 지점이 시드가 아니라 발급 요청이라 원인이 안 보인다(실측).
INSERT INTO grades(code, bit_value) VALUES
  ('WELCOME',1),('SILVER',2),('GOLD',4),('VIP',8)
ON DUPLICATE KEY UPDATE bit_value = VALUES(bit_value);

INSERT INTO brands(id, name, category) VALUES (@BRAND_ID, '측정용브랜드', 'FOOD')
ON DUPLICATE KEY UPDATE name = VALUES(name);

INSERT INTO coupon_templates(
  id, brand_id, name, policy_type, discount_rate, max_discount_amount,
  valid_days, nth_week, day_of_week, start_time, duration_hours,
  stock_per_occurrence, eligible_grades_mask, active)
-- ⚠️ coupon_templates 에는 min_order_amount 가 없다(V2 가 지웠다). coupons 에는 있다.
VALUES (@TEMPLATE_ID, @BRAND_ID, '측정용템플릿', 'PERCENT_CAPPED', 10, 1000,
  30, 1, 'MON', '10:00:00', 2, 10000, 15, 1)
ON DUPLICATE KEY UPDATE name = VALUES(name), policy_type = VALUES(policy_type);
