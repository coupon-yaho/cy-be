-- 쿠폰 도메인의 updated_at 은 Instant/JDBC UTC 축과 비교됩니다.
-- DB 세션이 KST 인 상태에서 CURRENT_TIMESTAMP(6)로 기록된 미래값만 UTC 축으로 되돌립니다.
-- 과거·현재 값은 그대로 두어 정상 이력의 정밀도와 순서를 보존합니다.

UPDATE `coupon_stocks`
SET `updated_at` = UTC_TIMESTAMP(6)
WHERE `updated_at` > UTC_TIMESTAMP(6);

UPDATE `issuances`
SET `updated_at` = UTC_TIMESTAMP(6)
WHERE `updated_at` > UTC_TIMESTAMP(6);
