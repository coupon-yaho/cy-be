-- V17의 템플릿 변경과 분리해 회차 스키마 변경 실패 시 이 버전만 재시도할 수 있게 합니다.
DROP TEMPORARY TABLE IF EXISTS v18_coupon_policy_guard;

CREATE TEMPORARY TABLE v18_coupon_policy_guard (
    unsupported_count BIGINT NOT NULL,
    CHECK (unsupported_count = 0)
);

INSERT INTO v18_coupon_policy_guard (unsupported_count)
SELECT COUNT(*)
  FROM coupons
 WHERE policy_type = 'DATA_GRANT';

DROP TEMPORARY TABLE v18_coupon_policy_guard;

ALTER TABLE coupons
    DROP COLUMN data_grant_mb,
    MODIFY COLUMN policy_type VARCHAR(20) NOT NULL
        COMMENT 'PERCENT_CAPPED / FIXED_AMOUNT',
    ADD CONSTRAINT ck_coupons_policy_type
        CHECK (policy_type IN ('PERCENT_CAPPED', 'FIXED_AMOUNT'));
