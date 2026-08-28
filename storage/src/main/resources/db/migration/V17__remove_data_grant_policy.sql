-- DATA_GRANT 정책은 지원 대상이 아니므로 템플릿의 전용 컬럼과 허용 가능성을 제거합니다.
-- 두 테이블을 먼저 검사하고 테이블별 DDL을 버전으로 분리해 실패 후 재시도를 보장합니다.
DROP TEMPORARY TABLE IF EXISTS v17_coupon_policy_guard;

CREATE TEMPORARY TABLE v17_coupon_policy_guard (
    unsupported_count BIGINT NOT NULL,
    CHECK (unsupported_count = 0)
);

INSERT INTO v17_coupon_policy_guard (unsupported_count)
SELECT
    (SELECT COUNT(*)
       FROM coupon_templates
      WHERE policy_type = 'DATA_GRANT')
    +
    (SELECT COUNT(*)
       FROM coupons
      WHERE policy_type = 'DATA_GRANT');

DROP TEMPORARY TABLE v17_coupon_policy_guard;

ALTER TABLE coupon_templates
    DROP COLUMN data_grant_mb,
    MODIFY COLUMN policy_type VARCHAR(20) NOT NULL
        COMMENT 'PERCENT_CAPPED / FIXED_AMOUNT',
    ADD CONSTRAINT ck_coupon_templates_policy_type
        CHECK (policy_type IN ('PERCENT_CAPPED', 'FIXED_AMOUNT'));
