-- DATA_GRANT 정책은 지원 대상이 아니므로 전용 컬럼과 허용 가능성을 함께 제거합니다.
-- 기존 DATA_GRANT 행이 있으면 CHECK 제약 추가가 실패하여 잘못된 데이터를 조용히 보존하지 않습니다.
ALTER TABLE coupon_templates
    DROP COLUMN data_grant_mb,
    MODIFY COLUMN policy_type VARCHAR(20) NOT NULL
        COMMENT 'PERCENT_CAPPED / FIXED_AMOUNT',
    ADD CONSTRAINT ck_coupon_templates_policy_type
        CHECK (policy_type IN ('PERCENT_CAPPED', 'FIXED_AMOUNT'));

ALTER TABLE coupons
    DROP COLUMN data_grant_mb,
    MODIFY COLUMN policy_type VARCHAR(20) NOT NULL
        COMMENT 'PERCENT_CAPPED / FIXED_AMOUNT',
    ADD CONSTRAINT ck_coupons_policy_type
        CHECK (policy_type IN ('PERCENT_CAPPED', 'FIXED_AMOUNT'));
