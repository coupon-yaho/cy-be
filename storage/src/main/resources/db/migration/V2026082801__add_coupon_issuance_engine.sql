ALTER TABLE coupons
    ADD COLUMN issuance_engine_version varchar(10) NULL DEFAULT 'V1'
        COMMENT '회차별 발급 엔진. NULL은 하위 호환을 위해 V1로 판정',
    ADD COLUMN issuance_engine_locked boolean NOT NULL DEFAULT FALSE
        COMMENT '인스턴스가 회차 정의를 읽은 뒤 발급 엔진 변경 금지',
    ADD CONSTRAINT ck_coupon_issuance_engine_version
        CHECK (issuance_engine_version IS NULL
            OR issuance_engine_version IN ('V1', 'V2'));
