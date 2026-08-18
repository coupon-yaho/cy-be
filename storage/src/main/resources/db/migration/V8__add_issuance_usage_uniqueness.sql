-- 사용 취소 이력은 보존하면서 동일 주문 중복과 발급 건별 활성 사용 중복을 DB에서 차단합니다.
ALTER TABLE issuance_usages
    MODIFY COLUMN order_id BIGINT NOT NULL,
    ADD COLUMN active_issuance_id BIGINT
        GENERATED ALWAYS AS (
            CASE
                WHEN canceled_at IS NULL THEN issuance_id
                ELSE NULL
            END
        ) STORED,
    ADD CONSTRAINT uk_issuance_usages_issuance_order
        UNIQUE (issuance_id, order_id),
    ADD CONSTRAINT uk_issuance_usages_active
        UNIQUE (active_issuance_id);
