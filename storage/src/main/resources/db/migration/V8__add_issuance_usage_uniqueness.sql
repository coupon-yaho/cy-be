-- 제약 추가 전에 기존 데이터 오염을 확인하고 임의 정리 없이 명시적으로 배포를 차단합니다.
CREATE TEMPORARY TABLE v8_issuance_usage_precheck (
    null_order_id_count BIGINT NOT NULL,
    duplicate_order_count BIGINT NOT NULL,
    duplicate_active_count BIGINT NOT NULL,
    CONSTRAINT chk_v8_no_null_order_id
        CHECK (null_order_id_count = 0),
    CONSTRAINT chk_v8_no_duplicate_order
        CHECK (duplicate_order_count = 0),
    CONSTRAINT chk_v8_no_duplicate_active
        CHECK (duplicate_active_count = 0)
);

INSERT INTO v8_issuance_usage_precheck (
    null_order_id_count,
    duplicate_order_count,
    duplicate_active_count
)
SELECT
    (SELECT COUNT(*)
     FROM issuance_usages
     WHERE order_id IS NULL),
    (SELECT COUNT(*)
     FROM (
         SELECT issuance_id, order_id
         FROM issuance_usages
         WHERE order_id IS NOT NULL
         GROUP BY issuance_id, order_id
         HAVING COUNT(*) > 1
     ) duplicate_orders),
    (SELECT COUNT(*)
     FROM (
         SELECT issuance_id
         FROM issuance_usages
         WHERE canceled_at IS NULL
         GROUP BY issuance_id
         HAVING COUNT(*) > 1
     ) duplicate_active_usages);

DROP TEMPORARY TABLE v8_issuance_usage_precheck;

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
