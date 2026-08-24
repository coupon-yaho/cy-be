-- 사용 실적 감사 시각과 멱등 상태별 필수 대상 값의 무결성을 DB에서 보장합니다.
ALTER TABLE issuance_usages
    ADD COLUMN created_at DATETIME(6) NULL;

UPDATE issuance_usages
SET created_at = used_at
WHERE created_at IS NULL;

ALTER TABLE issuance_usages
    MODIFY COLUMN created_at DATETIME(6) NOT NULL;

ALTER TABLE idempotency_records
    ADD CONSTRAINT ck_idempotency_status_targets CHECK (
        (
            status = 'IN_PROGRESS'
            AND member_id IS NULL
            AND issuance_id IS NULL
            AND response_body IS NULL
        )
        OR
        (
            status = 'DONE'
            AND member_id IS NOT NULL
            AND issuance_id IS NOT NULL
            AND response_body IS NOT NULL
        )
    );
