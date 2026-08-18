-- 처리 중인 멱등 레코드는 대상 검증 전 생성되므로 완료 전까지 추적 FK를 비워둘 수 있게 합니다.
ALTER TABLE idempotency_records
    MODIFY COLUMN member_id BIGINT NULL,
    MODIFY COLUMN issuance_id BIGINT NULL;
