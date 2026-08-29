-- 취소 시각 제약을 추가하기 전에 기존 역전 데이터를 명시적으로 차단합니다.
CREATE TEMPORARY TABLE v9_usage_cancel_time_precheck (
    invalid_cancel_time_count BIGINT NOT NULL,
    CONSTRAINT chk_v9_no_invalid_cancel_time
        CHECK (invalid_cancel_time_count = 0)
);

INSERT INTO v9_usage_cancel_time_precheck (invalid_cancel_time_count)
SELECT COUNT(*)
FROM issuance_usages
WHERE canceled_at IS NOT NULL
  AND canceled_at < used_at;

DROP TEMPORARY TABLE v9_usage_cancel_time_precheck;

ALTER TABLE issuance_usages
    ADD CONSTRAINT ck_issuance_usages_cancel_time
        CHECK (canceled_at IS NULL OR canceled_at >= used_at);
