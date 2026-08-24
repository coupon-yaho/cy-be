-- 쿠폰의 도메인 발급 시각과 실제 DB 저장 감사 시각을 분리해 유효기간 계산을 보존합니다.
ALTER TABLE `issuances`
    ADD COLUMN `created_at` datetime(6) NULL AFTER `expires_at`;

UPDATE `issuances`
SET `created_at` = `issued_at`
WHERE `created_at` IS NULL;

ALTER TABLE `issuances`
    MODIFY COLUMN `created_at` datetime(6) NOT NULL;
