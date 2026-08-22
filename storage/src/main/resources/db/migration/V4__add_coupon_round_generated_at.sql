-- 회차 생성 작업의 기준 시각을 실제 DB 저장 감사 시각과 분리해 보존합니다.
ALTER TABLE `coupons`
    ADD COLUMN `generated_at` datetime(6) NULL AFTER `status`;

UPDATE `coupons`
SET `generated_at` = `created_at`
WHERE `generated_at` IS NULL;

ALTER TABLE `coupons`
    MODIFY COLUMN `generated_at` datetime(6) NOT NULL;
