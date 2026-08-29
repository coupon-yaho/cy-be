ALTER TABLE `coupon_templates`
    ADD COLUMN `created_at` datetime(6) NULL,
    ADD COLUMN `updated_at` datetime(6) NULL;

UPDATE `coupon_templates`
SET `created_at` = CURRENT_TIMESTAMP(6),
    `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `created_at` IS NULL
   OR `updated_at` IS NULL;

ALTER TABLE `coupon_templates`
    MODIFY COLUMN `created_at` datetime(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),
    MODIFY COLUMN `updated_at` datetime(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6);
