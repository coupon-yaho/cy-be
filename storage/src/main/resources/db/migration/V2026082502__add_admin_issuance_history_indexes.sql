ALTER TABLE `issuance_histories`
    ADD INDEX `idx_issuance_histories_created_id` (`created_at` DESC, `id` DESC),
    ADD INDEX `idx_issuance_histories_issuance_created_id` (`issuance_id`, `created_at` DESC, `id` DESC),
    ALGORITHM=INPLACE,
    LOCK=NONE;

ALTER TABLE `issuances`
    ADD INDEX `idx_issuances_coupon_id` (`coupon_id`, `id`),
    ALGORITHM=INPLACE,
    LOCK=NONE;
