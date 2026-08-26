ALTER TABLE `benchmark_runs`
    ADD COLUMN `consistency_status` varchar(11) NOT NULL DEFAULT 'NONE'
        COMMENT 'NONE · IN_PROGRESS · DONE · FAILED. run_status와 독립이다'
        AFTER `archive_status`,
    ADD COLUMN `consistency_failure_reason` varchar(200) NULL
        COMMENT 'FAILED 재실행 판단 근거'
        AFTER `archive_failure_reason`,
    ADD COLUMN `consistency_claimed_at` datetime(6) NULL
        COMMENT 'IN_PROGRESS retry lease 획득 시각'
        AFTER `consistency_failure_reason`,
    ADD COLUMN `consistency_claim_token` char(36) NULL
        COMMENT '현재 FINAL 정합성 작업 소유자의 UUID v4 fencing token'
        AFTER `consistency_claimed_at`,
    ADD CONSTRAINT `ck_run_consistency_status` CHECK (
        `consistency_status` COLLATE utf8mb4_0900_as_cs
            IN ('NONE', 'IN_PROGRESS', 'DONE', 'FAILED')),
    ADD CONSTRAINT `ck_run_consistency_claim` CHECK (
        (`consistency_status` COLLATE utf8mb4_0900_as_cs = 'IN_PROGRESS')
            = (`consistency_claimed_at` IS NOT NULL)
        AND (`consistency_status` COLLATE utf8mb4_0900_as_cs = 'IN_PROGRESS')
            = (`consistency_claim_token` IS NOT NULL)),
    ADD CONSTRAINT `ck_run_consistency_failure` CHECK (
        (`consistency_status` COLLATE utf8mb4_0900_as_cs = 'FAILED')
            = (`consistency_failure_reason` IS NOT NULL));

CREATE TABLE `consistency_finals` (
    `run_id` bigint NOT NULL,
    `coupon_id` bigint NOT NULL,
    `engine_version` varchar(10) NOT NULL,
    `evaluated_at` datetime(6) NOT NULL,
    `verdict` varchar(10) NOT NULL,
    `severity` varchar(10) NOT NULL,

    `active_db_gap_value` bigint NULL,
    `active_db_gap_state` varchar(11) NOT NULL,
    `active_db_gap_observed_at` datetime(6) NULL,
    `lua_gap_value` bigint NULL,
    `lua_gap_state` varchar(11) NOT NULL,
    `lua_gap_observed_at` datetime(6) NULL,
    `persist_gap_value` bigint NULL,
    `persist_gap_state` varchar(11) NOT NULL,
    `persist_gap_observed_at` datetime(6) NULL,
    `db_counter_gap_value` bigint NULL,
    `db_counter_gap_state` varchar(11) NOT NULL,
    `db_counter_gap_observed_at` datetime(6) NULL,
    `over_issued_value` bigint NULL,
    `over_issued_state` varchar(11) NOT NULL,
    `over_issued_observed_at` datetime(6) NULL,

    PRIMARY KEY (`run_id`),
    KEY `ix_consistency_final_latest` (`coupon_id`, `evaluated_at` DESC, `run_id` DESC),

    CONSTRAINT `ck_consistency_final_engine` CHECK (
        `engine_version` COLLATE utf8mb4_0900_as_cs IN ('V1', 'V2', 'V3')),
    CONSTRAINT `ck_consistency_final_verdict` CHECK (
        `verdict` COLLATE utf8mb4_0900_as_cs IN ('PASS', 'FAIL')),
    CONSTRAINT `ck_consistency_final_severity` CHECK (
        `severity` COLLATE utf8mb4_0900_as_cs IN ('NONE', 'WARN', 'CRITICAL')),
    CONSTRAINT `ck_final_active_db_gap` CHECK (
        ((`active_db_gap_state` COLLATE utf8mb4_0900_as_cs IN ('VALID', 'STALE'))
            AND `active_db_gap_value` IS NOT NULL AND `active_db_gap_observed_at` IS NOT NULL)
        OR ((`active_db_gap_state` COLLATE utf8mb4_0900_as_cs IN ('PENDING', 'UNAVAILABLE', 'N_A'))
            AND `active_db_gap_value` IS NULL AND `active_db_gap_observed_at` IS NULL)),
    CONSTRAINT `ck_final_lua_gap` CHECK (
        ((`lua_gap_state` COLLATE utf8mb4_0900_as_cs IN ('VALID', 'STALE'))
            AND `lua_gap_value` IS NOT NULL AND `lua_gap_observed_at` IS NOT NULL)
        OR ((`lua_gap_state` COLLATE utf8mb4_0900_as_cs IN ('PENDING', 'UNAVAILABLE', 'N_A'))
            AND `lua_gap_value` IS NULL AND `lua_gap_observed_at` IS NULL)),
    CONSTRAINT `ck_final_persist_gap` CHECK (
        ((`persist_gap_state` COLLATE utf8mb4_0900_as_cs IN ('VALID', 'STALE'))
            AND `persist_gap_value` IS NOT NULL AND `persist_gap_observed_at` IS NOT NULL)
        OR ((`persist_gap_state` COLLATE utf8mb4_0900_as_cs IN ('PENDING', 'UNAVAILABLE', 'N_A'))
            AND `persist_gap_value` IS NULL AND `persist_gap_observed_at` IS NULL)),
    CONSTRAINT `ck_final_db_counter_gap` CHECK (
        ((`db_counter_gap_state` COLLATE utf8mb4_0900_as_cs IN ('VALID', 'STALE'))
            AND `db_counter_gap_value` IS NOT NULL AND `db_counter_gap_observed_at` IS NOT NULL)
        OR ((`db_counter_gap_state` COLLATE utf8mb4_0900_as_cs IN ('PENDING', 'UNAVAILABLE', 'N_A'))
            AND `db_counter_gap_value` IS NULL AND `db_counter_gap_observed_at` IS NULL)),
    CONSTRAINT `ck_final_over_issued` CHECK (
        ((`over_issued_state` COLLATE utf8mb4_0900_as_cs IN ('VALID', 'STALE'))
            AND `over_issued_value` IS NOT NULL AND `over_issued_observed_at` IS NOT NULL)
        OR ((`over_issued_state` COLLATE utf8mb4_0900_as_cs IN ('PENDING', 'UNAVAILABLE', 'N_A'))
            AND `over_issued_value` IS NULL AND `over_issued_observed_at` IS NULL))
);
