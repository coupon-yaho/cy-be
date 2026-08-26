ALTER TABLE `benchmark_runs`
    ADD COLUMN `consistency_status` varchar(11) NOT NULL DEFAULT 'NONE'
        COMMENT 'NONE · IN_PROGRESS · DONE · FAILED · EXPIRED. run_status와 독립이다'
        AFTER `archive_status`,
    -- batch 가 돌려주는 원인 본문(gap 이름 + state)이 200자를 넘겨 잘리면 재실행 판단이 막힌다.
    ADD COLUMN `consistency_failure_reason` varchar(500) NULL
        COMMENT 'FAILED 재실행 판단 근거'
        AFTER `archive_failure_reason`,
    ADD COLUMN `consistency_claimed_at` datetime(6) NULL
        COMMENT 'IN_PROGRESS retry lease 획득 시각'
        AFTER `consistency_failure_reason`,
    ADD COLUMN `consistency_claim_token` char(36) NULL
        COMMENT '현재 FINAL 정합성 작업 소유자의 UUID v4 fencing token'
        AFTER `consistency_claimed_at`,
    ADD COLUMN `consistency_attempt_count` int NOT NULL DEFAULT 0
        COMMENT 'claim 성공 횟수. 저장된 판정이 몇 번째 시도의 라이브 관측인지 되짚는 근거다'
        AFTER `consistency_claim_token`,
    ADD CONSTRAINT `ck_run_consistency_status` CHECK (
        `consistency_status` COLLATE utf8mb4_0900_as_cs
            IN ('NONE', 'IN_PROGRESS', 'DONE', 'FAILED', 'EXPIRED')),
    ADD CONSTRAINT `ck_run_consistency_claim` CHECK (
        (`consistency_status` COLLATE utf8mb4_0900_as_cs = 'IN_PROGRESS')
            = (`consistency_claimed_at` IS NOT NULL)
        AND (`consistency_status` COLLATE utf8mb4_0900_as_cs = 'IN_PROGRESS')
            = (`consistency_claim_token` IS NOT NULL)),
    ADD CONSTRAINT `ck_run_consistency_failure` CHECK (
        (`consistency_status` COLLATE utf8mb4_0900_as_cs IN ('FAILED', 'EXPIRED'))
            = (`consistency_failure_reason` IS NOT NULL));

CREATE TABLE `consistency_finals` (
    `run_id` bigint NOT NULL,
    `coupon_id` bigint NOT NULL,
    `engine_version` varchar(10) NOT NULL,
    -- 회차 확정 시각이다. 재시도해도 바뀌지 않으므로 gap별 observed_at보다 앞설 수 있다 —
    -- 실제 관측이 언제였는지는 각 observed_at 열이 갖는다.
    `evaluated_at` datetime(6) NOT NULL,
    -- FINAL 은 asOf 스냅샷이 아니라 그 시점의 라이브 관측이다. 재실행이면 값이 달라질 수
    -- 있으므로 몇 번째 시도의 값인지가 판정과 함께 남아야 한다.
    `attempt_no` int NOT NULL,
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
    -- 회차 전용이고 NOT NULL 이므로 run_timeseries 와 같은 규칙을 따른다. 회차를 지우면
    -- 판정도 함께 사라져야 고아 행이 "최신 FINAL" 로 남지 않는다.
    CONSTRAINT `fk_consistency_final_benchmark_run` FOREIGN KEY (`run_id`)
        REFERENCES `benchmark_runs` (`id`) ON DELETE CASCADE,
    KEY `ix_consistency_final_latest` (`coupon_id`, `evaluated_at` DESC, `run_id` DESC),

    CONSTRAINT `ck_consistency_final_engine` CHECK (
        `engine_version` COLLATE utf8mb4_0900_as_cs IN ('V1', 'V2', 'V3')),
    CONSTRAINT `ck_consistency_final_attempt` CHECK (`attempt_no` >= 1),
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
