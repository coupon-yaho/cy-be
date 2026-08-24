ALTER TABLE `benchmark_runs`
    DROP CHECK `ck_run_archive_status`,
    MODIFY COLUMN `archive_status` varchar(11) NOT NULL DEFAULT 'NONE'
        COMMENT 'NONE · IN_PROGRESS · DONE · FAILED. run_status와 독립이다',
    ADD COLUMN `archive_claimed_at` datetime(6) NULL
        COMMENT 'IN_PROGRESS retry lease 획득 시각. 만료된 claim은 다른 프로세스가 회수한다'
        AFTER `archive_failure_reason`,
    ADD COLUMN `archive_claim_token` char(36) NULL
        COMMENT '현재 archive 소유자의 UUID v4 fencing token'
        AFTER `archive_claimed_at`,
    ADD CONSTRAINT `ck_run_archive_status` CHECK (
        `archive_status` COLLATE utf8mb4_0900_as_cs
            IN ('NONE', 'IN_PROGRESS', 'DONE', 'FAILED')),
    ADD CONSTRAINT `ck_run_archive_claim` CHECK (
        (`archive_status` = 'IN_PROGRESS') = (`archive_claimed_at` IS NOT NULL)
        AND (`archive_status` = 'IN_PROGRESS') = (`archive_claim_token` IS NOT NULL));
