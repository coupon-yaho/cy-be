-- 완료 회차의 발표용 시계열 사본. Prometheus 원본은 보존하며 이 테이블로 이관하지 않는다.
CREATE TABLE `run_timeseries` (
    `id`                bigint       NOT NULL AUTO_INCREMENT,
    `benchmark_run_id`  bigint       NOT NULL,
    `metric`            varchar(32)  NOT NULL COMMENT 'STOCK_REMAINING · LATENCY_P99 · DB_POOL_USAGE',
    `snapshot_sequence` bigint       NOT NULL,
    `observed_at`       datetime(6)  NOT NULL COMMENT 'query_range 표본 자체의 시각',
    `value`             double       NULL COMMENT '값 없음은 0이 아니라 NULL',
    `state`             varchar(16)  NOT NULL COMMENT 'VALID · UNAVAILABLE · N_A',
    `source_instance`   varchar(255) NULL COMMENT 'p99 최댓값을 낸 Prometheus instance',

    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_run_metric_seq` (`benchmark_run_id`, `metric`, `snapshot_sequence`),
    KEY `ix_run_metric` (`benchmark_run_id`, `metric`, `observed_at`),

    -- 이 테이블은 회차 전용이고 표본은 회차 없이 의미가 없다. 운영 요청도 섞이는
    -- issue_attempts.benchmark_run_id 와 달리 FK를 걸 수 있으며 회차 삭제 때 함께 정리한다.
    CONSTRAINT `fk_timeseries_benchmark_run` FOREIGN KEY (`benchmark_run_id`)
        REFERENCES `benchmark_runs` (`id`) ON DELETE CASCADE,
    CONSTRAINT `ck_timeseries_metric` CHECK (
        `metric` COLLATE utf8mb4_0900_as_cs IN ('STOCK_REMAINING', 'LATENCY_P99', 'DB_POOL_USAGE')),
    CONSTRAINT `ck_timeseries_state_value` CHECK (
        (`state` COLLATE utf8mb4_0900_as_cs = 'VALID' AND `value` IS NOT NULL)
        OR (`state` COLLATE utf8mb4_0900_as_cs IN ('UNAVAILABLE', 'N_A') AND `value` IS NULL)),
    CONSTRAINT `ck_timeseries_sequence` CHECK (`snapshot_sequence` >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
