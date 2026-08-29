-- OBS-46. 지연 시계열에 시스템 실패 축을 더한다.
--
-- 값을 더하기만 하고 기존 'LATENCY_P99' 는 그대로 둔다. 완료 회차의 archive 는 불변이라
-- 개명하면 과거 행을 UPDATE 해야 하고, 그 순간 과거 회차와 현재 회차의 비교 축이 갈린다.
-- 'LATENCY_P99' 는 OBS-31 이래 성공 경로(outcome="success")를 뜻하며 이 마이그레이션은
-- 그 뜻을 바꾸지 않는다 — 기존 행은 한 줄도 건드리지 않는다.
--
-- ⚠️ 떨구기와 되살리기를 <한 ALTER 문>으로 묶는다. MySQL 은 CHECK 을 자리에서 못 고쳐
--    DROP 후 재생성해야 하는데, 두 문장으로 나누면 DDL 이 각각 즉시 커밋되어(MySQL 은 DDL 을
--    트랜잭션으로 묶지 않는다) 그 사이에 제약이 아예 없는 창이 생긴다. Flyway 의 스키마 락은
--    다른 Flyway 실행만 막지 그동안 살아 있는 애플리케이션의 INSERT 는 막지 않는다.
--    한 문장이면 그 창이 없다 — V2026082004 가 ck_run_archive_status 를 같은 방식으로 바꾼다.
--
-- COLLATE utf8mb4_0900_as_cs 는 대소문자를 구분한다. 소문자로 적재하면 값이 맞아도 거절된다 —
-- 같은 테이블의 ck_timeseries_state_value 와 같은 방식이다.
ALTER TABLE `run_timeseries`
    DROP CHECK `ck_timeseries_metric`,
    MODIFY COLUMN `metric` varchar(32) NOT NULL
        COMMENT 'STOCK_REMAINING · LATENCY_P99(성공 축) · LATENCY_P99_SYSTEM_FAILURE · DB_POOL_USAGE',
    ADD CONSTRAINT `ck_timeseries_metric` CHECK (
        `metric` COLLATE utf8mb4_0900_as_cs IN (
            'STOCK_REMAINING', 'LATENCY_P99', 'LATENCY_P99_SYSTEM_FAILURE', 'DB_POOL_USAGE'));
