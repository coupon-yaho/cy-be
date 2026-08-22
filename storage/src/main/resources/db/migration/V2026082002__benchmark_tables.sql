-- 부하 측정 회차의 메타와 공식 요약. 회차를 식별하고, 그 수치가 어떤 조건에서 나왔는지를 함께 남긴다.
--
-- 측정은 되돌릴 수 없다
--   회차를 다시 돌리려면 처음부터다. 그래서 "나중에 컬럼을 더하자" 가 이 테이블에서는
--   "그 회차는 영원히 그 값을 모른다" 와 같은 말이다. 기록해 둘 수 있는 조건은 지금 다 넣는다.
--
-- 시계열은 여기 없다 (B안 확정)
--   measurement_samples · stock_samples 를 만들지 않는다. 실시간 1초 시계열은 Prometheus 가
--   보관하고(retention 30d), 완료 회차의 MySQL 사본은 OBS-22 의 run_timeseries 가 따로 갖는다.
--   이 테이블은 회차 메타와 공식 요약만 담는다.
--
-- 마이그레이션 번호
--   V2026082001__issue_attempts.sql 의 주석이 "같은 날 두 번째는 V2026082002 다 —
--   benchmark_runs(OBS-14b)가 가져간다" 고 이 자리를 예약해 뒀다. 그 주석은 Flyway checksum 에
--   들어가 있어 고칠 수 없으므로 번호는 협상 대상이 아니다. V2026082003 은 OBS-22 몫으로 비워 둔다.
--   날짜와 일련번호 사이에 _ 를 넣지 않고 일련번호는 두 자리 고정이다 — 이유는 V2026082001 참고.
--
-- CREATE TABLE IF NOT EXISTS 를 쓰지 않는 이유도 V2026082001 과 같다
--   조용히 skip 되면 Flyway 가 success=1 로 "적용됐다" 고 거짓말한다. 복구는 DROP TABLE 로 끝나지
--   않는다 — flyway_schema_history 의 success=0 행이 이후 모든 migrate 를 거부하므로 flyway repair 가 필요하다.

-- ─────────────────────────────────────────────────────────────────────────────
-- 왜 시각이 셋인가
--
--   started_at              회차 시작
--   load_stopped_at         부하 종료
--   observation_stopped_at  관측 종료
--
--   v3 는 부하가 끝난 뒤에도 Kafka 백로그가 0 으로 수렴할 때까지 시계열이 필요하고,
--   그 수렴 곡선이 "비동기여도 결국 맞는다" 의 증명이다. 뒤의 둘을 하나로 합치면 그 구간을
--   잘라내게 되어 v3 의 핵심 증거가 사라진다. PromQL 질의 범위와 OBS-22 의 archive 구간이
--   observation_stopped_at 기준이다 — load_stopped_at 에서 자르면 안 된다.
--
--   v1 · v2 는 둘이 거의 같은 순간일 수 있다. 그래서 시각 순서 CHECK 는 > 가 아니라 >= 다.
--   "v3 는 뒤의 둘이 실제로 벌어져야 한다" 는 회차 운용의 문제라 스키마가 아니라 서비스가 본다.
--
-- 왜 상태가 둘인가
--
--   run_status      RUNNING · LOAD_STOPPED · OBSERVED · FINALIZED
--   archive_status  NONE · DONE · FAILED
--
--   archive 가 실패해도 회차 자체는 완료다. 하나로 두면 실패한 archive 때문에 회차가 미완으로
--   남고, "회차는 끝났는데 archive 만 다시 돌리면 된다" 는 재실행 조건을 표현할 수 없다.
--   OBS-22 는 finalize 와 별도 트랜잭션으로 돌고 결과를 archive_status 에만 쓴다.
--
-- 왜 server_* / client_* 를 처음부터 나누는가
--
--   나중에 섞이면 비교표가 무의미해진다. 서버가 스스로 잰 값과 종단에서 잰 값은 다른 수치이고,
--   둘이 벌어지는 폭 자체가 관측 대상이다(네트워크·큐 대기).
--
--   접두어를 도구 이름으로 짓지 않는다 — k6_ 를 스키마에 박으면 부하 도구를 바꾸는 순간
--   컬럼명이 거짓이 된다(전환 검토 중이다). 도구·버전·스크립트 해시는 load_tool* 메타 컬럼이 받는다.
--
--   여기 들어가는 공식 p95 · p99 · TPS 는 부하 생성기 원본 표본으로 다시 계산한 값이다.
--   도구 요약값을 그대로 넣지 않는다 — 분산 실행 시 k6 OSS 는 노드별 요약을 병합하지 않고,
--   Locust master 는 응답시간을 두 자리 유효숫자로 버킷화한다. 도구마다 산식이 달라
--   요약값을 그대로 실으면 도구를 바꾸는 순간 과거 회차와의 비교가 끊긴다.
--
-- 왜 benchmark_run_id 쪽에 FK 가 없고, 여기 coupon_id 에도 안 거는가
--
--   issue_attempts.benchmark_run_id 는 일반 운영 요청이 회차에 속하지 않아 NULL 이라 FK 가 불가능하다.
--   coupon_id 는 사정이 다르다 — NULL 이 드물다. 그런데도 걸지 않는다.
--   회차 기록은 쿠폰보다 오래 살아야 한다. 측정용 쿠폰(회차마다 새로 만드는 재고 10,000짜리)을
--   정리하는 순간 FK 가 회차 행을 함께 지우거나(CASCADE) 정리 자체를 막는다(RESTRICT).
--   둘 다 틀렸다 — 쿠폰이 사라져도 "그 회차가 어떤 조건에서 무슨 수치를 냈는지" 는 남아야 한다.
--   coupons 는 A 소유(V1__init_schema)라 B 의 테이블이 그쪽 수명주기를 붙잡는 것도 곤란하다.
--
-- MySQL 의 CHECK 는 NULL 이면 통과시킨다 (FALSE 일 때만 거부한다)
--   issue_attempts 에서 실제로 밟아 CY-253 리뷰에서 수정했다. 아래 CHECK 는 NULL 가능 컬럼을
--   쓰는 모든 분기에 IS NOT NULL 을 명시적으로 붙인다. 회귀 테스트는 BenchmarkRunsMigrationTest 다 —
--   실제 MySQL 에 적용한 뒤 원시 INSERT 로 경계값을 깨뜨려 본다(IssueAttemptsMigrationTest 와 같은 방식).
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE `benchmark_runs` (
    `id`                        bigint       NOT NULL AUTO_INCREMENT,

    -- 회차 식별. 사람이 정하는 키다(예: V3-MAIN-01). start 를 두 번 눌러도 같은 회차가 둘로
    -- 갈리지 않게 하고, 부하 생성기 스크립트와 회차 행을 잇는 이름이기도 하다.
    `run_key`                   varchar(64)  NOT NULL COMMENT '회차 키. 중복 start 를 uk_run_key 가 거부한다',
    `run_type`                  varchar(10)  NOT NULL COMMENT 'WARMUP(데이터 버림) · MAIN · CHAOS',
    `scenario_code`             varchar(64)  NOT NULL COMMENT '부하 시나리오 코드. 생성기 스크립트와 공유하는 식별자',

    `engine_version`            varchar(10)  NOT NULL COMMENT '이 회차의 권위값. 회차 도중 런타임 토글(OBS-19)이 바뀌어도 기준은 시작 시점에 박제된 이 값이다',
    `release_stage`             varchar(10)  NOT NULL,
    `queue_mode`                varchar(10)  NOT NULL,
    `coupon_id`                 bigint       NULL COMMENT 'coupons.id. 회차마다 새 쿠폰(재고 10,000)이라 재고 리셋 절차가 없다. FK 는 걸지 않는다 — 위 주석 참고',

    `run_status`                varchar(20)  NOT NULL COMMENT 'RUNNING · LOAD_STOPPED · OBSERVED · FINALIZED',
    `archive_status`            varchar(10)  NOT NULL DEFAULT 'NONE' COMMENT 'NONE · DONE · FAILED. run_status 와 독립이다 — archive 실패가 회차를 미완으로 만들지 않는다',

    -- 진행 중인 회차가 둘일 수 없다는 것을 DB 가 지킨다.
    --   두 회차가 동시에 돌면 이벤트의 benchmark_run_id 가 섞이고, 섞인 회차는 사후에 분리할 방법이 없다.
    --   앱에서 SELECT 후 INSERT 로 막으면 N=3 이라 세 인스턴스가 같은 순간에 통과할 수 있다.
    --   MySQL 에는 부분 유니크 인덱스가 없으므로 RUNNING 일 때만 1, 아니면 NULL 인 가상 컬럼에 유니크를 건다 —
    --   유니크 인덱스는 NULL 을 여러 개 허용하므로 끝난 회차는 서로 부딪히지 않는다.
    --   VIRTUAL 이라 행에 저장되지 않고 인덱스에만 들어간다.
    `running_guard`             tinyint      GENERATED ALWAYS AS (CASE WHEN `run_status` = 'RUNNING' THEN 1 ELSE NULL END) VIRTUAL,

    `started_at`                datetime(6)  NOT NULL,
    `load_stopped_at`           datetime(6)  NULL COMMENT '부하 종료',
    `observation_stopped_at`    datetime(6)  NULL COMMENT '관측 종료. PromQL 질의 범위와 OBS-22 archive 구간의 오른쪽 끝이다',
    `finalized_at`              datetime(6)  NULL,

    -- 계정 식별자만 넣는다. 이름·이메일 같은 개인 식별 정보를 넣지 않는다 — 이 값은 회차 목록 응답과
    -- 오류 detail 로그를 타고 밖으로 나간다. 호출자 식별은 Caller(record Caller(long memberId)) 가
    -- members.id 하나로 하므로, 그 값을 문자열로 넣으면 충분하다.
    `requested_by`              varchar(100) NOT NULL COMMENT '회차를 연 계정 식별자(members.id). 이름·이메일 금지 — 감사 주체이지 인스턴스 식별자가 아니다',
    `load_stop_reason`          varchar(200) NULL COMMENT '부하를 왜 끊었는지. 계획대로 끝난 회차는 NULL 이다',
    `archive_failure_reason`    varchar(200) NULL COMMENT 'archive_status = FAILED 의 이유. 재실행 판단 근거다',

    -- ── 토폴로지·자원 조건 ────────────────────────────────────────────────────
    -- 기록이 없으면 나중에 "이 수치가 어떤 조건에서 나왔는지" 재현이 안 된다.
    `app_replicas`              int          NOT NULL COMMENT 'api 인스턴스 수 N',
    `available_processors`      int          NOT NULL COMMENT 'DEC-07. Runtime.availableProcessors() 실측값이다 — 가정을 설정에 박는 대신 실제 값을 기록한다. 인스턴스를 한 머신에 몰아넣는 구성으로 바뀌어도 그 사실이 데이터에 남는다',
    `cpu_millicores_total`      int          NULL COMMENT 'compose 에 cpus 제한이 한 줄도 없어(실측) 지금은 강제되지 않는다. cpuset 으로 격리하면 그때 채운다',
    `memory_mb_total`           int          NULL,
    `tomcat_workers_total`      int          NOT NULL COMMENT 'worker 총량. Little의 법칙 기준값이지 첫 관문이 아니다',
    `tomcat_max_connections`    int          NULL COMMENT '2만 연결이 1초에 들이닥치면 worker 60 이 아니라 이 값과 accept-count 가 먼저 받는다. 여기서 잘리면 세 버전이 전부 똑같이 소켓 거부만 내고 끝나 구분이 안 된다',
    `tomcat_accept_count`       int          NULL COMMENT '위와 같은 이유. 회차 전에 확정하고 기록한다',
    `hikari_pool_total`         int          NOT NULL COMMENT 'N 으로 나눠떨어지는 값을 쓴다(DEC-07). offered RPS 와 무관하다 — 초과분은 풀에서 대기하고, 그 대기가 쌓이는 걸 보는 게 v1 측정의 목적이다',
    `mysql_max_connections`     int          NOT NULL COMMENT '50 유지. N=3 이면 여유가 3뿐이라 회차 중 mysql CLI 수동 접속이 금지다',

    -- ── 부하 입력 ────────────────────────────────────────────────────────────
    -- 램프가 아니라 스파이크다. 선착순이라 오픈 0초에 전원이 몰리고, 천천히 올리는 부하는
    -- 서버에 워밍업 시간을 줘서 재고가 소진되는 첫 1초의 거동을 놓친다.
    `offered_rps`               int          NOT NULL COMMENT '총량이 아니라 도착률이다. 20,000 × 5초 = 100,000건',
    `load_hold_seconds`         int          NOT NULL COMMENT '스파이크 유지 시간',
    `observation_hold_seconds`  int          NOT NULL COMMENT '부하 종료 뒤 회복을 관측하는 시간. v3 의 수렴 구간이 여기 들어간다',
    `stock_total`               int          NULL COMMENT '이 회차 쿠폰의 재고',
    `generator_idle_rtt_millis` decimal(9,3) NULL COMMENT '회차 전 유휴 RTT. 나중에 서버 지연과 네트워크를 가르는 근거다',

    -- ── 부하 도구 메타 ───────────────────────────────────────────────────────
    -- 도구 이름은 접두어가 아니라 여기에만 있다. 도구를 바꿔도 컬럼명이 거짓이 되지 않는다.
    `load_tool`                 varchar(32)  NULL,
    `load_tool_version`         varchar(32)  NULL,
    `load_script_hash`          char(64)     NULL COMMENT '스크립트 SHA-256. 같은 시나리오 코드로 다른 스크립트를 돌린 회차를 가른다',

    -- ── client_* 공식 요약 (부하 생성기 원본 표본에서 재계산) ────────────────
    `client_request_count`      bigint        NULL,
    `client_failure_count`      bigint        NULL COMMENT 'timeout · 연결 실패 포함',
    `client_dropped_iterations` bigint        NULL COMMENT '회차 유효 조건은 0 이다. open model 은 서버가 느려지면 VU 가 묶여 도착률이 조용히 목표 아래로 떨어지는데, 그걸 서버 한계로 착각하면 회차 전체가 거짓이 된다',
    `client_tps`                decimal(12,3) NULL,
    `client_p95_millis`         decimal(10,3) NULL,
    `client_p99_millis`         decimal(10,3) NULL,
    `client_measured_at`        datetime(6)   NULL COMMENT '이 요약이 확정된 시각. 여섯 값과 함께 들어오거나 함께 비어 있다',

    -- ── server_* 요약 ────────────────────────────────────────────────────────
    `server_request_count`      bigint        NULL,
    `server_failure_count`      bigint        NULL,
    `server_tps`                decimal(12,3) NULL,
    `server_p95_millis`         decimal(10,3) NULL,
    `server_p99_millis`         decimal(10,3) NULL,
    `server_measured_at`        datetime(6)   NULL,

    -- ── FINAL 진입 게이트 ────────────────────────────────────────────────────
    `observed_lag_total`        bigint        NULL COMMENT '관측 종료 시점의 Kafka 백로그. 0 이 아니면 OBSERVED 로 못 넘어간다 — 수렴 전에 회차를 닫으면 v3 의 적재가 잘린 채 공식 결과가 된다',

    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_run_key` (`run_key`),
    UNIQUE KEY `uk_run_running` (`running_guard`),

    -- COLLATE 를 붙이는 이유 — 테이블 콜레이션이 utf8mb4_0900_ai_ci 라 IN 비교가 대소문자를 무시한다.
    --   실측: run_status = 'running' 이 CHECK 를 통과하고 소문자 그대로 저장된다. 그런데 읽는 쪽은
    --   BenchmarkRunStatus.valueOf 라 "No enum constant ... running" 으로 죽는다.
    --   값은 들어가는데 읽을 때 죽는 행이 만들어지는 것이고, 하필 이 CHECK 들이 존재하는 유일한 이유가
    --   그 경로(백필·수동 보정)다. as_cs 로 바꾸면 대소문자가 다른 값은 애초에 못 들어온다.
    --
    --   running_guard 는 그대로 둔다. ai_ci 덕에 'running' 도 매칭돼 동시 RUNNING 가드는 안 뚫렸고,
    --   위 CHECK 가 소문자를 막으면 그 입력 자체가 사라진다.
    CONSTRAINT `ck_run_status` CHECK (
        `run_status` COLLATE utf8mb4_0900_as_cs IN ('RUNNING', 'LOAD_STOPPED', 'OBSERVED', 'FINALIZED')),
    CONSTRAINT `ck_run_archive_status` CHECK (
        `archive_status` COLLATE utf8mb4_0900_as_cs IN ('NONE', 'DONE', 'FAILED')),
    CONSTRAINT `ck_run_type` CHECK (
        `run_type` COLLATE utf8mb4_0900_as_cs IN ('WARMUP', 'MAIN', 'CHAOS')),

    -- 상태와 시각이 어긋난 행을 만들 수 없다. 양쪽 다 NOT NULL 인 run_status 로만 판정하므로
    -- NULL 통과 구멍이 없다 — IS NOT NULL 은 1/0 을 내고 IN 도 1/0 을 낸다.
    CONSTRAINT `ck_run_status_times` CHECK (
        (`load_stopped_at` IS NOT NULL) = (`run_status` IN ('LOAD_STOPPED', 'OBSERVED', 'FINALIZED'))
        AND (`observation_stopped_at` IS NOT NULL) = (`run_status` IN ('OBSERVED', 'FINALIZED'))
        AND (`finalized_at` IS NOT NULL) = (`run_status` = 'FINALIZED')
    ),

    -- >= 인 이유는 위 주석 참고. v1 · v2 는 부하 종료와 관측 종료가 같은 순간일 수 있다.
    CONSTRAINT `ck_run_time_order` CHECK (
        (`load_stopped_at` IS NULL OR `load_stopped_at` >= `started_at`)
        AND (`observation_stopped_at` IS NULL
             OR (`load_stopped_at` IS NOT NULL AND `observation_stopped_at` >= `load_stopped_at`))
        AND (`finalized_at` IS NULL
             OR (`observation_stopped_at` IS NOT NULL AND `finalized_at` >= `observation_stopped_at`))
    ),

    -- lag=0 을 확인하지 않은 회차는 OBSERVED 로도, FINALIZED 로도 갈 수 없다.
    CONSTRAINT `ck_run_lag_gate` CHECK (
        (`observed_lag_total` IS NULL OR `observed_lag_total` >= 0)
        AND (`run_status` NOT IN ('OBSERVED', 'FINALIZED')
             OR (`observed_lag_total` IS NOT NULL AND `observed_lag_total` = 0))
    ),

    -- 공식 결과가 없는 회차를 확정할 수 없다. client_* 가 공식이고 server_* 는 대조용이라
    -- 여기서 요구하는 것은 client 쪽이다.
    CONSTRAINT `ck_run_final_summary` CHECK (
        `run_status` <> 'FINALIZED' OR `client_measured_at` IS NOT NULL
    ),

    CONSTRAINT `ck_run_topology` CHECK (
        `app_replicas` > 0 AND `available_processors` > 0
        AND `tomcat_workers_total` > 0 AND `hikari_pool_total` > 0 AND `mysql_max_connections` > 0
        AND (`cpu_millicores_total` IS NULL OR `cpu_millicores_total` > 0)
        AND (`memory_mb_total` IS NULL OR `memory_mb_total` > 0)
        AND (`tomcat_max_connections` IS NULL OR `tomcat_max_connections` > 0)
        AND (`tomcat_accept_count` IS NULL OR `tomcat_accept_count` >= 0)
    ),

    CONSTRAINT `ck_run_load_input` CHECK (
        `offered_rps` > 0 AND `load_hold_seconds` > 0 AND `observation_hold_seconds` >= 0
        AND (`stock_total` IS NULL OR `stock_total` >= 0)
        AND (`generator_idle_rtt_millis` IS NULL OR `generator_idle_rtt_millis` >= 0)
    ),

    -- 요약은 통째로 들어오거나 통째로 비어 있다. 반쯤 채워진 요약은 "아직 안 올라왔다" 와
    -- "이 값은 못 쟀다" 가 같은 모양이 되어 비교표에서 구분되지 않는다.
    CONSTRAINT `ck_run_client_summary_atomic` CHECK (
        (`client_measured_at` IS NULL) = (`client_request_count` IS NULL)
        AND (`client_measured_at` IS NULL) = (`client_failure_count` IS NULL)
        AND (`client_measured_at` IS NULL) = (`client_dropped_iterations` IS NULL)
        AND (`client_measured_at` IS NULL) = (`client_tps` IS NULL)
        AND (`client_measured_at` IS NULL) = (`client_p95_millis` IS NULL)
        AND (`client_measured_at` IS NULL) = (`client_p99_millis` IS NULL)
    ),

    CONSTRAINT `ck_run_client_summary_range` CHECK (
        (`client_request_count` IS NULL OR `client_request_count` >= 0)
        AND (`client_failure_count` IS NULL OR `client_failure_count` >= 0)
        AND (`client_dropped_iterations` IS NULL OR `client_dropped_iterations` >= 0)
        AND (`client_tps` IS NULL OR `client_tps` >= 0)
        AND (`client_p95_millis` IS NULL OR `client_p95_millis` >= 0)
        AND (`client_p99_millis` IS NULL OR `client_p99_millis` >= 0)
        AND (`client_p95_millis` IS NULL OR `client_p99_millis` IS NULL
             OR `client_p99_millis` >= `client_p95_millis`)
        AND (`client_request_count` IS NULL OR `client_failure_count` IS NULL
             OR `client_failure_count` <= `client_request_count`)
    ),

    CONSTRAINT `ck_run_server_summary_atomic` CHECK (
        (`server_measured_at` IS NULL) = (`server_request_count` IS NULL)
        AND (`server_measured_at` IS NULL) = (`server_failure_count` IS NULL)
        AND (`server_measured_at` IS NULL) = (`server_tps` IS NULL)
        AND (`server_measured_at` IS NULL) = (`server_p95_millis` IS NULL)
        AND (`server_measured_at` IS NULL) = (`server_p99_millis` IS NULL)
    ),

    CONSTRAINT `ck_run_server_summary_range` CHECK (
        (`server_request_count` IS NULL OR `server_request_count` >= 0)
        AND (`server_failure_count` IS NULL OR `server_failure_count` >= 0)
        AND (`server_tps` IS NULL OR `server_tps` >= 0)
        AND (`server_p95_millis` IS NULL OR `server_p95_millis` >= 0)
        AND (`server_p99_millis` IS NULL OR `server_p99_millis` >= 0)
        AND (`server_p95_millis` IS NULL OR `server_p99_millis` IS NULL
             OR `server_p99_millis` >= `server_p95_millis`)
        AND (`server_request_count` IS NULL OR `server_failure_count` IS NULL
             OR `server_failure_count` <= `server_request_count`)
    ),

    -- IS NOT NULL 만으로는 부족하다. '' 도 NOT NULL 이라 "이유 없는 FAILED" 가 그대로 들어온다 —
    -- 재실행 판단의 유일한 근거가 빈 문자열로 남는다.
    --
    -- TRIM 도 부족하다. MySQL 의 TRIM 은 기본으로 <b>스페이스(CHAR(32))만</b> 자르고 탭·줄바꿈은
    -- 그대로 둔다(실측: '\t' · '\n' 만 있는 이유가 통과했다). 그러면 앱의 isBlank 와 DB 의 판정이
    -- 갈린다 — 서비스를 지나는 요청은 막히는데 백필·수동 보정은 통과한다. 하필 그 경로가 이
    -- CHECK 가 존재하는 유일한 이유다. REGEXP_LIKE 의 [:space:] 는 탭·줄바꿈·캐리지리턴을 포함하므로
    -- "공백 아닌 문자가 하나라도 있는가" 를 묻는 쪽으로 바꾼다.
    CONSTRAINT `ck_run_archive_reason` CHECK (
        (`archive_status` = 'FAILED')
            = (`archive_failure_reason` IS NOT NULL
               AND REGEXP_LIKE(`archive_failure_reason`, '[^[:space:]]'))
    )
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- 인덱스는 두 유니크 키 말고 만들지 않는다
--   이 테이블은 프로젝트 전체를 통틀어 수십 행이다(세 버전 × 회차 몇 번 + 워밍업).
--   목록 조회가 started_at DESC 로 정렬하지만 수십 행 풀스캔은 인덱스를 탈 이유가 없고,
--   issue_attempts 와 달리 적재 경로도 아니라 인덱스 비용을 걱정할 자리도 아니다.
--   uk_run_key 는 중복 start 방어, uk_run_running 은 동시 RUNNING 방어라 둘 다 제약이지 조회용이 아니다.
--
-- instance_id 컬럼을 두지 않는다 (DEC-08)
--   Prometheus 가 scrape 때 이미 instance 라벨을 붙인다. 앱이 또 붙이면 이름표가 두 개가 되고
--   시계열 수가 그만큼 는다. 인스턴스별 비교는 Prometheus 의 instance 라벨로 하고,
--   이 테이블은 회차 전체의 조건만 담는다 — app_replicas 와 *_total 접미어가 그 뜻이다.
--
-- Chaos 회차에 메모리 버퍼를 쓰지 않는다
--   앱이 죽는 순간 버퍼가 사라지는데, 앱을 죽이는 게 그 회차의 목적이다. 그래서 회차 메타는
--   명령마다 곧바로 이 테이블에 쓰고, 시계열은 Prometheus 가 이미 긁어둔 것을 종료 후에 뽑는다.
