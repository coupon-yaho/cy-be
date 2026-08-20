-- 발급 시도 이력. Kafka coupon.issue.attempt 의 IssuanceFlowEvent 를 Consumer(OBS-15)가 적재한다.
--
-- 왜 issuance_histories 로는 안 되나
--   issuance_histories(A 소유)는 issuance_id 가 NOT NULL + FK 라 발급이 성사된 뒤의 상태 전이만 담는다.
--   재고 소진 · 중복 요청 · 타임아웃으로 튕긴 요청은 구조적으로 기록될 수 없는데,
--   관제가 봐야 하는 건 정확히 그쪽이다. 그래서 별도 테이블이다.
--
-- 왜 CREATE TABLE IF NOT EXISTS 가 아닌가
--   원격에 수동 DDL 이 있으면 IF NOT EXISTS 는 Flyway 가 조용히 skip 하고 success=1 로 기록한다.
--   컬럼이 다르면 앱은 정상 기동하고 benchmark_run_id 를 쓰는 순간 Unknown column 으로 죽는다.
--   그냥 CREATE TABLE 이면 시끄럽게 실패한다. 이 테이블에 보존할 운영 데이터는 없다.
--   복구는 DROP TABLE 만으로 끝나지 않는다 — flyway_schema_history 에 success=0 행이 남아
--   이후 모든 migrate 가 거부된다. DROP TABLE 뒤 flyway repair 로 실패 행을 지우고 다시 돌린다.
--
-- 왜 날짜 버전인가, 그리고 왜 날짜와 일련번호 사이에 _ 를 넣지 않는가
--   A 가 V2__~V99__ 를 쓰고 B 는 날짜 기반을 쓴다. 둘 다 V2__ 를 만들면 머지는 되는데
--   런타임에 checksum 충돌로 터지므로 대역을 나눈다.
--
--   Flyway 는 _ 를 소수점으로 읽는다. V20260820_1__ 이면 버전이 (20260820, 1) 두 부분이 되고,
--   _ 를 빼면 2,026,082,001 이라는 단일 정수가 된다. 일련번호는 두 자리로 고정한다 —
--   한 자리면 하루 10개를 넘길 때 자릿수가 늘어 순서가 뒤집힌다
--   (2026082010 이 다음 날 202608211 보다 크다). 폭이 같으면 영구히 안전하다.
--   같은 날 두 번째는 V2026082002 다 — benchmark_runs(OBS-14b)가 가져간다.
--
--   ⚠️ 이 대역 분리는 api 의 spring.flyway.out-of-order: true 를 전제로 한다(CY-253 에서 켰다).
--      날짜 기반 버전은 형식을 어떻게 적든 A 대역(2~99)보다 항상 크다. 이 마이그레이션이 적용된 DB 에
--      A 가 나중에 더 낮은 번호를 추가하면 기본값(false)에서
--      "Detected resolved migration not applied to database: N" 으로 api 기동이 죽는다.
--      빈 DB 에서 시작하면 전부 버전 순으로 한 번에 적용되므로 이 문제는 나타나지 않는다 —
--      오래 살아 있는 로컬 볼륨이나 공용 개발 DB 에서만 걸린다.
--      켜도 checksum 검증은 그대로 남으므로 위의 수동 DDL 방어는 유지된다.
--
-- 이 테이블을 TPS · 성공률 · 정합성의 원천으로 쓰지 않는다
--   attempt 토픽은 acks=0 이라 Kafka 전달 자체가 유실될 수 있다. 지연뿐 아니라 모든 카운트가 하한값이다.
--   유실은 부하 최고 구간에서 가장 많이 나므로 STOCK_EXHAUSTED 가 실제보다 적게 잡히고,
--   그대로 읽으면 "재고 소진이 별로 없었다"는 반대 결론이 나온다.
--   공식 지연 · 처리량은 부하 생성기 원본 표본이, 정합성은 verification_runs(A)가 원천이다.
--
-- INSERT 는 ON DUPLICATE KEY UPDATE id = id 로 넣는다
--   유니크 키가 둘이라 리밸런싱 후 재소비(정상 경로)에서 중복이 들어온다.
--   평범한 배치 INSERT 면 청크 전체가 Duplicate entry 로 롤백되고 offset 을 못 넘겨 무한 재시도한다.
--   ⚠️ INSERT IGNORE 는 쓰지 않는다 — 어느 키에 걸렸는지 구분하지 못해 uk_kafka 위반까지 함께 삼킨다.
--      무시된 건수는 affectedRows == 0 으로 세어 지표로 올린다(OBS-15).
--      정상 재소비라면 이 값이 리밸런싱 직후에만 튀고, 그 밖의 구간에서 계속 오르면 키 설계를 의심한다.
--
-- 보존 기간 삭제는 ingested_at 으로 경계를 잡고 id 키셋으로 끊어 돈다
--   occurred_at 은 프로듀서 시계라 N대 시계 차이로 근사다. 재소비된 지연 이벤트는 occurred_at 이 옛날인데
--   id 는 최신이라, occurred_at 으로 경계를 잡고 id 로 지우면 보존 기간 내 행이 함께 지워진다.
--   경계는 SELECT MAX(id) WHERE ingested_at < :cutoff 로 구한다 — 단일 Consumer 시계이고 id 와 단조 일치한다.
--   occurred_at 단독 범위 삭제는 인덱스 갱신 + 갭 락 + 언두 팽창이 Consumer 적재를 정면으로 막는다.
--   RANGE 파티셔닝은 쓸 수 없다 — MySQL 은 모든 유니크 키가 파티션 컬럼을 포함할 것을 요구하는데,
--   uk_event 에 시각 컬럼을 넣는 순간 중복 소비 차단이라는 그 키의 존재 이유가 사라진다.
--
-- CHARSET · COLLATE 를 명시하지만 V1__init_schema.sql 은 선언이 없어 서버 기본값을 따른다
--   현재 서버가 utf8mb4 라 양쪽 collation 이 utf8mb4_0900_ai_ci 로 같다.
--   서버 기본이 utf8mb3 인 이미지를 쓰는 환경이 생기면 V1 쪽만 달라져 issuances.code 조인이 1267 로 거부된다.
--   TODO(A 소유 · 티켓 미발급): V1__init_schema.sql 에 CHARSET · COLLATE 선언을 박아야 근본 해결이다.
--
-- 시각 정밀도는 V1 의 datetime(6)에 맞춘다
--   MySQL 은 초과 정밀도를 버리지 않고 반올림한다. Instant 는 나노초라 (6)에서도 반올림이 남으므로
--   Consumer 가 truncatedTo(ChronoUnit.MICROS) 로 잘라 넣는다. 안 그러면 ingested_at < occurred_at 이 되어
--   지연이 음수로 나온다.

CREATE TABLE `issue_attempts` (
    `id`                   bigint       NOT NULL AUTO_INCREMENT,
    `schema_version`       int          NOT NULL COMMENT 'IssuanceFlowEvent.CURRENT_SCHEMA_VERSION. 다른 값은 OBS-15 가 DLT 로 격리한다',
    `event_id`             binary(16)   NOT NULL COMMENT 'UUID. char(36) 보다 인덱스가 작다. msb·lsb 그대로라 UUID_TO_BIN(x, 0) — 조회는 BIN_TO_UUID(event_id)',
    `event_type`           varchar(20)  NOT NULL COMMENT 'EventType 3종. issuance_histories.event_type(30)과는 다른 enum 이다',
    `request_id`           varchar(36)  NULL COMMENT 'QUEUE_ADMITTED 에는 없다',
    `member_id`            bigint       NOT NULL,
    `coupon_id`            bigint       NOT NULL COMMENT 'coupons.id (회차)',
    `issuance_id`          bigint       NULL COMMENT '201 ISSUE_RESULT 에만 있다. FK 를 걸지 않는다 — 발급 실패 행이 대부분이다',
    `issuance_code`        char(16)     NULL COMMENT 'issuances.code 와 타입을 맞춘다. 201 ISSUE_RESULT 에만 있다',
    `grade`                varchar(10)  NULL COMMENT '등급 조회에 실패해도 이벤트 기록은 중단하지 않으므로 NULL 이 가능하다',
    `http_status`          int          NULL COMMENT 'QUEUE_ADMITTED 에는 없다',
    `reason_code`          varchar(40)  NULL COMMENT '4xx · 5xx 에만 있다. 미매핑은 유실이 아니라 UNMAPPED 로 온다',
    `dependency`           varchar(10)  NOT NULL COMMENT 'NONE 이 명시적 enum 값이다. NULL 은 의미가 없다',
    `queue_position`       bigint       NULL COMMENT '202 ENTRY_RESULT 에만 있다',
    `queue_sequence`       bigint       NULL COMMENT '202 ENTRY_RESULT · QUEUE_ADMITTED 에만 있다',
    `replayed`             tinyint(1)   NOT NULL COMMENT '계약이 primitive boolean 이다. 3상태를 만들지 않는다',
    `occurred_at`          datetime(6)  NOT NULL COMMENT '프로듀서 시계. N대 시계 차이로 정렬은 근사다 — 보존 삭제 경계로 쓰지 않아 인덱스도 없다',
    `ingested_at`          datetime(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'Consumer 도착 시각. 도착 순서와 보존 삭제 경계의 원천. DEFAULT 는 컬럼 누락 시 1364 무한 재시도를 막는 안전망이다',
    `engine_version`       varchar(10)  NOT NULL,
    `release_stage`        varchar(10)  NOT NULL,
    `queue_mode`           varchar(10)  NOT NULL,
    `benchmark_run_id`     bigint       NULL COMMENT 'benchmark_runs.id (OBS-14b · V2026082002). 일반 운영 요청은 회차에 속하지 않아 NULL 이라 FK 를 걸 수 없다',
    `producer_instance_id` varchar(100) NOT NULL COMMENT '계약이 requireText(..., 100) 이다. 64 로 잡으면 strict mode 에서 1406 으로 죽는다',
    `topic`                varchar(120) NOT NULL COMMENT 'coupon.issue.attempt. 오프셋은 토픽 안에서만 유일하므로 uk_kafka 에 함께 들어간다',
    `kafka_partition`      int          NOT NULL,
    `kafka_offset`         bigint       NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_event` (`event_id`),
    UNIQUE KEY `uk_kafka` (`topic`, `kafka_partition`, `kafka_offset`),
    KEY `ix_run` (`benchmark_run_id`),
    KEY `ix_ingested` (`ingested_at`),
    CONSTRAINT `ck_attempt_schema_version` CHECK (`schema_version` > 0),
    CONSTRAINT `ck_attempt_issue_ids` CHECK (
        (`issuance_id` IS NULL) = (`issuance_code` IS NULL)
        AND (`issuance_id` IS NULL OR (`http_status` IS NOT NULL AND `http_status` = 201))
    ),
    CONSTRAINT `ck_attempt_reason` CHECK (
        (`http_status` IS NULL AND `reason_code` IS NULL)
        OR (`http_status` IS NOT NULL AND `http_status` >= 400 AND `reason_code` IS NOT NULL)
        OR (`http_status` IS NOT NULL AND `http_status` <  400 AND `reason_code` IS NULL)
    )
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- NOT NULL 5개는 설계 스케치(AB-0 §6)가 NULL 로 적어둔 것을 좁힌 것이다
--   dependency · engine_version · release_stage · queue_mode 는 IssuanceFlowEvent 의 compact constructor 가
--   requireNonNull 로, replayed 는 primitive boolean 으로 이미 보장한다. 역직렬화도 같은 생성자를 지나므로
--   Consumer 손에 들어온 이벤트 중 이 5개가 비어 있는 것은 존재할 수 없다 — 조여도 거부되는 정상 이벤트가 없다.
--   반대로 지금 NULL 로 두면 나중에 조일 때 이미 쌓인 NULL 행이 원래 무엇이었는지 알 방법이 없다.
--   푸는 건 ALTER MODIFY 한 번이고 조이는 건 불가능하므로, 되돌리기 쉬운 쪽을 지금 고른다.
--
-- CHECK 3종은 validateByEventType(IssuanceFlowEvent.java)의 규칙 중 DB 로 옮길 수 있는 것만 옮긴 것이다
--   Consumer 경로는 생성자가 막아 주지만 DLT 수동 보정 · 백필은 이 테이블에 직접 쓴다.
--   "409 인데 issuance_id 가 있는" 행 같은 불법 상태를 그 경로에서도 막는다.
--   eventType 별 세부 규칙(202 에만 queue_position 등)은 조건이 겹쳐 CHECK 로 옮기면 오탐이 나므로 남겨 두지 않았다.
--
--   비교마다 http_status IS NOT NULL 을 함께 쓴다. MySQL 의 CHECK 는 FALSE 일 때만 거부하고 NULL 이면 통과시키는데,
--   http_status 가 NULL 이면 http_status = 201 · >= 400 이 전부 NULL 로 평가돼 식 전체가 NULL 이 된다.
--   그러면 "http_status 가 없는데 issuance_id 가 있는" 행이 그대로 들어온다 — 막으려던 경로가 정확히 뚫린다.
--
--   TODO(OBS-15): 이 CHECK 3종의 회귀 테스트는 Consumer 테스트와 함께 만든다.
--   storage/src/testFixtures 의 MySqlContainerConfig 로 Flyway 적용 후 불법 INSERT 가 거부되는지 검증한다.

-- 인덱스는 지금 쓰이는 용도가 확실한 것만 만든다 (설계 스케치의 6개 중 셋을 뺐다)
--   이 테이블은 유실이 가장 심한 부하 최고 구간에서 가장 빨리 적재돼야 하는데, 인덱스는 그 구간의 비용이다.
--   300,000 행 적재 실측: 인덱스 7개 2,641ms · 108MB → 5개 1,960ms · 83MB (적재 26% 단축, 용량 23% 감소).
--
--   ix_member (member_id, occurred_at) · ix_coupon (coupon_id, occurred_at) 을 만들지 않는다
--     회원별 · 캠페인별 조회를 예상한 인덱스지만 그 조회가 아직 없다. 이 테이블을 읽는 코드 자체가 없다.
--
--   ix_time (occurred_at) 을 만들지 않는다
--     원래 보존 삭제 경계 탐색용이었는데 그 역할이 ix_ingested 로 넘어가면서 용도가 사라졌다.
--     시간축 조회의 실사용처도 없다 — OBS-15 의 이벤트 목록은 Redis Stream 에서 나오고(커서가 Stream ID 다),
--     패널 12 · 13 · 16 은 Prometheus 카운터이며, 회차 시계열은 OBS-22 의 run_timeseries 가 따로 갖는다.
--     회차 단위 조회가 생기더라도 ix_run 으로 좁힌 뒤가 10만 행 수준이라 별도 시간 인덱스는 안 탄다.
--
--   OBS-15 의 조회가 확정되면 그때 실제 쿼리로 재측정해 필요한 것만 추가한다.
--   ⚠️ 행이 쌓인 뒤의 ALTER TABLE ADD INDEX 는 적재를 멈추므로, 추가한다면 조회 구현과 함께 이른 시점에 한다.

-- uk_kafka 에 topic 을 넣는 이유
--   Kafka 의 offset 은 토픽 안에서 파티션별로 0 부터 증가한다. 진짜 식별자는 (topic, partition, offset) 이다.
--   토픽을 재생성하면 offset 이 0 부터 다시 시작하므로, topic 이 없으면 재생성 이후 들어오는
--   event_id 도 내용도 전혀 다른 정상 이벤트가 옛 행과 (partition, offset) 이 겹쳐 전량 거부된다.
--   Consumer 는 중복을 무시하고 넘어가야 하므로(리밸런싱 후 재소비가 정상 경로다) 그 거부는 조용하고,
--   대시보드에는 "attempt 0건" 이 그려진다. 유실이 아니라 전량 소실인데 아무도 모르는 형태다.
--
--   컬럼이 26개가 되지만 이벤트 계약은 바뀌지 않는다 — IssuanceFlowEvent 의 레코드 컴포넌트는 21개이고,
--   id · ingested_at · kafka_partition · kafka_offset 은 Consumer 가 붙이는 메타데이터다.
--   topic 은 정확히 같은 부류이고, 그 Kafka 좌표를 완성하는 나머지 한 조각이다.
--
--   길이 120 은 현재 토픽명(coupon.issue.attempt)에 충분한 여유다. Kafka 자체 상한은 249 자이므로
--   그보다 긴 이름을 쓸 일이 생기면 이 컬럼을 함께 넓힌다.
