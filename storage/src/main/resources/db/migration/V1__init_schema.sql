CREATE TABLE `grades` (
                          `code` varchar(10) PRIMARY KEY COMMENT 'WELCOME / SILVER / GOLD / VIP',
                          `bit_value` tinyint NOT NULL COMMENT '1 / 2 / 4 / 8 — 비트마스크 자리'
);

CREATE TABLE `members` (
                           `id` bigint PRIMARY KEY AUTO_INCREMENT,
                           `membership_grade` varchar(10) NOT NULL,
                           `name_enc` varbinary(256) COMMENT 'AES-256-GCM',
                           `email_enc` varbinary(256),
                           `email_hash` char(64) UNIQUE COMMENT 'HMAC-SHA256 블라인드 인덱스 — 키 없는 SHA-256 금지',
                           `phone_enc` varbinary(256),
                           `phone_hash` char(64) COMMENT 'HMAC-SHA256. 전화번호는 중복 허용이라 UNIQUE 없음',
                           `created_at` datetime(6) NOT NULL
);

CREATE TABLE `brands` (
                          `id` bigint PRIMARY KEY AUTO_INCREMENT,
                          `name` varchar(50) NOT NULL,
                          `category` varchar(20) NOT NULL COMMENT '카페 / 영화 / 외식 — 통계 그룹핑용'
);

CREATE TABLE `coupon_templates` (
                                    `id` bigint PRIMARY KEY AUTO_INCREMENT,
                                    `brand_id` bigint NOT NULL,
                                    `name` varchar(100) NOT NULL COMMENT '"신학기 요금제 할인"',
                                    `policy_type` varchar(20) NOT NULL COMMENT 'PERCENT_CAPPED / FIXED_AMOUNT / DATA_GRANT',
                                    `discount_rate` int COMMENT '정률 전용. 20 = 20%',
                                    `max_discount_amount` int COMMENT '정률 상한. 20000',
                                    `discount_amount` int COMMENT '정액 전용. 5000',
                                    `data_grant_mb` int COMMENT '데이터 전용. 1024',
                                    `min_order_amount` int,
                                    `valid_days` int NOT NULL COMMENT '발급 시점부터 며칠 — 회차 마감과 무관',
                                    `nth_week` tinyint COMMENT '1~4. 매월 N번째 주',
                                    `day_of_week` varchar(3) COMMENT 'MON ~ SUN',
                                    `start_time` time,
                                    `duration_hours` int,
                                    `stock_per_occurrence` int NOT NULL COMMENT '회차마다 몇 장을 열 것인가',
                                    `eligible_grades_mask` tinyint NOT NULL COMMENT 'VIP+GOLD = 8 or 4 = 12',
                                    `active` boolean NOT NULL DEFAULT true COMMENT 'false 면 스케줄러가 건너뛴다'
);

CREATE TABLE `coupons` (
                           `id` bigint PRIMARY KEY AUTO_INCREMENT,
                           `template_id` bigint NOT NULL,
                           `brand_id` bigint NOT NULL COMMENT '역정규화 — 목록 조회에서 조인 1단 제거',
                           `name` varchar(100) NOT NULL COMMENT '스냅샷 [1] — 템플릿명 변경이 과거 회차에 소급되면 안 됨',
                           `policy_type` varchar(20) NOT NULL COMMENT '스냅샷 [1]',
                           `discount_rate` int,
                           `max_discount_amount` int,
                           `discount_amount` int,
                           `data_grant_mb` int,
                           `min_order_amount` int COMMENT '스냅샷 [1] — 할인 계산에 들어가므로 반드시 복사',
                           `valid_days` int NOT NULL,
                           `eligible_grades_mask` tinyint NOT NULL COMMENT '스냅샷 [1]',
                           `open_at` datetime NOT NULL,
                           `close_at` datetime NOT NULL,
                           `status` varchar(20) NOT NULL COMMENT 'SCHEDULED / OPEN / CLOSED',
                           `created_at` datetime(6) NOT NULL
);

CREATE TABLE `coupon_stocks` (
                                 `coupon_id` bigint PRIMARY KEY,
                                 `total_quantity` int NOT NULL,
                                 `active_count` int NOT NULL DEFAULT 0 COMMENT 'ISSUED + USED 합계',
                                 `updated_at` datetime NOT NULL
);

CREATE TABLE `issuances` (
                             `id` bigint PRIMARY KEY AUTO_INCREMENT,
                             `coupon_id` bigint NOT NULL,
                             `member_id` bigint NOT NULL,
                             `code` char(16) UNIQUE NOT NULL COMMENT '사용자에게 보이는 쿠폰 코드',
                             `issued_grade` varchar(10) NOT NULL COMMENT '발급 시점 등급 스냅샷',
                             `status` varchar(12) NOT NULL COMMENT 'ISSUED / USED / CANCELLED / EXPIRED',
                             `issued_at` datetime(6) NOT NULL,
                             `expires_at` datetime(6) NOT NULL COMMENT 'issued_at + campaigns.valid_days. 만료 판정의 유일한 기준',
                             `updated_at` datetime(6) NOT NULL COMMENT '마지막 상태 변경 시각 — 데이터 지문 입력'
);

CREATE TABLE `issuance_histories` (
                                      `id` bigint PRIMARY KEY AUTO_INCREMENT,
                                      `issuance_id` bigint NOT NULL,
                                      `event_type` varchar(30) NOT NULL,
                                      `from_status` varchar(20),
                                      `to_status` varchar(20) NOT NULL,
                                      `reason` varchar(120),
                                      `request_id` varchar(36) COMMENT 'F4 — idempotency_records.idem_key 와 연결',
                                      `created_at` datetime(6) NOT NULL
);

CREATE TABLE `issuance_usages` (
                                   `id` bigint PRIMARY KEY AUTO_INCREMENT,
                                   `issuance_id` bigint NOT NULL,
                                   `order_id` bigint COMMENT '참조값만 — 주문 도메인은 범위 밖',
                                   `discount_amount` int NOT NULL COMMENT '실제로 깎인 금액',
                                   `used_at` datetime NOT NULL,
                                   `canceled_at` datetime
);

CREATE TABLE `idempotency_records` (
                                       `idem_key` varchar(36) PRIMARY KEY COMMENT '클라이언트가 만든 UUID v4',
                                       `member_id` bigint NOT NULL,
                                       `issuance_id` bigint NOT NULL COMMENT 'F4 — 어느 쿠폰에 대한 요청이었나',
                                       `request_hash` char(64) NOT NULL COMMENT '같은 키에 다른 본문이면 422',
                                       `status` varchar(12) NOT NULL COMMENT 'IN_PROGRESS / DONE — 없으면 빈 응답 반환 버그',
                                       `response_body` text COMMENT '최초 응답을 그대로 보관했다가 재요청에 반환',
                                       `created_at` datetime(6) NOT NULL
);

CREATE TABLE `verification_runs` (
                                     `id` bigint PRIMARY KEY AUTO_INCREMENT,
                                     `as_of` datetime(6) NOT NULL COMMENT '검증 기준 시각 — 같은 값이면 같은 결과',
                                     `from_ts` datetime(6) COMMENT 'INCREMENTAL 전용. 절대 구간 (from_ts, as_of]',
                                     `scope` varchar(12) NOT NULL COMMENT 'FULL / INCREMENTAL — 합격 판정은 FULL 에서만',
                                     `dataset` varchar(10) NOT NULL COMMENT 'CLEAN / CORRUPT',
                                     `attempt` int NOT NULL DEFAULT 1 COMMENT 'Spring Batch JobParameters 식별자 — 재실행에 필수',
                                     `verdict` varchar(8) COMMENT 'PASS / FAIL — D10 게이트가 읽는다',
                                     `stats_status` varchar(10) COMMENT 'COMPLETE / PARTIAL / SKIPPED — 대시보드가 읽는다',
                                     `finding_count` int NOT NULL DEFAULT 0,
                                     `findings_checksum` char(64) COMMENT 'SHA-256 of 정렬된 (finding_type, target_key)',
                                     `dataset_fingerprint` char(64) COMMENT '두 run 이 같은 데이터를 봤는지',
                                     `started_at` datetime(6) NOT NULL,
                                     `finished_at` datetime(6)
);

CREATE TABLE `verification_findings` (
                                         `id` bigint PRIMARY KEY AUTO_INCREMENT,
                                         `run_id` bigint NOT NULL,
                                         `finding_type` varchar(40) NOT NULL COMMENT 'V1~V6 상수',
                                         `target_key` varchar(64) NOT NULL COMMENT '정규화 키 — 각주 [8]',
                                         `campaign_id` bigint COMMENT 'V1',
                                         `member_id` bigint COMMENT 'V2',
                                         `coupon_id` bigint COMMENT 'V3 · V5 · V6',
                                         `history_id` bigint COMMENT 'V4',
                                         `expected` varchar(200) NOT NULL COMMENT '"active_count=9998"',
                                         `actual` varchar(200) NOT NULL COMMENT '"coupons 집계=10001"'
);

CREATE TABLE `expected_findings` (
                                     `id` bigint PRIMARY KEY AUTO_INCREMENT,
                                     `seed_run_id` bigint NOT NULL COMMENT '어느 주입 실행이 만든 정답인가',
                                     `corrupt_type` tinyint NOT NULL COMMENT '오염 유형 1~7',
                                     `finding_type` varchar(40) NOT NULL COMMENT 'V1~V6 상수 — findings 와 동일 어휘',
                                     `target_key` varchar(64) NOT NULL COMMENT 'findings 와 동일 형식이어야 조인이 성립',
                                     `campaign_id` bigint,
                                     `member_id` bigint,
                                     `coupon_id` bigint,
                                     `history_id` bigint,
                                     `note` varchar(200) COMMENT '"CANCEL_USE 를 2번 심었음"',
                                     `created_at` datetime(6) NOT NULL
);

CREATE TABLE `asof_state` (
                              `run_id` bigint NOT NULL,
                              `coupon_id` bigint NOT NULL,
                              `state` varchar(12) NOT NULL COMMENT 'asOf 시점의 재구성 상태',
                              `last_history_id` bigint COMMENT 'asOf 이하 마지막 이력',
                              `last_event_at` datetime(6),
                              `active_usage_count` int NOT NULL DEFAULT 0 COMMENT 'asOf 기준 활성 사용 행 수',
                              PRIMARY KEY (`run_id`, `coupon_id`)
);

CREATE TABLE `coupon_stats` (
                                `run_id` bigint NOT NULL,
                                `coupon_id` bigint NOT NULL,
                                `issued_total` int NOT NULL DEFAULT 0 COMMENT '누적 발급 — 퍼널의 분모',
                                `issued` int NOT NULL DEFAULT 0 COMMENT '현재 ISSUED',
                                `used` int NOT NULL DEFAULT 0 COMMENT '현재 USED',
                                `cancelled` int NOT NULL DEFAULT 0 COMMENT '현재 CANCELLED',
                                `expired` int NOT NULL DEFAULT 0 COMMENT '현재 EXPIRED',
                                `sold_out_seconds` int COMMENT '완판 회차만. 미달은 NULL',
                                PRIMARY KEY (`run_id`, `coupon_id`)
);

CREATE TABLE `grade_stats` (
                               `run_id` bigint NOT NULL,
                               `coupon_id` bigint NOT NULL,
                               `grade` varchar(10) NOT NULL,
                               `issued_total` int NOT NULL DEFAULT 0,
                               `used_total` int NOT NULL DEFAULT 0,
                               PRIMARY KEY (`run_id`, `coupon_id`, `grade`)
);

CREATE TABLE `hourly_stats` (
                                `run_id` bigint NOT NULL,
                                `day_of_week` varchar(3) NOT NULL COMMENT 'MON ~ SUN',
                                `hour` tinyint NOT NULL COMMENT '0-23',
                                `issued_total` int NOT NULL DEFAULT 0,
                                PRIMARY KEY (`run_id`, `day_of_week`, `hour`)
);


CREATE UNIQUE INDEX `uk_template_open` ON `coupons` (`template_id`, `open_at`);

CREATE UNIQUE INDEX `uk_coupon_member` ON `issuances` (`coupon_id`, `member_id`);

CREATE UNIQUE INDEX `uk_run_params` ON `verification_runs` (`as_of`, `dataset`, `scope`, `attempt`);

CREATE UNIQUE INDEX `uk_run_finding` ON `verification_findings` (`run_id`, `finding_type`, `target_key`);

CREATE UNIQUE INDEX `uk_expected` ON `expected_findings` (`seed_run_id`, `finding_type`, `target_key`);

ALTER TABLE `grades` COMMENT = '**왜 있나** — eligible_grades_mask 값 12 가 VIP+GOLD 라는 걸 사람이 읽게 하는
단일 출처입니다. 없으면 비트 자릿값이 코드 곳곳에 매직넘버로 흩어집니다.

**언제 쓰나** — 앱 부팅 시 4행 전량을 메모리에 올리고 끝입니다.
요청마다 조회하지 않습니다.

**누가 쓰나** — 등급 자격 검증(EnumSet 변환), 관리자 화면의 등급 선택 UI,
그리고 검증 배치 V6 의 (mask & bit_value) = 0.
';

ALTER TABLE `members` COMMENT = '**왜 있나** — 회원 100만. 평문 저장 금지라 암호문과 해시를 쌍으로 둡니다.
암호문은 검색이 불가능하므로 조회용 해시가 따로 필요합니다.

**HMAC 이어야 합니다** — 키 없는 SHA-256 은 이메일처럼 엔트로피가 낮은 값에
사전 공격이 통해서 블라인드 인덱스의 의미가 사라집니다. PRD:356 이 명시적으로
HMAC-SHA256 입니다.

**언제 쓰나** — 더미데이터 적재, 등급 분포 확인.
**발급 경로에서는 조회하지 않습니다** — 각주 [3].
**검증 리포트·통계 쿼리에서도 조인하지 않습니다** — 10-batch-design.md 3절·4절④.
등급이 필요한 곳은 coupons.issued_grade 스냅샷을 씁니다.

**적재 주의** — 무인덱스 JDBC batch 는 @Convert AttributeConverter 를 우회합니다.
시드가 직접 AES-256-GCM + HMAC 을 100만 행에 계산해야 하고, uk_email_hash 를
나중에 거는 이상 **해시 충돌 0 을 시드가 보장**해야 합니다. 충돌하면
ADD CONSTRAINT 가 통째로 실패합니다. (10-batch-design.md 4절⑨)
';

ALTER TABLE `brands` COMMENT = '**왜 있나** — 제휴 브랜드 12개. 카테고리별 분석의 기준축입니다.

**누가 쓰나** — 목록 API, 대시보드 화면 3(분석·비교) 패널 24.
';

ALTER TABLE `coupon_templates` COMMENT = '**왜 있나** — 브랜드 데이는 매월 반복됩니다. 반복 규칙이 없으면 운영자가
147개 회차를 손으로 만들어야 합니다. 이 12행이 그걸 대신합니다.

**언제 쓰나** — **매일 새벽 스케줄러가 전체를 스캔**해서 다음 회차를
생성할 때가 됐는지 판단합니다. 사용자 요청 경로에는 등장하지 않습니다.

**누가 쓰나** — 브랜드 데이 스케줄러(배치), 관리자 CRUD 화면.

**주의 1** — 여기 정책을 바꿔도 이미 생성된 campaigns 는 변하지 않습니다. 각주 [1].

**주의 2 — 검증하지 않습니다** (F6)
stock_per_occurrence ↔ coupon_stocks.total_quantity **불일치는 정상입니다.**
더미데이터의 과거 회차 144개는 재고를 18,000~34,000 으로 의도적으로 흩뿌립니다.
이걸 규칙으로 추가하면 **정상셋 0건이 원천적으로 불가능해집니다.**
Seed 의 "검증하지 않는 것" 섹션에 반드시 명시할 것.
';

ALTER TABLE `coupons` COMMENT = '**왜 있나** — 사람들이 "이번 달 모카빈 쿠폰"이라 부르는 그것입니다.
오픈 시각, 마감 시각, 그리고 그 시점의 정책 스냅샷을 갖습니다. 147행.

**언제 쓰나** — 스케줄러가 SCHEDULED 로 생성 → open_at 도달 시 OPEN →
재고 소진 또는 close_at 도달 시 CLOSED. 발급 요청은 OPEN 일 때만 통과합니다.

**누가 쓰나** — 목록 API, 발급 API, 그리고 **시스템 경계를 넘는 식별자**로
쓰입니다. Redis 키(stock:{campaignId} / queue:{campaignId}), Kafka 파티션 키,
대시보드 필터, 검증 리포트가 전부 campaign_id 를 기준으로 돕니다.

**close_at 을 갱신하지 않습니다** (F5) — 재고 소진으로 닫혀도 예정값 그대로 둡니다.
갱신하면 "언제 닫힐 예정이었나"가 소실됩니다. 실제 소진 시각은 이력에서 계산합니다.

    sold_out_seconds = (해당 회차의 마지막 ISSUE 이력 created_at) − open_at
                       단, 완판된 회차만. 미달 회차는 NULL

**인덱스는 uk_template_open 하나뿐입니다** — 147행이라 status·open_at 단독
인덱스는 풀스캔보다 느립니다. (02-erd-decisions.md 인덱스 절)

**total_quantity 를 여기 두지 않습니다** — coupon_stocks 가 유일 출처입니다. 각주 [2].
';

ALTER TABLE `coupon_stocks` COMMENT = '**왜 별도 테이블인가** — 재고 차감은 전 요청이 같은 행을 잠그는 지점입니다.
이걸 campaigns 안에 두면 재고 락이 **회차 목록 조회까지 막습니다.**
떼어내서 경합을 이 한 행에 가둡니다. 각주 [2].

**불변식** — 잔여 = total_quantity - active_count.
active_count 는 취소분과 만료분을 제외한 살아 있는 발급 수입니다.
누적이 아니므로 취소가 30만 건 나도 재고가 줄지 않습니다.

**issued_count 라는 이름을 쓰지 마세요** — 누적으로 읽힙니다. 이름을 잘못 잡으면
구현자가 십중팔구 누적으로 짜고 초과 발급 판정이 통째로 어긋납니다.

**물리 제약을 겁니다** (CLEAN 스키마)

    CHECK (active_count >= 0 AND active_count <= total_quantity)

PRD 설계 원칙 1번이 *"불변식은 애플리케이션이 아니라 DB 제약으로 표현한다.
로직에 버그가 있어도 막혀야 한다"* 입니다. 1인 1매에 uk_campaign_member 를 둔
논리를 초과 발급에도 그대로 적용하는 것뿐입니다. v1/v2/v3 어느 버전에 버그가
있어도 초과 차감이 DB 에서 튕깁니다.

**언제 쓰나** — v1 은 SELECT FOR UPDATE 로 여기서 직접 판정하고,
v2/v3 는 Redis 에서 판정한 뒤 이 행에 사후 영속화합니다.

**누가 쓰나** — 발급 API, 드리프트 감시(1초 주기로 Redis 카운터와 대조),
검증 배치 V1(재고 정합).
';

ALTER TABLE `issuances` COMMENT = '**왜 있나** — 누가 무엇을 언제 받았나. 이 프로젝트의 판정이 전부 여기서
나옵니다. 300만 행.

**초과 발급 판정** — COUNT(status IN (ISSUED, USED)) 를 total_quantity 와
비교합니다. **누적으로 세지 않습니다.** 취소분까지 더하면 정상 운영에서도
대량 오탐이 납니다 — 더미데이터에 CANCELLED 가 10%(30만 건) 있습니다.

**1인 1매** — uk_campaign_member 가 물리적으로 막습니다. 애플리케이션 검사는
빠른 경로일 뿐이고 최후 방어선은 이 제약입니다. 취소 후 재발급도 여기서
막힙니다(이력 기준이라 상태와 무관).

**issued_grade 스냅샷** — members.membership_grade 는 **현재값**입니다.
등급 자격은 **발급 시점** 기준이어야 하므로 그대로 조인하면 회원이 강등되는
순간 정상 발급이 위반으로 잡힙니다. PRD:1677 이 discount_amount 를
*"스냅샷은 시점 고정이라 애초에 변하지 않으므로 검증 대상이 아니다"* 로
처리한 것과 같은 논리입니다. 컬럼 하나로 셋이 풀립니다.

    V6 등급 자격 위반이 members 조인 없이 결정론적으로 판정됨
    grade_stats 가 coupons 단일 스캔에서 나옴 (100만 행 조인 제거)
    대시보드 패널 ⑨ 이벤트 스트림이 VIP/WELCOME 을 조인 없이 표시

**members 조인 금지 규칙과의 충돌을 이 컬럼이 해소합니다** —
10-batch-design.md 3절·4절④ 가 리포트·통계 쿼리의 members 조인을 금지했는데
같은 문서 4절② 의 V6 정의는 members 조인을 요구합니다. 스냅샷이 답입니다.

**updated_at** (F7) — 개발 중 증분 검증용이자 dataset_fingerprint 의 5개 입력 중
하나입니다. 최종 판정은 반드시 FULL 전수로 돕니다.

**member_id 단독 인덱스를 두지 않습니다** — "내 쿠폰 목록" API 가 없고 대시보드도
캠페인 단위입니다. uk_campaign_member 가 1인 다매 검출 쿼리를 그대로 커버합니다.

**언제 쓰나** — 발급 성공 시 INSERT. 사용/취소/만료 시 status UPDATE.
v3 는 Kafka 컨슈머가 배치로 INSERT 합니다.
';

ALTER TABLE `issuance_histories` COMMENT = '**왜 있나** — coupons.status 는 **현재값만** 갖습니다. 언제 무엇을 거쳐
여기까지 왔는지는 알 수 없고, 되감기도 불가능합니다. 약 520만 행.

**정합성 검증의 핵심** — 이력을 시간순으로 접은 결과가 현재 status 와
일치하는지 대조합니다. 어긋나면 상태 전이 로직에 버그가 있다는 뜻입니다. 각주 [4].

**리플레이 정렬은 (created_at, id) 로 고정합니다** — 같은 시각 이력 2건의
순서가 안 정해지면 리플레이 결과가 실행마다 달라져 findings_checksum 이
흔들립니다. idx_history_coupon 은 InnoDB 에서 PK 가 뒤에 붙어 실질
(coupon_id, created_at, id) 라 이 정렬을 그대로 커버합니다.

**request_id** (F4) — 리플레이 검증 자체는 from_status/to_status 연쇄로 돌아가지만,
불일치를 찾은 **다음**에 "어느 요청이 그랬나"를 못 씁니다. 개발 중 디버깅에서
이 차이가 큽니다.

**언제 쓰나** — **모든 상태 전이마다** INSERT. 역방향 전이(USED → ISSUED,
주문 취소)도 빠짐없이 남깁니다. 갱신하거나 삭제하지 않습니다.

**누가 쓰나** — 상태 변경 서비스(쓰기), 검증 배치 Step 0·V4(읽기),
대시보드 상태 전이 스트림(화면 1 패널 ⑩).
';

ALTER TABLE `issuance_usages` COMMENT = '**왜 histories 와 따로 있나** — histories 는 "무엇에서 무엇으로 바뀌었나"라는
전이 사실이고, 여기는 "얼마를 깎았고 어느 주문이었나"라는 **정산 성격**의
데이터입니다. 성격이 달라 섞으면 둘 다 지저분해집니다.

**쿠폰당 여러 행이 정상입니다** — 더미데이터의 "USED 중 20%가 사용 → 사용취소
→ 재사용" 이력이 여기 표현됩니다. 현재 유효한 사용은 canceled_at IS NULL 인
행이고 **쿠폰당 최대 1개**여야 합니다.

**검증 대상입니다** (F3 · V5) — PRD 본문이 "이것도 검증 대상"이라 써놓고
규칙 목록에도 오염 유형에도 없었습니다. 세 축이 서로 맞아야 합니다.

    coupons.status = USED
      ↔ coupon_histories 리플레이 결과 = USED
      ↔ coupon_usages 에 활성 행이 정확히 1개

asOf 기준 활성 판정은 used_at <= asOf AND (canceled_at IS NULL OR canceled_at > asOf).
used_at·canceled_at 이 둘 다 시각이라 V5 는 완전 결정론입니다.

**누가 쓰나** — 사용/취소 API, 할인 금액 집계, 검증 배치 V5.
discount_amount 가 발급 시점 정책과 맞는지가 스냅샷[1]이 필요한 이유입니다.
';

ALTER TABLE `idempotency_records` COMMENT = '**왜 있나** — 네트워크 타임아웃이 나면 클라이언트는 성공인지 실패인지 모른 채
재시도합니다. 그때 쿠폰 사용이 두 번 처리되면 안 됩니다.

**언제 쓰나** — **상태 변경(use · cancel-use · cancel) API 에만** 겁니다.
발급은 uk_campaign_member 가 대신하므로 멱등 키를 받지 않습니다. 각주 [5].

**🔴 status 가 없으면 동시 요청에 빈 본문을 돌려줍니다**

    요청 A  INSERT(IN_PROGRESS) → 비즈니스 로직 처리 → UPDATE(response_body, DONE)
    요청 B                ↑ 이 구간에 PK 충돌로 진입
                          response_body 는 아직 NULL

PRD 멱등성 표가 *"나머지는 **완료 대기 후** 저장된 응답 반환"* 이라고 쓴 이유가
이것입니다. status 없이 PK INSERT 만으로는 "처리 중"과 "완료"를 구분할 수 없어
동시성 시나리오 4번(같은 멱등키로 동시 10회 → 1회 반영, 나머지 동일 응답)이
통과하지 못합니다.

**흐름**

    1. PK 로 INSERT (status = IN_PROGRESS) 시도
    2. 성공 → 최초 요청. 처리 후 response_body 채우고 DONE 으로 UPDATE
    3. 충돌 + DONE        → 저장된 response_body 를 200 으로 반환
    4. 충돌 + IN_PROGRESS → 짧게 재시도. 지속되면 409 (처리 중)
    5. 충돌 + 다른 request_hash → 422 IDEMPOTENCY_KEY_REUSED

**정리** — 24시간 지난 행은 배치로 삭제합니다. created_at 인덱스가 없으면
이 배치가 풀스캔합니다. (잡 목록 9번 — 컷 2순위)
';

ALTER TABLE `verification_runs` COMMENT = '**왜 있나** — 300만 건 정합성 검증을 **재실행 가능**하게 만듭니다.
as_of 를 고정하면 몇 번을 돌려도 같은 결과가 나옵니다. 각주 [7].

**attempt 가 없으면 재실행이 아예 안 됩니다** — Spring Batch 는 동일
JobParameters 재실행을 차단합니다. 결정론 검증은 같은 asOf 로 두 번 돌려야
하므로 attempt 를 식별 파라미터에 포함합니다.

    .addString("asOf", asOf, true).addString("dataset", dataset, true)
    .addString("scope", scope, true).addLong("attempt", n, true)

**🔴 세 컬럼이 서로 다른 것을 증명합니다. 하나도 뺄 수 없습니다.**

    findings_checksum    재실행 결정론 (run1 == run2)
    dataset_fingerprint  두 run 이 같은 데이터를 봤는가
    expected_findings    검출이 정확한가 (D10 게이트)  ← 별도 테이블

finding_count 만으로는 아무것도 증명 못 합니다. 다른 쿠폰이 걸려도 개수는
같을 수 있고, **오탐 350 + 미검출 350 도 count 는 700** 입니다.

**dataset_fingerprint 공식**

    SHA256(
      max(coupon_histories.id)  WHERE created_at <= asOf,
      count(coupon_histories)   WHERE created_at <= asOf,
      count(coupons),
      sum(coupon_stocks.active_count),
      max(coupons.updated_at)
    )

    지문 같음 + checksum 같음  →  결정론 증명
    지문 같음 + checksum 다름  →  🔴 검증기 버그. 이게 진짜 잡고 싶던 것
    지문 다름                  →  데이터가 바뀜. 비교 대상이 아니고, 그걸 안다

지문이 없으면 세 번째가 두 번째로 오인됩니다 — 만료 배치가 한 건 돌았을 뿐인데
"검증이 비결정적"으로 보고됩니다.

**findings_checksum 계산 규칙** — 정렬된 (finding_type, target_key) 만 해싱합니다.
expected/actual 같은 자유 문자열을 섞으면 포맷 한 글자에 거짓 실패가 납니다.

**verdict 와 stats_status 를 나눕니다** — 통계 Step 이 죽어도 검증 판정은
앞 Step 에서 이미 확정돼 있으므로 *"검증은 됐고 마지막이 죽었다"* 가 표현됩니다.
*"오염 700건을 정확히 검출했는데 시간 때문에 FAILED"* 라는 읽을 수 없는 상태를
만들지 않기 위해서입니다.

**CLEAN / CORRUPT 는 물리적으로 분리된 스키마입니다.**
이 테이블은 **각 스키마에 하나씩** 존재하고 run_id 네임스페이스도 갈립니다.
리포트에서 두 쪽 run 을 나란히 놓을 때 dataset 을 반드시 같이 표기하세요.

**시간 상한이 없습니다** — FULL 에 "몇 분 안에 끝나야 합격"이라는 캡을 두지
않습니다. 시간은 판정이 아니라 관측치입니다(L1 은 정합성 전용).
대신 쿼리 단위 상한(MAX_EXECUTION_TIME)은 둡니다 — 없으면 폭주 시 커넥션을
물고 늘어져 공유 환경 전체가 막힙니다.
';

ALTER TABLE `verification_findings` COMMENT = '**왜 있나** — 검증이 "통과/실패"만 알려 주면 고칠 수가 없습니다.
무엇이 어떻게 틀렸는지를 행 단위로 남깁니다.

**finding_type 은 검증 규칙 6종입니다 — 오염 유형과 1:1 이 아닙니다**

    V1  STOCK_MISMATCH       캠페인      집계    재고 정합
    V2  DUP_PER_MEMBER       캠페인·회원  집계    1인 1매 위반
    V3  REPLAY_MISMATCH      쿠폰        행순회  이력 리플레이 불일치
    V4  ILLEGAL_TRANSITION   이력 행      행순회  불법 전이
    V5  USAGE_MISMATCH       쿠폰        행순회  사용 실적 정합
    V6  GRADE_VIOLATION      쿠폰        조인    등급 자격 위반

오염 유형은 *주입하는 데이터의 종류*이고 검증 규칙은 *잡아내는 로직의 종류*입니다.
하나의 규칙이 여러 오염 유형을 잡습니다 — V1 하나가 오염 유형 1(재고는 줄었는데
ISSUE 이력 없음)과 3(CANCEL_USE 이중 복원)을 모두 잡습니다.

**"만료 누락"은 finding 이 아닙니다.** expires_at < asOf 인데 status = ISSUED 인
쿠폰은 이력 재구성에서 asof_state 도 ISSUED 라 자동으로 일치합니다. 만료 지연은
결함이 아니라 배치 주기의 함수이므로 **별도 관측 지표**로 둡니다.
finding_type 에 넣으면 정상셋에서 매번 검출되어 "0건"이 원천 불가능해집니다.

**"고아 이력"도 규칙에 없습니다.** V4 가 전이 연쇄로 잡습니다.

**🔴 target_key 가 없으면 판정이 오작동합니다** — 각주 [8]

    CAMPAIGN:812                 V1
    CAMPAIGN:812|MEMBER:9931     V2
    COUPON:44210                 V3 · V5 · V6
    HISTORY:88131                V4

개별 FK 컬럼은 조회 편의로 남기되 **UNIQUE · 집합 비교 · checksum 은 전부
target_key 로만** 돕니다. MySQL 은 NULL 을 UNIQUE 중복으로 보지 않고
NULL = NULL 이 UNKNOWN 이라, 다형 키 컬럼으로 직접 비교하면 정확히 검출한
finding 이 전부 "누락"으로 잡힙니다.

**expected / actual 은 NOT NULL 입니다** — 없으면 리포트가 "쿠폰 812934가
이상함"까지만 말합니다. 개발 중 제일 많이 보는 화면이라 근거가 있어야 합니다.

**FK 를 걸지 않습니다** — CORRUPT 스키마의 오염 행을 가리켜야 하고,
campaign_id / member_id / coupon_id / history_id 가 상호 배타적입니다.
';

ALTER TABLE `expected_findings` COMMENT = '**왜 있나 — 정답 매니페스트**

findings_checksum 은 run1 == run2 밖에 증명하지 못합니다. *"검출된 700건이
주입한 그 700건인가"* 는 다른 질문이고, **오탐 350 + 미검출 350 도 count 는
700** 입니다. 이 테이블이 없으면 D10 게이트가 총계밖에 볼 수 없습니다.

**오염을 심는 잡이 자기가 뭘 심었는지 알고 있으므로**, 심으면서 정답을 같이 씁니다.

**판정은 count 비교가 아니라 집합 비교입니다**

    누락 = expected_findings  MINUS  verification_findings   -- 못 잡은 것
    오탐 = verification_findings  MINUS  expected_findings   -- 잘못 잡은 것
    합격 = 누락 0건 AND 오탐 0건

조인 키는 (finding_type, target_key) 입니다. 다형 FK 컬럼으로 조인하면
NULL 비교가 UNKNOWN 이라 전부 어긋납니다. 각주 [8].

**CORRUPT 스키마에만 존재합니다.** CLEAN 셋의 기대값은 "0건"이라 테이블이
필요 없습니다.

**오염 7유형 700건**

    1  재고는 줄었는데 history 에 ISSUE 기록 없음      100  → V1
    2  history 는 USED 인데 coupons.status 는 ISSUED   100  → V3
    3  CANCEL_USE 가 2번 기록되어 재고 이중 복원        100  → V1 · V4
    4  종단 상태(EXPIRED)에서 USED 로 불법 전이         100  → V4
    5  동일 쿠폰이 두 유저에게 발급                     100  → V2
    6  동일 유저가 같은 캠페인에서 2건 발급             100  → V2
    7  status 는 ISSUED 인데 활성 usages 행이 남아 있음  100  → V5

**V6 는 오염 유형이 없습니다** — 정상셋 0건으로만 검증되고, 등급 위반 1건을
수동으로 심어 눈으로 확인하는 것으로 대체합니다. 여유가 생기면 유형 8을
추가해 800 으로 갑니다. 지금은 안 합니다.
';

ALTER TABLE `asof_state` COMMENT = '**왜 있나 — Step 0 의 산출물. 규칙 6개 중 4개가 이걸 읽습니다.**

    Step 0   이력 리플레이 → asof_state
               WHERE coupon_histories.created_at <= asOf
               ORDER BY (created_at, id)          ← 타이브레이커
    ────────── 여기부터 완전 결정론 ──────────
    Step 1   V4  불법 전이       이력만
    Step 2   V2  1인 1매         asof_state 집계
    Step 3   V5  사용 실적       asof_state ↔ coupon_usages
    ────────── 여기부터 현재 행을 읽음 ──────────
    Step 4   V3  리플레이 대조   asof_state ↔ coupons.status
    Step 5   V1  재고 정합       asof_state 집계 ↔ coupon_stocks.active_count
    Step 6   V6  등급 자격       coupons ⋈ campaigns ⋈ grades (issued_grade 스냅샷)
    Step 7   통계 집계 (CLEAN 만)

**추가 비용이 아닙니다** — V3 가 어차피 만드는 중간 산출물을 테이블로 내보내는
것뿐입니다. 다만 300만 행이라 메모리나 임시 테이블로는 안 됩니다.

**현재 행에 의존하는 V1·V3 를 마지막에 둡니다.** 앞의 결정론적 규칙이 먼저
끝나므로 폭주로 중단돼도 결정론적 부분은 이미 확보됩니다.

**정리** — run 종료 후 삭제하거나 최근 N개 run 만 남깁니다.
300만 x run 이라 방치하면 가장 빨리 커지는 테이블입니다.
';

ALTER TABLE `coupon_stats` COMMENT = '**왜 있나** — 대시보드가 1초마다 폴링하는데 300만 행을 매번 COUNT 할 수는
없습니다. 미리 세어 둡니다. 147행 x run.

**🔴 issued_total 과 issued 를 나눕니다** — 두 패널이 같은 컬럼을 다르게 씁니다.

    패널 ⑥  상태별 보유량        → 현재값 (issued/used/cancelled/expired)
    패널 26  상태 전이 퍼널       → 누적 (issued_total 이 분모)

하나로 두면 퍼널의 분모가 없거나 4개 합이 전체와 안 맞습니다. active_count 를
누적으로 짜면 초과 발급 판정이 어긋나는 것과 정확히 같은 함정입니다.

    불변식: issued + used = coupon_stocks.active_count
            issued + used + cancelled + expired = issued_total

**run_id 스냅샷** (10-batch-design.md 4절④) — 단일 트랜잭션은 undo log 가 크고
롤백하면 검증 결과와 통계 시점이 어긋납니다. STALE 플래그는 부분값이 실제로
남아 규율에 의존합니다. **run_id 는 틀린 값이 물리적으로 조회되지 않습니다.**

대시보드는 **status COMPLETE · dataset CLEAN 인 최신 run_id** 만 읽습니다.
이 필터를 **뷰로 한 번만 정의**하고 각 화면이 그 뷰를 씁니다 — 화면이 늘어날 때
필터를 빠뜨릴 수 없게.

**CORRUPT run 은 통계 Step 을 아예 실행하지 않습니다.** 그래도 영상 3:30–5:00
오염 구간에서 패널이 비지 않습니다 — 뷰가 직전 CLEAN 스냅샷을 그대로 보여주기
때문입니다. "오염 검출과 무관하게 마지막 정상 집계를 보여주는 중"이 됩니다.

저장 부담은 없습니다 — run 100번이면 14,700행.

**sold_out_seconds** (F5) — campaigns.close_at 을 갱신하지 않으므로
마지막 ISSUE 이력에서 계산합니다. 검증 배치가 어차피 이력을 전수 스캔하므로
같은 패스에서 공짜로 나옵니다.
';

ALTER TABLE `grade_stats` COMMENT = '**왜 있나** — "VIP 가 얼마나 받아 갔나"를 매번 GROUP BY 로 계산하면
300만 행을 스캔합니다. 등급 4종 x 회차 147 = 588행 x run 으로 대신합니다.

**coupons.issued_grade 로 집계합니다 — members 를 조인하지 않습니다.**
현재 등급으로 집계하면 등급이 바뀌는 순간 과거 회차의 분포가 소급 변경됩니다.
그리고 10-batch-design.md 가 리포트·통계 쿼리의 members 조인을 금지했습니다.

**누적 기준입니다** — 등급별 분포 차트는 "누가 얼마나 받아 갔나"를 묻습니다.
현재 보유량이 아닙니다. 이름에 _total 을 박아 혼동을 막습니다.

**누가 쓰나** — 대시보드 화면 3(분석·비교), 그리고 발표 자료.
등급 제한은 동시성 난이도를 올리지 않지만 도메인 현실성을 만듭니다.
그 근거를 숫자로 보여 주는 게 이 테이블입니다.
';

ALTER TABLE `hourly_stats` COMMENT = '**왜 있나** — 요일 x 시간 히트맵 전용입니다. 브랜드 데이를 특정 요일에
몰아 놓았을 때 그 요일이 실제로 도드라지는지 확인합니다. 168행(7 x 24) x run.

**run_id 가 PK 에 있어야 하는 이유가 여기서 제일 명확합니다** — 전체 기간
누적이라, 덮어쓰기인지 누적인지 정하지 않으면 **배치를 돌릴 때마다 히트맵
숫자가 커집니다.** as_of 결정론을 그렇게 강조한 프로젝트에서 집계만
비결정적이면 앞뒤가 안 맞습니다.

**캠페인 FK 가 없습니다** — 전체 기간 누적이라 특정 회차에 종속되지 않는
유일한 집계 테이블입니다.

**누가 쓰나** — 대시보드 화면 3 패널 25.
';

ALTER TABLE `members` ADD FOREIGN KEY (`membership_grade`) REFERENCES `grades` (`code`);

ALTER TABLE `coupon_templates` ADD FOREIGN KEY (`brand_id`) REFERENCES `brands` (`id`);

ALTER TABLE `coupons` ADD FOREIGN KEY (`template_id`) REFERENCES `coupon_templates` (`id`);

ALTER TABLE `coupon_stocks` ADD FOREIGN KEY (`coupon_id`) REFERENCES `coupons` (`id`);

ALTER TABLE `issuances` ADD FOREIGN KEY (`coupon_id`) REFERENCES `coupons` (`id`);

ALTER TABLE `issuances` ADD FOREIGN KEY (`member_id`) REFERENCES `members` (`id`);

ALTER TABLE `issuances` ADD FOREIGN KEY (`issued_grade`) REFERENCES `grades` (`code`);

ALTER TABLE `issuance_histories` ADD FOREIGN KEY (`issuance_id`) REFERENCES `issuances` (`id`);

ALTER TABLE `issuance_usages` ADD FOREIGN KEY (`issuance_id`) REFERENCES `issuances` (`id`);

ALTER TABLE `idempotency_records` ADD FOREIGN KEY (`member_id`) REFERENCES `members` (`id`);

ALTER TABLE `idempotency_records` ADD FOREIGN KEY (`issuance_id`) REFERENCES `issuances` (`id`);

ALTER TABLE `verification_findings` ADD FOREIGN KEY (`run_id`) REFERENCES `verification_runs` (`id`);

ALTER TABLE `asof_state` ADD FOREIGN KEY (`run_id`) REFERENCES `verification_runs` (`id`);

ALTER TABLE `coupon_stats` ADD FOREIGN KEY (`run_id`) REFERENCES `verification_runs` (`id`);

ALTER TABLE `coupon_stats` ADD FOREIGN KEY (`coupon_id`) REFERENCES `coupons` (`id`);

ALTER TABLE `grade_stats` ADD FOREIGN KEY (`run_id`) REFERENCES `verification_runs` (`id`);

ALTER TABLE `grade_stats` ADD FOREIGN KEY (`coupon_id`) REFERENCES `coupons` (`id`);

ALTER TABLE `grade_stats` ADD FOREIGN KEY (`grade`) REFERENCES `grades` (`code`);

ALTER TABLE `hourly_stats` ADD FOREIGN KEY (`run_id`) REFERENCES `verification_runs` (`id`);
