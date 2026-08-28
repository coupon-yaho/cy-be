-- OBS-51 소유. 관리자 브랜드 분석(A-11)이 읽을 집계 원천이다.
--
-- 경계 — B 는 여기까지다. 이 표들을 읽어 AdminAnalyticsDataset 을 조립하는
-- AdminAnalyticsSource 구현은 A 가 만든다.
--
-- 단위는 **회차**다. coupons 한 행이 회차 하나이고(uk_template_open), 1회차 티켓과
-- 2회차 티켓은 coupon_id 자체가 다르다. 그래서 여기의 coupon_id 는 템플릿이 아니라
-- 회차를 가리킨다 — AdminAnalyticsDataset.CampaignRef 가 opensOn·closesOn 을 갖는 것과
-- 같은 단위다.
--
-- 날짜는 **KST 로 버킷팅해 저장한다.** AdminAnalyticsQuery 가 zoneId 를 들고 다니지만
-- 실제로 넘어오는 값은 AdminDashboardController:40 의 ANALYTICS_ZONE = Asia/Seoul 하나뿐이다.
-- ⚠️ 다른 시간대가 들어오기 시작하면 이 표들은 다시 버킷팅할 수 없다 — 그때는 저장 단위를
--    바꿔야 하고, 조용히 어긋나므로 zoneId 가 늘어나는 변경은 여기까지 같이 봐야 한다.

CREATE TABLE `analytics_runs` (
    `id` bigint PRIMARY KEY AUTO_INCREMENT,

    -- 집계 기준 시각. 배치 시작 시각을 쓰지 않고 파라미터로 받아 박는다 — 같은 as_of 로
    -- 다시 돌리면 issued_at 기준 두 축은 같은 값이 나와야 한다.
    -- 이 값은 회차가 **목표한** 지점이고, 실제로 도달한 지점은 축별 aggregated_through 다.
    `as_of` datetime(6) NOT NULL,
    `started_at` datetime(6) NOT NULL,
    `status` varchar(11) NOT NULL
        COMMENT 'IN_PROGRESS · SUCCEEDED · FAILED',
    `failure_reason` varchar(500) NULL
        COMMENT 'FAILED 재실행 판단 근거',

    -- 축마다 상태와 완료 시각을 따로 둔다. AdminAnalyticsDataset 이 세 축을 각각
    -- AggregateObservation 으로 들고 있어서 축 하나가 실패해도 나머지는 값을 내야 한다.
    -- 값은 AggregateAvailability 와 같은 이름을 쓴다 — 변환하지 않고 그대로 실린다.
    -- ⚠️ 완료 **시각**과 집계 **지점**은 다른 값이다.
    --    completed_at 은 "언제 돌았나" — 운영 진단용이고 화면 최신성 판정에는 안 쓴다,
    --    aggregated_through 는 "어디까지 셌나" — 이쪽이 AggregateObservation.observedAt 이다.
    --    한 회차가 밀린 구간을 여러 걸음에 나눠 따라잡으므로 둘이 갈린다 — 지금 막 돌았지만
    --    아직 3일 전까지밖에 못 세었을 수 있다. 한 컬럼으로 합치면 그 상태를 표현하지 못하고,
    --    따라잡는 중인 회차가 "최신" 으로 읽혀 stale-after 가 발동하지 않는다.
    --    [A 확정 2026-08-26] observedAt = aggregated_through · completed_at 은 판정에 미사용.
    --
    -- ⚠️ **축마다 수위선이 다를 수 있다.** 한 축이 실패하면 그 축만 뒤처진 채 나머지가 전진하므로,
    --    같은 기간을 조회해도 analytics_daily_issues.issue_count 합계와
    --    analytics_issuance_statuses.total_issued 합계가 서로 다를 수 있다. 버그가 아니라
    --    축별 관측 시점이 다른 것이고, 각 축의 aggregated_through 가 그 차이를 설명한다.
    --    두 값을 나란히 보여 주는 화면을 만든다면 이 점을 함께 봐야 한다.
    `monthly_trend_status` varchar(11) NOT NULL DEFAULT 'PENDING'
        COMMENT 'AVAILABLE · PENDING · UNAVAILABLE',
    `monthly_trend_completed_at` datetime(6) NULL
        COMMENT '이 축이 마지막으로 정상 커밋된 시각. 운영 진단용',
    `monthly_trend_aggregated_through` datetime(6) NULL
        COMMENT '집계 수위선. 이 시각 이전 원천이 반영됐다. AggregateObservation.observedAt 의 원천',
    `hourly_heatmap_status` varchar(11) NOT NULL DEFAULT 'PENDING',
    `hourly_heatmap_completed_at` datetime(6) NULL,
    `hourly_heatmap_aggregated_through` datetime(6) NULL,
    `issuance_status_status` varchar(11) NOT NULL DEFAULT 'PENDING',
    `issuance_status_completed_at` datetime(6) NULL,
    `issuance_status_aggregated_through` datetime(6) NULL,

    -- 축별 최신 성공 회차를 고르는 경로다. 배치도 A 도 회차 상태(`status`)가 아니라
    -- **축 상태**로 거른다 — 그래서 인덱스도 축마다 하나씩이다.
    --
    -- 두 번째 컬럼은 as_of 가 아니라 **집계 지점**이다. 다음 회차의 시작점을 고르는 질의가
    -- MAX(<축>_aggregated_through) 라, as_of 를 넣으면 선두만 맞고 집계는 못 덮는다.
    --
    -- ⚠️ 예전에 여기 `(status, as_of DESC, id DESC)` 하나가 있었는데, 그 인덱스를 타는 질의가
    --    이 저장소에 하나도 없었다(실측 — 실제 질의는 key=null 로 풀스캔, status 로 거르는
    --    가짜 질의만 그 인덱스를 탔다). 선두 컬럼이 안 맞으면 인덱스는 서 있기만 한다.

    -- 마감되지 않은 회차를 거두는 경로. 인덱스가 없으면 그 UPDATE 가 표 전체를 훑고, 훑은
    -- 행에 전부 락을 건다 — 매 회차 운영 풀에서 벌어질 일이라 조건에 맞춘 인덱스를 둔다.
    KEY `ix_analytics_run_abandoned` (`status`, `started_at`),

    KEY `ix_analytics_run_monthly`
        (`monthly_trend_status`, `monthly_trend_aggregated_through` DESC),
    KEY `ix_analytics_run_hourly`
        (`hourly_heatmap_status`, `hourly_heatmap_aggregated_through` DESC),
    KEY `ix_analytics_run_status_axis`
        (`issuance_status_status`, `issuance_status_aggregated_through` DESC),

    CONSTRAINT `ck_analytics_run_status` CHECK (
        `status` COLLATE utf8mb4_0900_as_cs
            IN ('IN_PROGRESS', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT `ck_analytics_run_failure` CHECK (
        (`status` COLLATE utf8mb4_0900_as_cs = 'FAILED')
            = (`failure_reason` IS NOT NULL
               AND REGEXP_LIKE(`failure_reason`, '[^[:space:]]'))),
    CONSTRAINT `ck_analytics_run_monthly_trend` CHECK (
        (`monthly_trend_status` COLLATE utf8mb4_0900_as_cs = 'AVAILABLE')
            = (`monthly_trend_completed_at` IS NOT NULL)
        AND (`monthly_trend_status` COLLATE utf8mb4_0900_as_cs = 'AVAILABLE')
            = (`monthly_trend_aggregated_through` IS NOT NULL)
        AND (`monthly_trend_aggregated_through` IS NULL
             OR `monthly_trend_aggregated_through` <= `as_of`)
        AND `monthly_trend_status` COLLATE utf8mb4_0900_as_cs
            IN ('AVAILABLE', 'PENDING', 'UNAVAILABLE')),
    CONSTRAINT `ck_analytics_run_hourly_heatmap` CHECK (
        (`hourly_heatmap_status` COLLATE utf8mb4_0900_as_cs = 'AVAILABLE')
            = (`hourly_heatmap_completed_at` IS NOT NULL)
        AND (`hourly_heatmap_status` COLLATE utf8mb4_0900_as_cs = 'AVAILABLE')
            = (`hourly_heatmap_aggregated_through` IS NOT NULL)
        AND (`hourly_heatmap_aggregated_through` IS NULL
             OR `hourly_heatmap_aggregated_through` <= `as_of`)
        AND `hourly_heatmap_status` COLLATE utf8mb4_0900_as_cs
            IN ('AVAILABLE', 'PENDING', 'UNAVAILABLE')),
    CONSTRAINT `ck_analytics_run_issuance_status` CHECK (
        (`issuance_status_status` COLLATE utf8mb4_0900_as_cs = 'AVAILABLE')
            = (`issuance_status_completed_at` IS NOT NULL)
        AND (`issuance_status_status` COLLATE utf8mb4_0900_as_cs = 'AVAILABLE')
            = (`issuance_status_aggregated_through` IS NOT NULL)
        AND (`issuance_status_aggregated_through` IS NULL
             OR `issuance_status_aggregated_through` <= `as_of`)
        AND `issuance_status_status` COLLATE utf8mb4_0900_as_cs
            IN ('AVAILABLE', 'PENDING', 'UNAVAILABLE'))
);

-- 축 1 — 월별 추이의 원천. DailyIssueAggregate 와 1:1 이다.
--
-- issued_at 기준이라 과거가 안 변한다. 같은 as_of 로 재실행하면 같은 값이다.
-- ⚠️ brand_id 는 coupons.brand_id 의 역정규화 사본이다. 회차의 소속 브랜드를 사람이 손으로
--    고치면(코드 경로는 없다), 새 발급이 없어 재계수되지 않는 과거 버킷은 옛 브랜드로 남는다.
--    그 상태에서 A 의 AdminAnalyticsDataset 생성자가 카탈로그 대조에 실패해 분석 화면이 500 이 된다.
--
--    배치가 매 회차 자동으로 맞추게 해 봤지만 걷어냈다 — 인덱스를 못 타는 조건이라 매시간
--    세 표를 통째로 훑고, 그것이 운영 풀에서 도는 순간 이 프로젝트의 처리량 측정이 흔들린다.
--    (이 티켓이 issuances 인덱스를 금지한 이유와 같은 종류의 비용이다.)
--
--    브랜드를 고쳤다면 아래를 한 번 돌린다. 세 표 모두에 대해:
--      UPDATE analytics_daily_issues a JOIN coupons c ON c.id = a.coupon_id
--         SET a.brand_id = c.brand_id WHERE a.brand_id <> c.brand_id;
--      UPDATE analytics_hourly_issues ... (같은 형태)
--      UPDATE analytics_issuance_statuses ... (같은 형태)
--    집계값은 다시 셀 필요가 없다 — 소속이 바뀌어도 그 회차의 발급 수는 같다.
CREATE TABLE `analytics_daily_issues` (
    `issue_date` date NOT NULL COMMENT 'KST 발급일',
    `coupon_id` bigint NOT NULL COMMENT '회차 ID',
    `brand_id` bigint NOT NULL COMMENT 'coupons.brand_id 역정규화 사본',
    `issue_count` bigint NOT NULL,
    -- ⚠️ **조회에서 이 컬럼으로 거르지 말 것.** 증분이라 한 회차는 그 창에서 바뀐 버킷만 쓴다 —
    --    안 바뀐 버킷은 예전 회차 번호를 단 채 그대로 남아 있어서, 최신 회차로 거르면 통째로 빈다.
    --    여기는 버킷당 한 행(Upsert)이라 요청 기간의 행을 그대로 읽으면 된다.
    --    [A 확정 2026-08-26] 월별·시간대 축은 run_id 로 제한하지 않는다.
    `run_id` bigint NOT NULL COMMENT '이 행을 마지막으로 쓴 집계 회차. 조회 필터 아님',

    PRIMARY KEY (`issue_date`, `coupon_id`),
    KEY `ix_analytics_daily_brand` (`brand_id`, `issue_date`),
    CONSTRAINT `fk_analytics_daily_run` FOREIGN KEY (`run_id`)
        REFERENCES `analytics_runs` (`id`),
    -- 회차가 사라지면 A 의 카탈로그 대조가 실패해 분석 화면이 500 이 되는데, 그때
    -- 집계 행만 보고는 원인을 짚을 수 없다. DB 가 그 삭제를 먼저 막는다.
    CONSTRAINT `fk_analytics_daily_coupon` FOREIGN KEY (`coupon_id`)
        REFERENCES `coupons` (`id`),
    CONSTRAINT `ck_analytics_daily_count` CHECK (`issue_count` >= 0)
);

-- 축 2 — 요일·시간대 히트맵의 원천. HourlyIssueAggregate 와 1:1 이다.
--
-- 요일은 저장하지 않는다. issue_date 에서 계산되는 값이라 같이 담으면 두 값이 어긋날 수 있다.
CREATE TABLE `analytics_hourly_issues` (
    `issue_date` date NOT NULL COMMENT 'KST 발급일',
    `issue_hour` tinyint NOT NULL COMMENT 'KST 0~23시',
    `coupon_id` bigint NOT NULL COMMENT '회차 ID',
    `brand_id` bigint NOT NULL,
    `issue_count` bigint NOT NULL,
    `run_id` bigint NOT NULL,

    PRIMARY KEY (`issue_date`, `issue_hour`, `coupon_id`),
    KEY `ix_analytics_hourly_brand` (`brand_id`, `issue_date`),
    CONSTRAINT `fk_analytics_hourly_run` FOREIGN KEY (`run_id`)
        REFERENCES `analytics_runs` (`id`),
    -- 회차가 사라지면 A 의 카탈로그 대조가 실패해 분석 화면이 500 이 되는데, 그때
    -- 집계 행만 보고는 원인을 짚을 수 없다. DB 가 그 삭제를 먼저 막는다.
    CONSTRAINT `fk_analytics_hourly_coupon` FOREIGN KEY (`coupon_id`)
        REFERENCES `coupons` (`id`),
    CONSTRAINT `ck_analytics_hourly_hour` CHECK (
        `issue_hour` >= 0 AND `issue_hour` <= 23),
    CONSTRAINT `ck_analytics_hourly_count` CHECK (`issue_count` >= 0)
);

-- 축 3 — 현재 상태 분포의 원천. IssuanceStatusAggregate 의 재료다.
--
-- ⚠️ 이 축만 재실행 결정성이 없다. issuances.status 가 계속 바뀌기 때문이다 — 8월 1일 발급분의
--    분포는 8월 2일에 세는 것과 8월 9일에 세는 것이 다르다. 그래서 "8월 1일의 분포" 가 아니라
--    "8월 1일 발급분의 observed_at 시점 분포" 로 읽어야 맞다.
--
-- ⚠️ 만료가 지나도 굳지 않는다. CouponStateMachine:88 의 CANCEL_USE 가 만료 뒤에도 허용되고
--    (USED → EXPIRED), 취소 시한이 core·API·DB 어디에도 없다. 그래서 "확정된 회차" 라는
--    개념을 두지 않는다 — 근거 없는 확정 기준을 두면 그 뒤의 취소가 조용히 어긋난다.
--
-- 대신 전 구간을 다시 세지 않고 **바뀐 버킷만** 다시 센다. issuance_histories 의
-- created_at > 마지막 성공 as_of 로 바뀐 issuance 를 찾아 그것이 속한 (발급일, 회차)만
-- 재계산한다. idx_issuance_histories_created_id 가 이미 있어 인덱스를 새로 만들지 않는다.
-- ⚠️ issuances 에 updated_at 인덱스를 추가하는 방법은 쓰지 않는다 — 발급 경로의 쓰기
--    테이블이라 v1·v2·v3 처리량 측정에 영향을 준다.
--
-- 회차(run_id)를 키에 포함해 누적한다. 조회는 (발급일, 회차)마다 최신 run_id 하나를 고른다.
-- 안 바뀐 버킷은 새 행이 안 생기므로 누적량은 실제 변경량을 따른다.
--
-- 저장 단위가 조회 단위와 다르다. 여기는 **발급일**이고 IssuanceStatusAggregate 는
-- windowFrom~windowTo 다 — 조회 기간은 사용자가 고르는 값이라 미리 만들 수 없다.
-- 발급 1건은 발급일이 하나뿐이므로 Source 가 요청 기간의 행을 단순 합산하면 정확히 맞는다.
--
-- 상태를 행이 아니라 열로 편다(상태별 5행이 아니라 5열). 그래야 네 상태의 합이 total_issued 와
-- 같다는 불변식을 CHECK 로 막을 수 있다 — 행으로 펴면 그 불변식을 DB 가 표현하지 못한다.
-- 합산 가능성은 어느 쪽이든 같다.
CREATE TABLE `analytics_issuance_statuses` (
    `issue_date` date NOT NULL COMMENT 'KST 발급일',
    `coupon_id` bigint NOT NULL COMMENT '회차 ID',
    `brand_id` bigint NOT NULL,

    `total_issued` bigint NOT NULL,
    `currently_issued` bigint NOT NULL COMMENT 'ISSUED',
    `used` bigint NOT NULL,
    `cancelled` bigint NOT NULL,
    `expired` bigint NOT NULL,

    -- 이 행이 몇 시 기준 값인지. 축 단위가 아니라 행 단위로 갖는다 —
    -- 한 회차 안에서도 집계가 나뉘어 돌 수 있고, 그때 어긋난 것을 되짚을 근거가 필요하다.
    `observed_at` datetime(6) NOT NULL,
    -- 여기만 회차가 키에 들어가 누적된다. 조회는 (발급일, 회차)마다 MAX(run_id) 한 행을 고른 뒤
    -- 기간을 합산한다 — 회차 하나로 거르면 그 창에서 안 바뀐 버킷이 빠진다.
    -- [A 확정 2026-08-26] 상태 축만 버킷별 MAX(run_id) 선택.
    `run_id` bigint NOT NULL COMMENT '이 값을 만든 집계 회차. 버킷별 최신 선택의 기준',

    -- run_id 를 키에 넣어 누적한다. 이 정렬이 그대로 "버킷별 최신 회차" 조회 경로다.
    PRIMARY KEY (`issue_date`, `coupon_id`, `run_id`),
    KEY `ix_analytics_status_brand` (`brand_id`, `issue_date`),
    CONSTRAINT `fk_analytics_status_run` FOREIGN KEY (`run_id`)
        REFERENCES `analytics_runs` (`id`),
    -- 회차가 사라지면 A 의 카탈로그 대조가 실패해 분석 화면이 500 이 되는데, 그때
    -- 집계 행만 보고는 원인을 짚을 수 없다. DB 가 그 삭제를 먼저 막는다.
    CONSTRAINT `fk_analytics_status_coupon` FOREIGN KEY (`coupon_id`)
        REFERENCES `coupons` (`id`),
    CONSTRAINT `ck_analytics_status_nonnegative` CHECK (
        `total_issued` >= 0 AND `currently_issued` >= 0 AND `used` >= 0
        AND `cancelled` >= 0 AND `expired` >= 0),
    -- IssuanceStatusAggregate 생성자의 불변식을 DB 에서도 막는다. 여기서 새면 A 쪽 record 가
    -- 조립 단계에서 터지는데, 그때는 어느 집계 회차가 깨뜨렸는지 되짚기 어렵다.
    CONSTRAINT `ck_analytics_status_total` CHECK (
        `currently_issued` + `used` + `cancelled` + `expired` = `total_issued`)
);
