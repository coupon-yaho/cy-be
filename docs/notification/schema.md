# 알림 스키마

결정 근거는 [decisions.md](decisions.md), 상태 정의는 [pipeline.md](pipeline.md#상태-기계).

## 스키마

Flyway 파일명: `V2026082701__notifications.sql` (날짜 버전 관례)

### 3.1 `notifications`

```sql
CREATE TABLE `notifications` (
    `id`                  bigint       NOT NULL AUTO_INCREMENT,
    `coupon_id`           bigint       NOT NULL COMMENT '회차(coupons.id) 요약 집계 축',
    `member_id`           bigint       NOT NULL COMMENT 'Kafka partition key 이자 수신 대상',
    `issuance_id`         bigint       NOT NULL COMMENT '알림을 유발한 발급. 중복 알림 방지의 유일 축',
    `channel`             varchar(10)  NOT NULL DEFAULT 'DEFAULT' COMMENT 'D15. 지금은 DEFAULT 하나',
    `status`              varchar(8)   NOT NULL COMMENT 'PENDING·SENDING·SENT·FAILED·DEAD',
    `attempt_count`       int          NOT NULL DEFAULT 0 COMMENT '자동+수동 합산 시도 횟수',
    `resend_count`        int          NOT NULL DEFAULT 0 COMMENT '수동 재발송 횟수. D8 상한 3의 분자',
    `last_failure_reason` varchar(24)  NULL COMMENT 'NotifyFailureReason. 종결 실패 상태에서만 존재',
    `recipient_contact`     varchar(255) NOT NULL COMMENT 'D10. 프로토타입 평문',
    `message_body`          varchar(500) NOT NULL COMMENT 'D10. 프로토타입 평문',
    `created_at`          datetime(6)  NOT NULL,
    `updated_at`          datetime(6)  NOT NULL,
    `sent_at`             datetime(6)  NULL COMMENT 'SENT 확정 시각',
    `failed_at`           datetime(6)  NULL COMMENT '마지막 실패 시각. 실패 목록 정렬 축',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_notifications_issuance_channel` (`issuance_id`, `channel`),
    KEY `ix_notifications_coupon_status` (`coupon_id`, `status`),
    KEY `ix_notifications_failure_keyset` (`status`, `id` DESC),
    CONSTRAINT `ck_notifications_status` CHECK (
        `status` COLLATE utf8mb4_0900_as_cs
            IN ('PENDING','SENDING','SENT','FAILED','DEAD')),
    CONSTRAINT `ck_notifications_sent_at` CHECK (
        (`status` COLLATE utf8mb4_0900_as_cs = 'SENT') = (`sent_at` IS NOT NULL)),
    CONSTRAINT `ck_notifications_failure_reason` CHECK (
        (`status` COLLATE utf8mb4_0900_as_cs IN ('FAILED','DEAD'))
            = (`last_failure_reason` IS NOT NULL)),
    CONSTRAINT `ck_notifications_failed_at` CHECK (
        `status` COLLATE utf8mb4_0900_as_cs NOT IN ('FAILED','DEAD') OR `failed_at` IS NOT NULL),
    CONSTRAINT `ck_notifications_attempt_count` CHECK (`attempt_count` >= 0),
    CONSTRAINT `ck_notifications_resend_count` CHECK (`resend_count` BETWEEN 0 AND 3),
    CONSTRAINT `ck_notifications_channel` CHECK (
        `channel` COLLATE utf8mb4_0900_as_cs = 'DEFAULT'),
    CONSTRAINT `ck_notifications_failure_reason_value` CHECK (`last_failure_reason` IS NULL OR
        `last_failure_reason` COLLATE utf8mb4_0900_as_cs IN ('SEND_TIMEOUT','SEND_UNAVAILABLE',
        'CONNECTION_ERROR','INVALID_RECIPIENT','REJECTED_BY_PROVIDER','SERIALIZATION_ERROR',
        'OUTBOX_PUBLISH_FAILED','UNKNOWN'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
```

`uk_notifications_issuance_channel` 이 **중복 알림 생성의 유일한 방어선**이다.
발행 재시도나 이벤트 중복 수신이 있어도 알림 행은 발급당 하나다.

### 3.2 `notification_attempts`

```sql
CREATE TABLE `notification_attempts` (
    `id`              bigint      NOT NULL AUTO_INCREMENT,
    `notification_id` bigint      NOT NULL,
    `attempt_seq`     int         NOT NULL COMMENT '1부터. 멱등키의 뒷자리 (D8)',
    `trigger`         varchar(8)  NOT NULL COMMENT 'INITIAL·AUTO·MANUAL',
    `result`          varchar(7)  NOT NULL COMMENT 'SUCCESS·FAILED',
    `failure_reason`  varchar(24) NULL COMMENT 'FAILED 일 때만',
    `started_at`      datetime(6) NOT NULL,
    `finished_at`     datetime(6) NOT NULL,
    `created_at`      datetime(6) NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_attempts_notification_seq` (`notification_id`, `attempt_seq`),
    KEY `ix_attempts_notification` (`notification_id`, `id`),
    CONSTRAINT `ck_attempts_result` CHECK (
        `result` COLLATE utf8mb4_0900_as_cs IN ('SUCCESS','FAILED')),
    CONSTRAINT `ck_attempts_trigger` CHECK (
        `trigger` COLLATE utf8mb4_0900_as_cs IN ('INITIAL','AUTO','MANUAL')),
    CONSTRAINT `ck_attempts_failure_reason_value` CHECK (`failure_reason` IS NULL OR
        `failure_reason` COLLATE utf8mb4_0900_as_cs IN ('SEND_TIMEOUT','SEND_UNAVAILABLE',
        'CONNECTION_ERROR','INVALID_RECIPIENT','REJECTED_BY_PROVIDER','SERIALIZATION_ERROR',
        'OUTBOX_PUBLISH_FAILED','UNKNOWN')),
    CONSTRAINT `ck_attempts_failure_reason` CHECK (
        (`result` COLLATE utf8mb4_0900_as_cs = 'FAILED') = (`failure_reason` IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
```

`uk_attempts_notification_seq` 가 **완료된 시도 결과의 중복 적재 방어선**이다.
발송 전 선점은 `notifications` 조건부 상태 갱신이 담당한다([admin-api.md](admin-api.md#멱등-d8)).

### 3.3 `notification_resend_audits`

```sql
CREATE TABLE `notification_resend_audits` (
    `id`              bigint       NOT NULL AUTO_INCREMENT,
    `notification_id` bigint       NOT NULL,
    `attempt_seq`     int          NULL COMMENT '접수된 요청이 선점한 시도 번호. 선점 전 거부는 NULL',
    `requested_by`    bigint       NOT NULL COMMENT '관리자 memberId',
    `requested_at`    datetime(6)  NOT NULL,
    `accepted`        boolean      NOT NULL COMMENT '거부도 남긴다. 누가 무엇을 시도했는지가 감사 대상',
    `reject_code`     varchar(12)  NULL COMMENT 'ADMIN-005·006·007',
    `created_at`      datetime(6)  NOT NULL,
    PRIMARY KEY (`id`),
    KEY `ix_resend_audits_notification` (`notification_id`, `id`),
    CONSTRAINT `ck_resend_audits_reject_code` CHECK (`reject_code` IS NULL OR
        `reject_code` COLLATE utf8mb4_0900_as_cs IN ('ADMIN-005','ADMIN-006','ADMIN-007')),
    CONSTRAINT `ck_resend_audits_reject` CHECK (
        (`accepted` AND `reject_code` IS NULL AND `attempt_seq` IS NOT NULL)
        OR (NOT `accepted` AND `reject_code` IS NOT NULL AND `attempt_seq` IS NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
```

**거부된 요청도 기록한다.** 접수된 것만 남기면 "관리자가 30번 눌렀는데 다 막혔다" 를 못 본다.

### 3.4 `notification_outbox`

```sql
CREATE TABLE `notification_outbox` (
    `id`              bigint      NOT NULL AUTO_INCREMENT,
    `notification_id` bigint      NOT NULL,
    `attempt_seq`     int         NOT NULL,
    `trigger`         varchar(8)  NOT NULL COMMENT 'INITIAL·MANUAL. AUTO는 Consumer 내부 재시도',
    `status`          varchar(11) NOT NULL COMMENT 'PENDING·IN_PROGRESS·PUBLISHED·DEAD',
    `failure_count`   int         NOT NULL DEFAULT 0,
    `next_attempt_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    `claimed_at`      datetime(6) NULL,
    `claim_token`     char(36)    NULL,
    `created_at`      datetime(6) NOT NULL,
    `published_at`    datetime(6) NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_notification_outbox_attempt` (`notification_id`, `attempt_seq`),
    KEY `ix_notification_outbox_pending` (`status`, `next_attempt_at`, `id`),
    KEY `ix_notification_outbox_expired` (`status`, `claimed_at`, `id`),
    UNIQUE KEY `uk_notification_outbox_claim_token` (`claim_token`),
    CONSTRAINT `ck_notification_outbox_status` CHECK (
        `status` COLLATE utf8mb4_0900_as_cs IN ('PENDING', 'IN_PROGRESS', 'PUBLISHED', 'DEAD')),
    CONSTRAINT `ck_notification_outbox_published_at` CHECK (
        (`status` COLLATE utf8mb4_0900_as_cs = 'PUBLISHED') = (`published_at` IS NOT NULL)),
    CONSTRAINT `ck_notification_outbox_claim` CHECK (
        (`status` COLLATE utf8mb4_0900_as_cs = 'IN_PROGRESS') = (`claimed_at` IS NOT NULL)
        AND (`status` COLLATE utf8mb4_0900_as_cs = 'IN_PROGRESS') = (`claim_token` IS NOT NULL)),
    CONSTRAINT `ck_notification_outbox_failure_count` CHECK (`failure_count` BETWEEN 0 AND 10),
    CONSTRAINT `ck_notification_outbox_trigger` CHECK (
        `trigger` COLLATE utf8mb4_0900_as_cs IN ('INITIAL', 'MANUAL'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
```

`uk_notification_outbox_attempt`가 같은 발행 명령의 중복 저장을 막는다.

---
