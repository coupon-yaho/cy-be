# 관리자 알림 API

스키마는 [schema.md](schema.md), 결정 근거는 [decisions.md](decisions.md).

## 관리자 조회

### 6.1 실패 사유 enum — 기존 계약 변경 (D14)

현행 `NotificationFailureItem` 은 `core.observation.ReasonCode` 를 쓴다. 그런데 그 enum 값은
`NOT_OPENED` · `STOCK_EXHAUSTED` · `QUEUE_REQUIRED` 처럼 **전부 발급 실패용**이라 발송 실패를
표현할 값이 하나도 없다. 53개 파일이 참조하는 공용 enum 이라 알림 값을 섞으면 발급 도메인이 오염된다.

**신규 enum 을 만들고 DTO 필드 타입을 바꾼다.**

```java
package com.kafkick.core.notification.domain;

public enum NotifyFailureReason {
    SEND_TIMEOUT,        // 재시도 가능
    SEND_UNAVAILABLE,    // 재시도 가능 (5xx)
    CONNECTION_ERROR,    // 재시도 가능
    INVALID_RECIPIENT,   // 재시도 불가
    REJECTED_BY_PROVIDER,// 재시도 불가 (4xx)
    SERIALIZATION_ERROR, // 재시도 불가
    OUTBOX_PUBLISH_FAILED, // 재시도 불가. mock 발송 전 이벤트 발행 소진
    UNKNOWN;

    public boolean retryable() { ... }   // D4 / D4b 를 코드로 고정
}
```

`retryable()` 를 enum 에 두면 재시도 정책이 설정 파일이 아니라 타입에 박힌다.

**영향 범위는 좁다.** `NotificationFailurePageResponse` 를 참조하는 곳은 컨트롤러와
`AdminExtendedDtoJsonSerializationTest`(빈 리스트) 둘뿐이라 변경 비용이 거의 없다.
**이 변경은 T1 에서 한다.** T3 로 미루면 T3 가 계약 변경 티켓이 된다.

### 6.2 `GET /notifications/summary`

응답은 기존 `NotificationSummaryResponse` 그대로. 계산식:

| 필드 | 계산 |
|---|---|
| `totalRequests` | `COUNT(*)` |
| `sentCount` | `status = 'SENT'` |
| `failedCount` | `status IN ('FAILED','DEAD')` |
| `remainingCount` | `status IN ('PENDING','SENDING')` |
| `sentRate` | `sentCount / totalRequests`. **분모 0 이면 `N_A`** (0.0 아님) |

`ObservedValue` 상태 규칙 (`ObservedValue` 생성자 불변식이 강제한다):

- 정상 집계 → `VALID` + `observedAt`
- `couponId` 는 있으나 알림이 0건 → `NO_TRAFFIC` (값 0 + 시각 있음)
- 존재하지 않는 회차 → `N_A` (value·observedAt 모두 null)
- 조회 실패 → `UNAVAILABLE` (value·observedAt 모두 null)

**미수집·조회 실패를 0 건으로 표현하지 않는다.** `carriesValue()` 위반은 생성자가 던진다.

### 6.3 `GET /notifications/failures`

- 대상: `status IN ('FAILED','DEAD')`
- 정렬: `id DESC` (과거 방향). `ix_notifications_failure_keyset` 사용
- cursor: `beforeCursor` 는 `id` 의 문자열 표현. 파싱 실패 시 400
- `limit` 1~200, 기본 50 (기존 컨트롤러 검증 유지)
- `hasOlder` 는 `limit + 1` 조회로 판정
- 항목 필드: `notificationId`, `couponId`, `memberId`, `reason(NotifyFailureReason)`,
  `attemptCount`, `failedAt` — **연락처·본문 없음(D10)**

---

## 수동 재발송

### 7.1 순서

```text
POST /notifications/{id}/resend
  → 알림 조회             없음        → ADMIN-005 (404)
  → 상태 확인             SENT 등     → ADMIN-006 (409)
  → resend_count 확인     >= 3        → ADMIN-007 (409)
  → 상태·attempt_seq CAS  선점 실패  → ADMIN-006 (409, 멱등 윈도우 내 중복)
  → audit 기록 (accepted/rejected 모두; 선점 전 거부는 attempt_seq null)
  → accepted 이면 outbox(PENDING) 기록
  → notifications: FAILED/DEAD → SENDING, resend_count += 1
  → coupon.notify 재발행 (trigger=MANUAL, attemptSeq)
  → 202 + NotificationResendAcceptedResponse
```

`202` 는 **발송 완료가 아니라 접수**다. `requestedAt` 은 접수 시각이다(기존 DTO Javadoc 유지).

### 7.2 멱등 (D8)

발송 전에 `notifications`를
`FAILED/DEAD → SENDING`으로 조건부 갱신하며 `attempt_count`를 증가시켜 한 요청만 선점한다.
CAS 술어는 상태와 읽은 `attempt_count`를 둘 다 비교한다.
`notification_attempts.uk_attempts_notification_seq`는 발송이 끝난 뒤 같은 시도 결과가 두 번
적재되는 것을 막는 2차 방어선이다. 결과·종료 시각이 `NOT NULL`인 append-only 행이므로
발송 전 선점용으로 쓰지 않는다.

- 동시 2건이 같은 상태를 CAS → 하나만 1행 갱신 → 나머지는 ADMIN-006 으로 거부
- 10분 윈도우: 같은 알림에 대해 **직전 접수가 10분 이내면** 새 `attempt_seq` 를 발급하지 않고 거부
  (`findLatestAcceptedByNotificationId`; 더 최신인 거부 감사는 판정에서 제외)

발행과 상태 전이는 **같은 트랜잭션 안에서 하지 않는다** — 발행은 커밋 이후(D1 동일 원칙).
대신 CAS·accepted audit·outbox PENDING INSERT는 하나의 DB 트랜잭션으로 묶는다.

### 7.3 신규 에러 코드 (T1 에서 추가)

| 코드 | HTTP | 의미 |
|---|---:|---|
| `ADMIN-005` | 404 | 해당 알림이 없음 |
| `ADMIN-006` | 409 | 재발송 불가 상태이거나 멱등 윈도우 내 중복 요청 |
| `ADMIN-007` | 409 | 수동 재발송 횟수 상한 초과 |

---
