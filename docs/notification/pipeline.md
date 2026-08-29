# 알림 흐름과 발송 파이프라인

스키마는 [schema.md](schema.md), 결정 근거는 [decisions.md](decisions.md).

## 전체 흐름

```text
발급 트랜잭션 커밋
  → notifications(PENDING) + notification_outbox(PENDING) 저장
  → outbox relay가 coupon.notify 발행 (key = memberId)
  → notify-dispatch Consumer 수신
  → notifications SENDING
  → NotificationSender.send()
      성공 → attempts(SUCCESS) + notifications SENT + meter success
      재시도가능실패 → attempts(FAILED) → 백오프 재시도 (최대 3)
          소진 → notifications DEAD + DLT + meter failure
      재시도불가실패 → attempts(FAILED) + notifications DEAD + DLT + meter failure
  → (관리자) summary / failures 조회
  → (관리자) resend → 멱등 선점 → audit 기록 → coupon.notify 재발행 → 202
```

**권위값은 DB다.** Kafka 와 JVM 메모리는 전달 수단일 뿐, 관리자 조회·재발송 판정은
`notifications` / `notification_attempts` 만 읽는다.

---

## 상태 기계

T1 계약에서 상태 변경은 불변 도메인 객체를 반환한다. `startSending(trigger, at)`은
`PENDING` 또는 `FAILED`를 `SENDING`으로 옮기고, `DEAD`는 `MANUAL` trigger에서만 허용한다.
`markSent(at)`, `markFailed(reason, at)`, `markDead(reason, at)`이 나머지 전이를 담당한다.
`notification_attempts`와 `notification_resend_audits`는 record 값으로 생성한 뒤 변경 메서드를
두지 않는다.

### 2.1 `notifications.status` (D2)

| From | To | 계기 |
|---|---|---|
| — | `PENDING` | 발급 커밋 후 행 생성 |
| `PENDING` | `SENDING` | Consumer 수신 |
| `SENDING` | `SENT` | 발송 성공 |
| `SENDING` | `FAILED` | 재시도 가능 실패 (재시도 여력 남음) |
| `FAILED` | `SENDING` | 자동 재시도 또는 수동 재발송 |
| `SENDING` | `DEAD` | 재시도 소진 또는 재시도 불가 실패 |
| `FAILED` | `DEAD` | 재시도 소진 |

**금지 전이** — `SENT` 에서 나가는 전이는 없다. `DEAD → SENDING` 은 **수동 재발송만** 허용하고
자동 경로에서는 금지한다. 그 외 전이는 도메인에서 `IllegalStateException` 이 아니라
도메인 예외로 거부한다(프로젝트 관례).

### 2.2 `notification_attempts.result`

`SUCCESS` · `FAILED` 2개. 시도 행은 **추가만 되고 갱신되지 않는다**(append-only).

---

## 이벤트 계약

```java
public record NotificationRequestedEvent(
        Long notificationId,
        Long memberId,      // partition key
        Long couponId,
        int attemptSeq,     // 이 발행이 몇 번째 시도인지. 재발송 재발행이 이 값으로 구분된다
        AttemptTrigger trigger,    // INITIAL · MANUAL
        Instant requestedAt) { }
```

- **PII 를 이벤트에 싣지 않는다.** 수신처·본문은 Consumer 가 `notificationId` 로 DB 에서 읽는다.
  Kafka 는 7일 보존이라 PII 를 실으면 보관기간 정책(D11)을 우회한다.
- 파티션 키는 `memberId` (기존 `PartitionKeys` 계약).
- `AUTO` 는 이벤트 trigger 에 없다. 자동 재시도는 재발행이 아니라 Consumer 내부 백오프다.

---

## 발송 파이프라인

이 파이프라인의 주목적은 **mock 발송의 성공률과 Kafka retry/DLT 동작을 관측하는 것**이다.
실 벤더의 외부 부작용을 exactly-once로 만드는 문제는 다루지 않으며, 이를 위한 `SENDING`
lease도 두지 않는다(D21).

### 5.1 트랜잭션 경계 (D1)

```text
[발급 트랜잭션]  재고 차감 · issuance 저장 · notifications(PENDING) · outbox(PENDING) 저장
      ↓ 커밋
[outbox relay]   claim → coupon.notify 발행 → PUBLISHED 확정            ← DB 락 밖 발행
```

알림 행과 outbox 명령은 발급과 **같은 트랜잭션**이다. 그래야 발급됐는데 알림 또는 발행 명령이
없는 상태가 안 생긴다. 발행은 커밋 뒤 outbox relay가 수행한다. 발행 실패는 발급을 롤백하지 않고,
그 알림과 명령은 `PENDING`으로 남는다.

상태 저장 트랜잭션은 `notification_outbox(PENDING)`도 같이 남긴다. 발행기는 짧은 DB
트랜잭션에서 발행 가능한 행을 `IN_PROGRESS`로 바꾸고 DB 시각의 lease와 무작위 fencing token을
기록한 뒤 커밋한다. Kafka 발행은 DB 락 밖에서 수행하고, token이 같은 행만 `PUBLISHED`로
확정한다. 프로세스가 종료되면 lease가 만료된 행을 다른 인스턴스가 회수한다. Kafka 발행 후
확정 전에 종료되면 다시 발행될 수 있으며 이벤트의 `notificationId:attemptSeq`로 흡수한다.

발행 실패와 lease 만료 회수는 실패 횟수와 DB 기준 다음 재시도 시각을 기록하고 `PENDING`으로
되돌린다. 연속 10회 실패한
행은 `DEAD`로 격리한다. 조회는 `next_attempt_at`이 지난 행만 대상으로 하므로 한 독약 행이
뒤의 정상 명령을 막지 않는다.

> `PENDING` 으로 남은 알림의 회수는 **범위 밖**이다(정리·회수 배치 없음, D11). 관리자 요약의
> `remainingCount` 가 이 값을 드러내므로 운영자가 눈으로 본다.

### 5.2 Consumer

- 그룹 `notify-dispatch`, `auto.offset.reset` 은 `KafkaConsumerGroups` 가 정한 값을 그대로 따른다.
- 수신 시 `notifications` 를 `SENDING` 으로 전이. 이미 `SENT` 면 **아무것도 하지 않고 offset 만 넘긴다**
  (중복 수신 방어).
- 이벤트의 `attempt_seq` 는 해당 발행 명령의 시작 회차다. 최초·수동 발송은 이벤트 값과 DB를
  대조하고, 자동 재시도는 같은 발행 명령 안에서 DB `attempt_count + 1`을 새 회차로 사용한다.
  시작 회차 뒤에 다른 `MANUAL` 회차가 있으면 오래된 재발행이므로 버린다.
- 프로세스 종료 뒤 같은 이벤트가 다시 오면 `SENDING`이고 `attempt_count == attempt_seq`인 경우에만
  재개한다. 이미 완료 attempt가 있으면 다시 발송하지 않고 그 결과로 알림 상태만 수렴한다. 완료
  attempt의 UK가 결과 확정 승자를 정하며, 그 승자만 미터를 올린다.
- retry 소진 또는 재시도 불가 실패는 DLT로 보낸 뒤 알림을 `DEAD`로 종결한다.

수동 재발송 outbox가 10회 실패해 `DEAD`가 되면 같은 저장 트랜잭션에서 완료 실패 attempt를 먼저
기록한다. 같은 `attemptSeq`의 완료 결과는 먼저 기록한 쪽이 승자다. outbox 실패가 승자일 때만
`SENDING → FAILED(OUTBOX_PUBLISH_FAILED)`로 되돌리고, 실제 발송을 시작하지 못한 인프라 실패이므로
`resend_count`를 되돌린다. Consumer가 먼저 기록한 결과의 상태 수렴은 재수신 Consumer가 맡는다.
초기 발행 outbox의 알림은 `PENDING`이므로 이 전이를 적용하지 않고 관제의 `remainingCount`로 드러낸다.

### 5.3 재시도와 DLT (D5·D6)

- `DefaultErrorHandler`에 순서가 고정된 `SequenceBackOff(1s, 5s, 20s)`를 연결해
  최초 발송 뒤 재시도 3회를 수행한다. 최대 호출 수는 총 4회다.
- 재시도 불가 실패(D4b)는 `addNotRetryableExceptions` 로 즉시 DLT.
- DLT 파티션은 원본과 같은 6이다(기존 `KafkaTopicConfig` 주석 참조). 건드리지 않는다.
- **백오프가 컨슈머 스레드를 점유한다.** 최대 26초. 파티션 6 · concurrency 는 6 이하로 유지.

### 5.4 미터 (D13)

`app.notify.sent{result}` 는 **발송 결과가 확정된 지점에서만** 증가한다.

- 알림 행 생성 시 증가하지 않는다.
- Kafka 발행 성공만으로 `success` 로 세지 않는다.
- 재시도 3번 후 성공 = `success` **1**. 실패 시도는 세지 않는다(고객 알림 한 건 = 카운트 한 건).
- `MeterNames.NOTIFY_SENT` 주석을 T1 에서 해제하고 TODO 를 지운다.

---
