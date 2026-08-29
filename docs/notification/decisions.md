# 알림 결정 기록

알림 범위의 결정을 번호로 고정한다. 코드 주석과 다른 문서가 이 번호를 인용하므로
**여기 정의되지 않은 번호는 인용할 수 없다.**

"안 하기로 한 것"도 결정이다. [범위 밖](scope.md)의 항목은 빠뜨린 게 아니라 여기서 닫은 것이다.

## 결정표

| # | 항목 | 확정값 |
|---|---|---|
| D1 | 발송 시점 | 쿠폰 발급 트랜잭션 **커밋 이후**. 트랜잭션·락 구간 안에서 발행/발송 금지 |
| D2 | 알림 상태 | `PENDING · SENDING · SENT · FAILED · DEAD` 5개 |
| D3 | 재발송 표현 | 상태 되돌림이 아니라 `notification_attempts` **새 행**. `notifications` 는 최종 상태만 보유 |
| D4 | 재시도 가능 실패 | 타임아웃 · 5xx · 커넥션 오류 |
| D4b | 재시도 불가 실패 | 잘못된 수신자 · 4xx · 직렬화 실패 → 즉시 `DEAD` |
| D5 | 자동 재시도 | 3회 / 백오프 1s · 5s · 20s (`DefaultErrorHandler`) |
| D6 | DLT 이동 | 재시도 소진 또는 재시도 불가 실패 → `coupon.notify.DLT` |
| D7 | DLT 재처리 | **자동 소비자 없음.** `KafkaConsumerGroups.DLT_REPROCESS` 는 이번 범위 미사용. 복구는 수동 재발송 경로로만 |
| D8 | 수동 재발송 | 알림당 최대 3회, 멱등 윈도우 10분. `status + attempt_count` CAS로 선점하고 `notification_attempts` UK로 완료 중복을 막는다 |
| D9 | 재발송 가능 상태 | `FAILED` · `DEAD` 만. `SENT` · `SENDING` · `PENDING` 은 409 |
| D10 | PII | 프로토타입은 연락처·본문을 평문으로 저장한다. 관리자 응답 DTO와 로그에는 원문 필드가 없다 |
| D11 | 보관기간 | 알림·시도 90일, 감사 1년. **정리 배치는 범위 밖** (문서로만 남긴다) |
| D12 | 발송 Adapter | `NotificationSender` + `NotificationSendException(NotifyFailureReason)`. 로그 구현체만 두고 **실 벤더 연동은 범위 밖** |
| D13 | 미터 | `app.notify.sent{result="success\|failure"}` 하나. 실패 사유별 태그 분리 안 함 |
| D14 | 실패 사유 표현 | `core.observation.ReasonCode` 를 **쓰지 않는다.** 신규 `NotifyFailureReason` 를 만들고 `NotificationFailureItem` 필드 타입을 바꾼다 (근거 [admin-api.md](admin-api.md#실패-사유-enum--기존-계약-변경-d14)) |
| D15 | 알림 채널 | 단일 채널. `channel` 컬럼을 두되 값은 `DEFAULT` 하나. 멀티채널 범위 밖 |
| D16 | 알림 트리거 | 쿠폰 발급 성공 1건 = 알림 1건. 캠페인 종료·만료 임박 알림은 범위 밖 |
| D17 | 발행 내구성 | 상태 전이·감사와 `notification_outbox(PENDING)`를 같은 DB 트랜잭션에 저장. DB `CURRENT_TIMESTAMP(6)` 기반 lease와 fencing token으로 선점한 뒤 DB 락 밖에서 발행하고 token이 같은 행만 `PUBLISHED`로 확정 |
| D19 | outbox 실패 | 명시적 발행 실패와 lease 만료 회수 모두 실패 1회로 센다. lease 만료 회수 뒤에는 1초를 기다리고 한 poll에서 1건만 회수한다. 프로토타입은 10회 고정으로 `DEAD` 처리하며 운영 중 설정 변경은 후속 범위다. 회수와 신규 claim을 인덱스별 쿼리로 분리한다 |
| D20 | 회차 어휘 | 알림의 회차 축은 `coupons.id`이며 코드·HTTP는 `couponId`, DB는 `coupon_id`만 쓴다 |
| D21 | 전달 보장 수준 | 실 벤더가 아닌 mock 발송의 성공률과 retry/DLT 관측이 목적이다. 같은 `attemptSeq`의 `SENDING` 재수신은 재개하고, 완료 attempt INSERT 승자만 결과 미터를 올린다. 실 발송 exactly-once와 `SENDING` lease는 범위 밖 |
| D22 | outbox DEAD 종결 | 수동 재발송 outbox가 10회 실패하면 같은 트랜잭션에서 완료 실패 attempt를 먼저 기록한다. 같은 attempt의 완료 결과는 먼저 기록한 쪽만 상태를 확정한다. outbox가 승자면 `SENDING`을 `FAILED(OUTBOX_PUBLISH_FAILED)`로 되돌리고 재발송 예산을 환급한다. 초기 발행의 `PENDING` 잔류는 관제로 드러내며 자동 회수하지 않는다 |
| D24 | outbox lock 충돌 | lease 회수와 신규 claim은 독립 새 트랜잭션으로 실행한다. lock timeout·deadlock은 이번 poll의 빈 claim으로 처리하고 다음 poll이 재시도한다 |
| D25 | 프로토타입 참조 무결성 | 알림 4개 테이블에는 FK를 두지 않는다. 존재하지 않는 알림의 재발송 거부(`ADMIN-005`)도 감사해야 하며, T1은 mock 발송·관제 프로토타입이다. 운영 전에는 참조 무결성과 보존·삭제 정책을 함께 결정한다 |

---
