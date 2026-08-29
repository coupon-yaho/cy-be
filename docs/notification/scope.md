# 범위와 티켓 분할

결정 근거는 [decisions.md](decisions.md), 검증 항목은 [verification.md](verification.md).

## 범위 밖 (명시적으로 안 한다)

이 목록은 "빠뜨린 것"이 아니라 **닫은 결정**이다. 리뷰에서 지적되면 이 절을 근거로 답한다.

- 실 발송 벤더 연동 (D12)
- 실 발송 exactly-once와 `SENDING` lease/claim (D21)
- DLT 자동 재처리 컨슈머 (D7)
- 알림·감사 보관기간 정리 배치 (D11)
- `PENDING` 잔류 알림 회수 배치 ([pipeline.md](pipeline.md#트랜잭션-경계-d1))
- 멀티채널 (SMS/푸시/이메일 분기) (D15)
- 캠페인 종료·만료 임박 등 발급 외 알림 트리거 (D16)
- 실패 사유별 미터 태그 분리 (D13)
- 관리자 화면 구현 (A 범위)

---

## 티켓별 산출물

### T1 (CY-642) — 저장 기반과 계약 선정의
- `V2026082701__notifications.sql` ([schema.md](schema.md))
- `core/.../notification/domain/` — `Notification`, `NotificationStatus`, `NotificationAttempt`,
  `AttemptTrigger`, `AttemptResult`, `NotifyFailureReason`
- `core/.../notification/domain/NotificationOutbox` · `NotificationOutboxStatus`
- `core/.../notification/domain/NotificationOutboxClaim` · `NotificationFailure`
- `core/.../notification/NotificationErrorCode`
- `core/.../notification/NotificationSender` (인터페이스만)
- `core/.../notification/NotificationSendException` (`NotifyFailureReason` 전달)
- `core/.../notification/event/NotificationRequestedEventPublisher` (발행 포트만)
- `core/.../notification/event/NotificationRequestedEvent`
- `core/.../notification/NotificationRepository`, `NotificationAttemptRepository`,
  `NotificationResendAuditRepository`, `NotificationOutboxRepository` (인터페이스)
- `storage/.../db/notification/entity/` 4종 + `Repository` 구현
- D24 outbox lock 충돌 복구
- `AdminApiErrorCode` — ADMIN-005/006/007 추가
- `MeterNames.NOTIFY_SENT` 주석 해제 + TODO 제거
- `NotificationFailurePageResponse` — `ReasonCode` → `NotifyFailureReason` (D14)
- `docs/notification/` 유지·갱신

**완료 조건**: 마이그레이션 적용 · 상태 전이 위반 거부 테스트 · **T2·T3·T4 가 이 브랜치를
기다리지 않고 분기 가능**(계약이 전부 여기 있다)

### T2 — 파이프라인 ([pipeline.md](pipeline.md#발송-파이프라인)) · 검증 1~6
### T3 — 조회 ([admin-api.md](admin-api.md#관리자-조회)) · 검증 7~12
### T4 — 재발송 ([admin-api.md](admin-api.md#수동-재발송)) · 검증 13~18

T3·T4 모두 T1 만으로 착수 가능하다. T2 는 발행 포트를 구현하고 T4 는 그 포트를 사용한다.
