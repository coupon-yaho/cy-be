# 알림 설계 — 영역 A-09 (B 이관)

**이 디렉터리가 알림 범위의 단일 진실 원천(SSOT)이다.**
구현 중 여기 없는 것이 필요해지면 코드를 먼저 쓰지 말고 문서를 고친다. 문서를 안 고치고
구현하면 범위가 조용히 늘어난다 — 그게 후행 티켓이 된다.

- 티켓: T1(CY-642) · T2 · T3 · T4 · 에픽 「관리자 페이지 구성」
- 기준 브랜치 `feature/CY-5` (main 은 473 커밋 뒤라 통합 브랜치가 아니다)
- 진행 상태는 `.claude/CY-642.md` 가 갖는다. 이 디렉터리는 **결정만** 담는다

---

## 무엇이 필요한가 → 어디를 여는가

| 지금 하려는 일 | 여는 문서 | 절 |
|---|---|---|
| 왜 이렇게 정했는지 확인 · 새 판단이 필요 | [decisions.md](decisions.md) | D1~D16 |
| 테이블·컬럼·인덱스·제약을 쓴다 | [schema.md](schema.md) | §3 |
| 상태 전이를 구현·검토한다 | [pipeline.md](pipeline.md) | 상태 기계 |
| 이벤트 record 를 정의한다 | [pipeline.md](pipeline.md) | 이벤트 계약 |
| 트랜잭션 경계·Consumer·재시도·DLT | [pipeline.md](pipeline.md) | 발송 파이프라인 |
| 미터를 등록한다 | [pipeline.md](pipeline.md) | 미터 |
| 요약·실패목록 Service 를 만든다 | [admin-api.md](admin-api.md) | 관리자 조회 |
| 실패 사유 enum 을 쓴다 | [admin-api.md](admin-api.md) | 실패 사유 enum |
| 수동 재발송·멱등·감사를 만든다 | [admin-api.md](admin-api.md) | 수동 재발송 |
| 에러 코드를 추가한다 | [admin-api.md](admin-api.md) | 신규 에러 코드 |
| 리뷰 체크리스트가 필요하다 | [verification.md](verification.md) | 불변식 |
| 무슨 테스트를 써야 하나 | [verification.md](verification.md) | 검증 매트릭스 |
| 이걸 이번에 하는 게 맞나 | [scope.md](scope.md) | 범위 밖 |
| 어느 티켓 몫인가 | [scope.md](scope.md) | 티켓별 산출물 |

---

## 30초 요약

```text
발급 커밋 → notifications(PENDING) + outbox(PENDING) → outbox relay가 coupon.notify 발행(key=memberId)
  → notify-dispatch 수신 → SENDING → NotificationSender.send()
      성공         → SENT  + meter success
      재시도가능   → 1s·5s·20s 3회 → 소진 시 DEAD + DLT + meter failure
      재시도불가   → 즉시 DEAD + DLT + meter failure
  → 관리자 summary / failures 조회
  → 관리자 resend → 멱등 선점 → audit → 재발행 → 202
```

권위값은 **DB** 다. Kafka·JVM 메모리는 전달 수단일 뿐이고, 관리자 조회와 재발송 판정은
`notifications` / `notification_attempts` 만 읽는다.

## 반드시 알아야 할 3가지

1. **발송 실패는 쿠폰 발급을 롤백하지 않는다.** 알림 행 생성만 발급과 같은 트랜잭션이고,
   Kafka 발행은 커밋 이후다 (D1).
2. **관리자 응답에 연락처·메시지 본문이 없다.** 필드 자체가 없다 — 마스킹이 아니다 (D10).
3. **`app.notify.sent` 는 고객 알림 한 건당 한 번 센다.** 재시도 3번 후 성공은 `success` 1이다 (D13).

실 발송 벤더가 아니라 mock 발송의 성공률과 retry/DLT를 관측하는 것이 목적이다. 같은 시도의
Kafka 재수신은 재개하되, 실 발송 exactly-once나 `SENDING` lease는 만들지 않는다(D21).
