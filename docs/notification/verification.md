# 불변식과 검증 매트릭스

티켓 분할은 [scope.md](scope.md).

## 불변식 (리뷰 체크리스트)

1. 발급 성공 1건당 `notifications` 행은 정확히 1개다 (유니크 제약이 보증).
2. `SENT` 에서 나가는 상태 전이는 없다.
3. `notification_attempts` 는 append-only 다.
4. 관리자 응답 3종의 JSON 어디에도 연락처·본문이 없다.
5. 알림 실패는 쿠폰 발급 트랜잭션을 롤백하지 않는다.
6. 발송 외부 호출은 어떤 락도 들고 있지 않다.
7. `app.notify.sent` 총합 = 종결된 알림 수 (재시도 횟수와 무관).
8. `ObservedValue` 는 미수집을 0 으로 채우지 않는다.
9. 거부된 재발송 요청도 감사에 남는다.
10. 동시에 여러 발행기가 떠도 하나의 lease 소유자만 정상 발행하며 만료된 claim은 회수된다.
11. 같은 `attemptSeq`의 `SENDING` 재수신만 mock 발송을 재개하며 완료 attempt 승자만 미터를 올린다.
12. 수동 재발송 outbox가 DEAD가 되면 알림은 원자적으로 `FAILED`로 돌아간다.
13. `FAILED`·`DEAD` 알림은 실패 사유와 `failed_at`을 함께 가진다.

---

## 검증 매트릭스

| # | 시나리오 | 기대 | 티켓 |
|---|---|---|---|
| 1 | 발급 → 발송 성공 | `SENT`, attempts 1행 SUCCESS, meter success 1 | T2 |
| 2 | 타임아웃 2회 후 성공 | `SENT`, attempts 3행, meter success **1** | T2 |
| 3 | 타임아웃 3회 소진 | `DEAD`, DLT 1건, meter failure 1 | T2 |
| 4 | INVALID_RECIPIENT | 즉시 `DEAD`, 백오프 없음, DLT 1건 | T2 |
| 5 | 발송 실패 시 발급 트랜잭션 | 롤백 안 됨 | T2 |
| 6 | 같은 이벤트 2회 수신 | 알림 1건, 시도 1행 | T2 |
| 7 | summary 정상 | VALID + 계산값 일치 | T3 |
| 8 | summary 알림 0건 캠페인 | `sentRate` = `N_A`, 나머지 `NO_TRAFFIC` | T3 |
| 9 | summary 없는 캠페인 | 전 필드 `N_A` | T3 |
| 10 | failures cursor 없음/있음, limit 1·200 | 경계 동작 + `hasOlder` 정확 | T3 |
| 11 | 관리자 응답 PII 부재 | 직렬화 문자열에 연락처·본문 없음 | T3 |
| 12 | 비관리자 호출 | ADMIN-002 (403) | T3 |
| 13 | 동시 재발송 2건 | 1건만 202, 1건 ADMIN-006 | T4 |
| 14 | 4번째 재발송 | ADMIN-007 | T4 |
| 15 | SENT 재발송 | ADMIN-006 | T4 |
| 16 | 없는 알림 재발송 | ADMIN-005 | T4 |
| 17 | 재발송 접수 → 발송 성공 | `SENT`, audit accepted 1행 | T4 |
| 18 | 거부 요청 감사 | audit rejected 행 + reject_code | T4 |
| 20 | outbox 동시 claim | 한 인스턴스만 token 획득 | T1 |
| 21 | outbox claim 뒤 프로세스 종료 | lease 만료 후 다른 token으로 회수 | T1 |
| 22 | outbox 발행 10회 실패 | DEAD 격리, 뒤 PENDING 명령 claim 가능 | T1 |
| 23 | 워커 JVM 시계가 서로 다름 | claim 판정은 DB 시계만 사용하므로 결과 불변 | T1 |
| 24 | lease 만료 10회 | DEAD 격리, 뒤 PENDING 명령 claim 가능 | T1 |
| 26 | 같은 attempt가 SENDING에서 재수신 | attempt 증가 없이 재개, 다른 seq는 거부 | T1 |
| 27 | 수동 outbox 10회 실패 | outbox가 완료 attempt 승자면 notification FAILED 원자적 종결 | T1 |

---
