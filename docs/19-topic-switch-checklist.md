# 주제를 바꿀 때 무엇이 남고 무엇이 바뀌나

이 저장소는 **선착순 쿠폰**으로 만들었지만, 같은 뼈대를 다른 주제(예: 사전예약)에 다시
쓰려고 한다. 그때 **무엇을 그대로 들고 가고 무엇을 다시 쓰는지**를 <b>세어서</b> 적은 기록이다.

추측이 아니라 계층별로 **도메인 단어(`coupon`·`issuance`)가 코드에 나오는 파일 수**를 세었다.
**주석은 걷어냈다** — 설명에 도메인 이름이 나오는 것과 타입·필드·SQL 이 도메인에 묶인 것은
전혀 다른 문제다.

---

## 어떻게 셌나 — 다시 셀 수 있게

주석을 걷어낸 뒤 도메인 단어가 남는 파일을 센다. 계층이 늘거나 도메인이 바뀌면
`DIRS` 와 `pat` 만 고쳐 다시 돌린다.

```python
pat = re.compile(r'coupon|issuance', re.I)
def strip(src):
    src = re.sub(r'/\*.*?\*/', '', src, flags=re.S)   # 블록 주석·javadoc
    return re.sub(r'//[^\n]*', '', src)                # 줄 주석
# 계층 디렉터리마다 strip(소스) 에 pat 이 걸리는 파일 수 / 전체 파일 수
```

---

## 센 값

| 계층 | 결합 파일 / 전체 | 뜻 |
|---|---:|---|
| `core/batch` — 범용 관제 모델 | **1 / 8** | 이음매 하나(아래 §2) |
| `batch/api` — 범용 관제 API | **1 / 25** | 예외 핸들러의 컨트롤러 목록뿐 |
| `infra/mq` — 발행·소비·아웃박스 | 14 / 34 | 토픽 이름과 **구조**가 섞여 있다(§3) |
| `storage/…/notification` | 3 / 14 | 스키마 |
| `core/notification` | 10 / 41 | **알림이 무엇에 대한 것인가** = 도메인 |
| `core/observation` | 13 / 27 | `CouponRound*` 이벤트 = 도메인 |
| `batch/config` | 6 / 27 | 만료·회차 잡 = 도메인 |
| `batch/job` | 2 / 3 | 잡 정의 그 자체 = 도메인 |
| `core/verification` — **정합성 대사** | 12 / 26 | 규칙은 도메인, **실행 이력은 아니다**(§2.5) |

**읽는 법** — 위 넷이 "인프라", 아래 **다섯**이 "도메인" 이다. 인프라 쪽 결합이 **26개 파일 중 2개**다.
(대사는 §2.5 에서 따로 가른다 — 그 계층만 **파일 단위로 가져갈 수 있는 것과 없는 것**이 섞여 있다.)

---

## 1. 그대로 간다 — 손대지 않는다

**배치 관제 전부.** 경로에 잡 이름이 없고 실행 id 하나로 돈다
(`BatchControlApiTest#theControlPathDoesNotNameAnyJob` 가 그것을 지킨다).

- 이력·스텝 상세·실행 파라미터 조회 (CY-910·911)
- `restart` · `stop` (CY-912) · **시체 회수 `abandon`** (CY-927)
- `cy_batch_*` 알림 전부 — 잡 이름을 **라벨**로 받지 상수로 안 박는다

**아웃박스 릴레이의 기계 부분.** 선점(`SKIP LOCKED`)·fencing 토큰·lease·백프레셔·
Full Jitter 재시도·워커 풀 — 무엇을 나르는지와 무관하다.

**기동 가드들.** `DataSourceTimeoutGuard` · `RelayBinlogFormatGuard` ·
`RelayWorkerPoolHeadroomGuard`(CY-923) · `DefaultZoneGuard`.

**측정 기록.** `docs/12`(인덱스·격리) · `docs/18`(릴레이 처리량) 의 **방법**이 그대로 쓰인다.
숫자는 다시 재야 한다 — `docs/18` 이 *"이 표를 다른 배포 대상에 그대로 옮기지 말 것"* 이라고
적어 둔 이유다.

---

## 2.5 대사 — 가져갈 수 있는 것은 **값 타입 다섯뿐**이다

사전예약 PRD(저장소 밖 문서: `~/Downloads/사전예약 시스템 PRD.pdf`)가 **정합성 대사
배치**를 명시하므로 그쪽에서도 대사는 안 지워진다. 그래서 질문은 "남나 바뀌나" 가 아니라
**어디까지 가져가나**다.

⚠️ **처음 답은 틀렸다.** *"실행 이력은 그대로 간다"* 고 적었는데, **낱말만 세고 타입 참조를
안 봤기 때문**이다. 재 보니 —

```
VerificationRun            DatasetType dataset · Long seedRunId · String datasetFingerprint
VerificationRunRepository  DatasetScale × 2 · DatasetType × 6
```

`DatasetType`(정상셋/오염셋)과 `seedRunId` 는 **이 과제의 검증 방식 자체**다. 게다가
`dataset` 은 `uk_run_params(as_of, dataset, scope, attempt)` 로 **유일성 키에까지** 박혀 있다.
**실행 이력이라는 개념은 가지만 이 타입은 안 간다** — 그쪽은 자기 축으로 다시 쓰고,
이 레코드의 **모양**(언제·무엇을·몇 번째·판정·시작/종료)이 참고가 된다.

| 그대로 가져간다 | 왜 |
|---|---|
| `VerdictType` · `ScopeType` · `StatsStatus` | PASS/FAIL · 전수/증분 · 집계 완전성. 값만 있다 |
| `ResidualCount` | 잔여 집계(CY-947). `int` 셋뿐이고 지속·신규·해소는 집합 연산이다 |
| `FindingKey` | `(String, String)` 둘뿐이다 |

**다섯이 전부다.** 전부 값 타입이고 **다른 타입을 하나도 안 문다** — 그것이 이 목록의 조건이다.

| 다시 쓴다 | 왜 |
|---|---|
| `FindingType` · `TargetKey` · `VerificationFinding` | 규칙 어휘와 대상 키의 **모양** |
| `VerificationFindingRepository` | 낱말은 없지만 `FindingType` 을 문다 — 그 타입이 곧 V1~V6 어휘다 |
| `VerificationRuleRepository` · `StatsRepository` | 규칙 질의. `coupons`·`issuances` 를 직접 읽는다 |
| `DatasetScale` | 축은 무관한데(PRD 도 "검사 건수" 를 요구한다) 필드 이름이 쿠폰이다(CY-945) |
| `VerificationRun` · `VerificationRunRepository` | 위 참조 때문. **처음에 여기가 아니라 위 표에 있었다** |
| `DatasetType` · `ExpectedFindingRepository` · `HourlyIssued` · `CleanupRepository` | 낱말은 없지만 개념이 이 과제 고유다 — 정상셋/오염셋, 정답 매니페스트, 시드 스키마에 글자 단위로 맞춘 요일 표기, `asof_state`·`findings` 를 이름으로 드는 삭제 |

하위 패키지 둘은 재사용 판정에서 뺐다 — `replay/`(일곱 중 여섯이 도메인. `AsOfStateRepository`
만 낱말이 깨끗한데 그 표의 PK 가 `(run_id, coupon_id)` 다)와 `exception/`(검증 규칙의 실패 어휘).

**그 경계를 코드가 지킨다.** `VerificationDomainBoundaryTest` 가 넷을 본다 — 가져가는 쪽이
**낱말과 타입 둘 다** 안 무는가, 새 타입·하위 패키지가 분류에서 빠지지 않았는가,
"다시 쓴다" 고 적은 것이 실제로 도메인을 드는가, 분류에 적힌 파일이 실재하는가.

⚠️ **이 가드를 만들며 내 분류가 네 번 틀렸다** — `FindingKey` 를 못 준다고 적었고,
정규식이 `coupon_id`(낱말 경계)와 `CouponStateMachine`(대소문자)을 못 잡았고,
타입 참조를 안 봐서 위 오류가 났고, 세 번째 시험이 자기 이름으로 통과하는
항진명제였다. **네 번 다 시험이 잡았다.**

---

## 2. 이음매 하나 — `FailureSummary` 의 접두어 목록

`core/batch` 의 유일한 결합이다. `EXIT_MESSAGE` 에서 **도메인 에러 코드를 지우는** 정규식이
접두어를 <b>목록으로</b> 갖고 있다(`COUPON`·`COUPON_ROUND`·`ADMIN-COUPON-ROUND`…).

**일반 패턴(`[A-Z][A-Z_-]*-\d{3}`)으로 못 바꾼다.** 그러면 `SHA-256`·`ISO-8601` 같은 것을
같이 지운다. 목록인 것이 <b>의도</b>다.

**그래서 주제가 바뀌면 이 목록에 새 접두어를 넣어야 한다.** 잊어도 조용히 새지는 않는다 —
`FailureSummaryPrefixContractTest` 가 <b>소스에서 에러 코드를 세어</b> 목록과 대조하고,
빠지면 그 자리에서 깨진다. **이음매를 없애는 것보다 이음매를 시끄럽게 만드는 쪽을 골랐다.**

---

## 3. 이름만 바꾸면 되는 것 vs 다시 써야 하는 것

`infra/mq` 의 14개가 여기 섞여 있다. 갈라 두지 않으면 전부 다시 쓰는 줄 안다.

| | 무엇 | 어떻게 |
|---|---|---|
| **이름** | `KafkaTopicConfig` 의 토픽 상수 (`coupon.notify` 등) | 상수만 바꾼다. 파티션·복제·보존 정책은 그대로 |
| **이름** | `KafkaConsumerGroups` 의 그룹 id | 같이 바꾼다. **offset 정책 표(`earliest`/`latest`)는 그대로** — 그 판단은 "유실이 보이나" 축이지 도메인이 아니다 |
| **이름** | `PartitionKeys` | 키로 쓰는 필드만 바뀐다(`memberId` → 새 주체) |
| **구조** | `NotificationRequestedEvent` 의 `couponId` | 아래 §4 |

---

## 4. 다시 쓰는 것 — 알림이 **무엇에 대한** 것인가

`Notification` 이 `couponId`·`issuanceId` 를 든다. 사전예약이면 그 자리가
`productId`·`reservationId` 가 된다. 이벤트·엔티티·스키마·조회가 함께 움직인다.

**일부러 일반화하지 않았다.** `subjectType` + `subjectId` 로 바꾸면 두 가지를 잃는다.

- **DB 가 못 지킨다.** 지금은 `uk_notifications_issuance_channel` 이 *"한 발급에 채널당
  알림 하나"* 를 <b>스키마로</b> 강제한다. 다형 키로는 그 제약을 못 건다.
- **집계 축이 사라진다.** `ix_notifications_coupon_status` 로 회차별 집계를 하는데,
  다형 키에서는 그 인덱스가 의미를 잃는다.

**중복 알림을 막는 것이 이 서브시스템의 존재 이유**라, 그것을 애플리케이션 코드로 내리는
대가가 재사용성보다 크다. 주제가 바뀔 때 **이 네 파일을 다시 쓰는 것이 맞다** —
`Notification` · `NotificationEntity` · `NotificationRequestedEvent` · 스키마.

> 사용자가 처음에 적어 둔 전제와 같다 — *"물론 비슷한 프로젝트고 나중에 바뀌는 거에 따라
> 바뀌겠지만."* **바뀌어야 하는 것과 안 바뀌어도 되는 것을 가르는 것**이 이 문서의 일이다.

---

## 옮길 때 순서

1. **인프라를 그대로 들고 온다** — 관제·아웃박스·가드·알림 규칙. 고칠 것이 없다.
2. **이름을 바꾼다** — 토픽·그룹 id·파티션 키. 정책 값은 안 건드린다.
3. **`FailureSummary` 접두어를 넣는다.** 안 넣으면 계약 테스트가 잡는다.
4. **알림 주체를 새로 쓴다**(§4). 유일 키와 집계 인덱스를 **먼저** 정하고 그 위에 코드를 얹는다.
5. **숫자를 다시 잰다.** `docs/18` 의 하네스 절차를 그대로 쓰되 표는 새로 만든다 —
   절벽 위치가 그 기계의 **커넥션 풀 크기**에 달렸다(CY-923).
