# 17. 게이트를 실제로 통과시킨다 (CY-601)

이 문서는 설계가 아니라 **기록**이다. D5·D10·결정론을 *"통과할 것이다"* 로 두지 않고
한 번 끝까지 돌려서 나온 값을 적는다.

**왜 필요했나.** 지금까지 검증기는 다 서 있었는데(`verifyJob`·`expireJob`·`cleanupJob`·
스케줄러·리포트 API) **이 저장소가 검증할 데이터를 한 번도 만든 적이 없었다.** 그래서
`docs/15` 가 *"성공 경로를 살아 있는 배치로 못 쟀다"* 를 구멍으로 남겨 뒀다.

---

## 판정

| 게이트 | 기준 | 결과 |
|---|---|---|
| **D5** | 정상셋 검출 0건 | **0건** · `verdict=PASS` · 여섯 규칙 전부 0 |
| **D10** | 오염셋 양방향 집합 일치 | 검출 **800** · 누락 **0** · 오탐 **0** · `matches=true` |
| **결정론** | 같은 `asOf` 재실행 시 지문·checksum 일치 | 같은 `asOf`(`2026-08-26 05:00`)로 CLEAN `attempt` 1·2·3·4·**5**, CORRUPT 1·2·3 — `verification_runs` **7행·4행**이 **지문 1개·checksum 1개로 수렴**. CY-649 가 CLEAN `attempt 5`(run6)를 더했다 |

> **CLEAN 의 checksum 일치는 결정론 근거로 약하다.** 그것은 빈 입력끼리의 해시라
> (`e3b0c442…` = 빈 문자열의 SHA-256), **검증기가 아무 규칙도 안 돌려도 같은 값이 나온다.**
> CLEAN 쪽 결정론의 실질 근거는 `dataset_fingerprint` 재실행 일치와 규칙별 0건이고,
> *"규칙이 실제로 잡을 수 있는가"* 는 아래 "여섯째 규칙" 절이 따로 증명한다.

`asOf = 2026-08-26 05:00:00` (UTC).

**`asOf` 는 필수 쿼리 파라미터다**(`VerifyTriggerController:155` — 거기만 `required = false` 가 없다).
빼면 기본값이 들어가는 게 아니라 **400** 이다. 그 값이 곧 결정론의 기준점이라 그렇게 설계됐다.

---

## 교차 검증 — 이것이 이 실행의 핵심이다

시드 생성기(`cy-seed`, Python)와 검증 배치(`verifyJob`, Java)는 **독립 구현**이다.
같은 스키마 위에서 둘을 돌려 나온 값이 같으면, 우연히 같을 가능성이 낮다.

Python 쪽은 시드가 `verification_runs` 에 `origin='SEED'` 로 남긴 행이고, Java 쪽은
`origin='BATCH'` 행이다. 둘 다 아래 SQL 로 그대로 다시 볼 수 있다.

```sql
SELECT origin, dataset_fingerprint, findings_checksum
  FROM coupon_clean.verification_runs WHERE scope='FULL' ORDER BY id;
```

| | Python `seedgen` (`origin=SEED`) | Java `verifyJob` (`origin=BATCH`) |
|---|---|---|
| `dataset_fingerprint` (CLEAN) | `b20acfb867fe5d635bf4e7b180c213909ba7d61413efea3cd0c2f621cd931c3c` | 같은 값 |
| `dataset_fingerprint` (CORRUPT) | `f42c39759378d50f6dfab0800ef65349ab323e865ae6ae272c93b93e29f95d90` | 같은 값 |
| `findings_checksum` (CLEAN) | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | 같은 값 |
| `findings_checksum` (CORRUPT) | `27871a38f0d0128355c9bbb15b51086383eaca11ba71613561d359fb6db133da` | 같은 값 |
| 규칙별 검출 (CORRUPT) | `STOCK 200 · DUP 200 · REPLAY 100 · ILLEGAL 200 · USAGE 100` | 전부 같음 |

커밋된 증적에도 같은 값이 들어 있다 — `verify/2026-08-26-corrupt-full-run3.json` 의
`.data.run.datasetFingerprint` · `.data.run.findingsChecksum`.

**checksum 이 같다는 것이 집합이 같다는 것보다 세다.** 그것은 정렬된
`(finding_type, target_key)` 전체를 한 값으로 접은 것이라, 한 건이라도 다르면 달라진다.

CLEAN 의 checksum `e3b0c442…852b855` 는 **빈 입력의 SHA-256** 이다 — 검출 0건과 일관된다.

> **`verdict` 는 `origin` 에 따라 뜻이 다르다.** CORRUPT 실행에서 시드는 `FAIL`,
> 배치는 `PASS` 를 남긴다. 시드의 `FAIL` 은 *"이 데이터셋은 오염돼 있다"* 이고,
> 배치의 `PASS` 는 *"검증기가 심은 집합을 정확히 재현했다"* 다(`judgeAgainstManifest`).
> 같은 컬럼의 값을 나란히 놓고 읽으면 모순처럼 보이므로 여기 적어 둔다.

---

## 실측 수치

### 시드 적재

| | CLEAN | CORRUPT |
|---|---|---|
| 생성·적재 (제약 제외) | 127.0초 | 25.7초 |
| ＋ 제약 생성(UNIQUE·FK·CHECK) | 43초 | 7초 |
| **＝ 합계** | **170초** | **33초** |
| 회원 | 1,000,000 | 200,000 |
| 발급건 | 3,000,000 | 600,200 |
| 이력 | 5,339,616 | 1,069,320 |
| 사용 | 1,319,808 | 264,560 |
| 회차 | 147 | 291 |
| 정답(`expected_findings`) | — | **800** |

행수는 **적재된 전수**다 — `asOf` 로 자르기 전 값이라 시드 로그의 숫자와 같다.
검증이 보는 것은 `created_at <= asOf` 로 자른 뒤이므로 이보다 작을 수 있다.

회차 291개는 **12브랜드 × 24개월 = 288 에 현재 회차 3 을 더한 값**이다(`docs/11` §의 산식).
24개월인 이유가 따로 있다 — 유형 1과 3이 회차 그레인 키를 **서로소 슬롯**으로 100개씩
나눠 써야 해서 200개가 필요하다. **두 숫자의 기준을 섞지 말 것.**

주입 700건에 정답 800행인 것은 유형 3이 규칙 둘을 동시에 울리기 때문이다.

### 검증 소요

| 데이터셋 | attempt | 소요 |
|---|---|---|
| CLEAN | 3 | **116초** |
| CLEAN | 4 | **110초** |
| CORRUPT | 2 | 14초 |
| CORRUPT | 3 | 14초 |

**보조 인덱스는 만들지 않은 상태**의 수치다(`ddl/90_perf_indexes_optional.sql` 미적용).

### Step 별로 갈랐다 — 짐작이 두 번 다 틀렸다

CLEAN 116초를 `BATCH_STEP_EXECUTION` 으로 갈라 보니, 처음에 적었던 원인 둘이 **다 틀렸다.**

```sql
SELECT STEP_NAME, READ_COUNT, TIMESTAMPDIFF(SECOND, START_TIME, END_TIME)
  FROM coupon_clean.BATCH_STEP_EXECUTION WHERE JOB_EXECUTION_ID = 2
 ORDER BY STEP_EXECUTION_ID;
```

| Step | CLEAN | 비중 | CORRUPT |
|---|---|---|---|
| `usageCountStep` | **56초** | **48%** | 1초 |
| `replayStep` | 36초 | 31% | 8초 |
| `statsAggregateStep` | 20초 | 17% | **0초 (SKIPPED)** |
| `duplicateIssuanceStep` | 13초 | 11% | 2초 |
| 나머지 일곱 Step 합계 | 7초 | 6% | 0초 |

**한때 여기 "리플레이가 이력 534만 행을 접는다" 고 적었는데, 리플레이는 1위가 아니다.**
그리고 아래 "남은 것" 에는 *"`asof_state` 를 안 시딩해서 Step 0 이 매번 다시 만든다 —
116초의 상당 부분이 그것"* 이라고 적었는데, Step 0 가 곧 `replayStep` 이고 그것도 31% 다.
**둘 다 관측 없이 짚은 원인이었다.**

**8.3배가 5배(행수 비)보다 큰 것도 이걸로 설명된다** — `statsAggregateStep` 20초는
CORRUPT 가 아예 안 돈다(오염 데이터 위의 집계는 뜻이 없어 `SKIPPED`). 그 20초를 빼면
96 / 14 = 6.9배로, 행수 비에 가까워진다.

~~**인덱스를 넣을 거면 `usageCountStep` 부터 본다.**~~ ⚠️ **CY-767 에서 재 보니 이 Step 의
지배 요인은 인덱스가 아니라 버퍼 풀이었다** — 128 MiB 53~55초 vs 2 GiB 4.5초(12배).
같은 `UPDATE` 를 단독으로 돌렸을 때 `ROW_COUNT()` 가 0인데도 46.5초가 걸려, 쓰기가 아니라
**읽기 측 페이지 미스**임이 직접 확인됐다. 근거와 재현법은 `docs/12` §usageCountStep 에 있다.

여기 적은 **56초·48%** 는 인덱스 미적용 상태의 116초짜리 실행에서 잰 옛 값이라
`docs/12` 의 53.6~55.1초·30%(180.8초 실행)와 모수가 다르다 — 섞어 쓰면 안 된다.

`READ_COUNT` 가 0인 것은 그대로 유효한 단서다 — 청크 리더가 아니라 집계 SQL 한 방이라는
뜻이라, 청크 크기를 만져도 안 줄어든다.

---

## 절차에서 실제로 걸린 것

`docs/14` 에 절차가 적혀 있었지만, 그대로 따라가면 걸리는 자리가 셋 있었다.

### 1. `local_infile` 이 꺼져 있다

시드는 `LOAD DATA LOCAL INFILE` 로 넣는다. 서버 쪽 스위치가 꺼져 있으면
`(3948, 'Loading local data is disabled…')` 로 죽는다. `docs/14` 에 없던 단계다.

```sql
SET GLOBAL local_infile = 1;   -- 재시작하면 풀린다. compose 에 박지 않았다
```

### 2. `app` 계정에 검증 스키마 권한이 없다

`base.yml` 의 `MYSQL_USER` 는 `MYSQL_DATABASE` 하나에만 권한을 받는다.
`DB_NAME=coupon_clean` 으로 배치를 띄우면 접속 자체가 안 된다.

```sql
-- ALL PRIVILEGES 를 주지 않는다. 배치는 Flyway 가 꺼져 있고 DDL 을 안 친다 —
-- TRUNCATE 도 없다(asof_state 는 DELETE … LIMIT 로 걷는다). **실측했다**: 이 넷만 준
-- 계정으로 검증이 끝까지 돌고(801건 · PASS) 권한 거부 로그가 0이었다.
GRANT SELECT, INSERT, UPDATE, DELETE ON coupon_clean.*   TO 'app'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON coupon_corrupt.* TO 'app'@'%';
FLUSH PRIVILEGES;
```

### 3. 시드가 `attempt` 를 점유한다

`startRunStep` 이 같은 파라미터의 실행을 거절한다. 시드가 심어 둔 기준 실행이
**CLEAN 은 `attempt` 1·2, CORRUPT 는 1** 을 차지하므로 그다음 번호부터 써야 한다.
가드가 그 사실을 메시지에 적어 준다 — 설계대로 동작했다.

---

## 덤프 스크립트의 결함 — 살아 있는 것에 대고서야 드러났다

`scripts/dump-verify-report.sh` 는 `CLEAN` 과 `CORRUPT` 를 **둘 다 성공해야** 옮기게
돼 있었다. 그런데 배치는 `DB_NAME` 으로 스키마를 **하나만** 잡는다 —
`coupon_clean` 을 보는 기동에서 `?dataset=CORRUPT` 를 물으면 404 다.

**그래서 실제 배포에서는 영원히 아무것도 못 올렸다.**

고정 응답으로 한 테스트는 이것을 못 봤다. 가짜 응답이 `dataset` 과 무관하게 같은 본문을
줬기 때문이다 — **그 축이 테스트에 아예 없었다.**

**첫 수정이 또 틀렸다.** 404 를 *"이 기동이 안 보는 데이터셋"* 으로 읽어 건너뛰게 했는데,
그 해석에 근거가 없다 — 이 API 는 자기가 어느 데이터셋을 서비스하는지 **모른다.**
404(`RUN_NOT_FOUND`)는 현재 스키마에서 그 조합의 **닫힌 실행을 못 찾았다**는 뜻이고,
원인이 최소 셋이다: ⑴ 다른 스키마에 있다 ⑵ 시드 재주입으로 판정이 사라졌다
⑶ 오늘 검증이 아직 안 끝났거나 실패했다. **⑵·⑶ 을 건너뛰면 위증 방지가 통째로 무너진다.**

지금은 `REPORT_DATASETS` 로 **이 기동이 서비스하는 것을 선언**하게 하고 기본값을 없앴다.
선언한 것은 전부 200 이어야 하고, 없는 것은 애초에 목록에 안 들어온다. 404 는 다시 고장이다.

두 판정을 다 남기려면 **스키마를 바꿔 기동하고 다시 돌린다.**

⚠️ **아래 네 줄은 실행 절차가 아니다.** 스키마 축이 갈린다는 것만 보이는 요약이고 둘이 빠져 있다 —
**기동 대기**(`batch.yml` 에 healthcheck 가 없어 `up -d` 는 앱이 뜨기 전에 반환한다. 그 사이
`curl` 은 `HTTP 000` 이다)와 **검증 트리거**(덤프는 `/reports/latest` 를 **읽기만** 한다.
새 판정을 안 만드니 직전 판정의 `asOf` 나이에 그대로 걸린다). 실행 가능한 형태는
아래 「다시 돌리는 법」 §5 의 `gate()` 다.

```bash
# --no-deps 를 빼면 mysql 까지 재생성된다 — 아래 "--force-recreate" 절
# REPORT_DATASETS 는 **이 기동이 보는 것**을 선언한다. 기본값이 없다 — 아래 참고.
# REPORT_PUSH=1 이 없으면 커밋까지만 하고 로컬에 남는다.
DB_NAME=coupon_clean   docker compose -f base.yml -f batch.yml up -d --force-recreate --no-deps batch
REPORT_DATASETS=CLEAN   REPORT_PUSH=1 bash scripts/dump-verify-report.sh
DB_NAME=coupon_corrupt docker compose -f base.yml -f batch.yml up -d --force-recreate --no-deps batch
REPORT_DATASETS=CORRUPT REPORT_PUSH=1 bash scripts/dump-verify-report.sh
```

⚠️ **끝나면 `coupon_clean` 으로 되돌려 둔다.** 마지막 줄이 CORRUPT 라 그대로 두면 다음 사람이
CORRUPT 스키마를 보는 기동에서 시작한다 — `?dataset=CLEAN` 이 404 다.
**변수 없이 그냥 재기동하면 안 된다** — `batch.yml` 이 `DB_NAME: ${DB_NAME:-app}` 이라
`app` 스키마를 본다.

```bash
DB_NAME=coupon_clean docker compose -f base.yml -f batch.yml up -d --force-recreate --no-deps batch
```

결과는 [`reports` 브랜치의 `verify/`](https://github.com/coupon-yaho/cy-be/tree/reports/verify) 에 쌓인다.

| 파일 | 크기 | `asOf` | `attempt` |
|---|---|---|---|
| `verify/2026-08-26-clean-full-run5.json` | 826 B | `2026-08-26T05:00:00` | 4 |
| `verify/2026-08-26-corrupt-full-run3.json` | 1132 B | `2026-08-26T05:00:00` | 3 |
| `verify/2026-08-27-clean-full-run7.json` | 825 B | `2026-08-27T01:17:00` | 1 |
| `verify/2026-08-27-corrupt-full-run4.json` | 1131 B | `2026-08-27T01:28:00` | 1 |

**`asOf` 가 둘로 갈려 있다.** 위 두 개는 이 문서가 못 박은 `2026-08-26 05:00:00` 이고,
아래 두 개는 CY-649 가 **실행 시각으로 다시 돌린 것**이다 — 고정 `asOf` 로는 신선도 검사에
막힌다(아래 절).

**`attempt` 가 1인 것이 정상이다.** `nextAttempt` 는 `as_of` 로 스코프되므로(`uk_run_params`
가 `(as_of, dataset, scope, attempt)`) **`asOf` 가 바뀌면 번호가 1부터 다시 시작한다.**
시드가 CLEAN 1·2 를 점유한다는 서술은 **그 `asOf` 안에서만** 참이다.

**뒤 두 파일이 앞 두 개보다 1바이트 작다.** Jackson 이 나노초 뒤 0 을 떨어뜨려서다.
**다만 줄어든 필드가 서로 다르다** — `run7` 은 `finishedAt` 이 `01:19:13.28407`(5자리)이고,
`corrupt-run4` 는 반대로 `startedAt` 이 `01:28:49.34094`(5자리)다. 각 파일에서 나머지 한
필드는 6자리 그대로다. 자리수가 줄어드는 자리는 **그때 찍힌 값 나름**이라 필드로 고정돼
있지 않다 — 크기 차이를 확인할 때 한 필드만 세면 어긋난다.

> ⚠️ 여기 한때 `verify/2026-08-26-clean-full-run4.json 825 B` 라고 적혀 있었다.
> **그런 파일은 `reports` 브랜치 어느 커밋에도 존재한 적이 없다.** 뜬 것은 `run5` 다.
>
> `run5` 다음이 `run7` 인 것도 설명이 필요하다. **`run6` 은 파일이 없다** —
> 같은 `asOf`(`2026-08-26 05:00`)에 `attempt 5` 로 돌린 실행인데, 덤프가
> **신선도 검사에 막혀** 아무것도 안 남겼다. 아래 절이 그 자리다.

### `REPORT_PUSH=1` 이 처음 끝까지 간 것은 CY-649 다

그전까지 이 스크립트는 **커밋까지만 돌아 봤다.** 락·신선도 검사·지문 동일성 검사까지
공들여 넣어 두고 **푸시 경로만 실측이 없었다** — 원격 `reports` 브랜치가 아예 없었고,
문서 네 곳이 그 링크를 약속하는데 아무도 못 보는 상태였다.

**가는 길에 신선도 검사가 실제로 한 번 막았다.**

```
✗ CLEAN FULL 의 asOf 이 73019초 됐다(허용 21600초): 2026-08-26T05:00:00
```

막힌 것은 **`run6`** 이다 — 같은 `asOf`(`2026-08-26 05:00`)에 `attempt 5` 로 돌린 실행이다.
**판정 자체는 43초 전에 났다**(`finishedAt 2026-08-27T01:16:16Z`, 거절 시각 `01:16:59Z`).
그런데 그 판정이 보는 **데이터 스냅샷**은 73019초(20시간) 전이었다.

**축을 둘로 나눠 둔 것(`finishedAt`·`asOf`)이 여기서 갈렸다.** `finishedAt` 만 봤으면
43초짜리 신선한 리포트로 통과했을 것이고, *"오늘 뜬 리포트"* 라는 이름으로 **어제 데이터**가
올라갔다. 그래서 `run6` 은 `reports` 브랜치에 파일이 없고, 18초 뒤 새 `asOf` 로 다시 돌린
`run7` 이 올라갔다.

**worktree 갈림은 이 티켓이 안 탔다.** 스크립트는 ① 로컬 브랜치(`:193`) ② `origin/reports`
추적(`:196`) ③ orphan 생성(`:198`) 순으로 고르는데, `../cy-be-reports` 가 이미 있어서
그 앞의 *"이미 있다"* 분기로 들어갔다.

②는 갓 클론한 저장소에서 따로 확인했다 — `git worktree add --track -b reports … origin/reports`
성공. 다만 **`origin/reports` 를 한 번도 fetch 한 적 없는 오래된 클론**에서는 `ls-remote` 만
통과하고 그 명령이 `fatal: 잘못된 레퍼런스: origin/reports` 로 exit 128 이다. 스크립트가
`|| exit 1` 이라 `fail()` 메시지 없이 raw git 에러만 남는다 — `:196` 앞에
`git fetch -q origin "$BRANCH"` 를 넣어야 메워진다. **아직 안 넣었다.**

---

## 여섯째 규칙 — `GRADE_VIOLATION` 도 검출을 증명했다

기본 오염셋은 `GRADE_VIOLATION` 을 **0건**으로 둔다(`--plant-v6` 가 off). 그래서 위 800행
판정만 보면 규칙 여섯 중 하나는 *"잡아야 할 것을 잡는가"* 를 한 번도 안 보인 셈이다 —
규칙이 돌긴 한다는 것은 CLEAN 0건이 보여 주지만, **0건은 "안 잡았다" 와 "잡을 게 없었다"
를 못 가른다.**

그래서 별도 스키마(`coupon_v6`)에 `--plant-v6` 로 한 번 더 심어 돌렸다. 이미 커밋한
증적은 안 건드리려고 스키마를 갈랐다.

| | |
|---|---|
| 정답 | **801** (기본 800 + 등급 위반 1) |
| 검출 | **801** |
| `GRADE_VIOLATION` | **1** |
| 누락 · 오탐 | **0 · 0** · `matches=true` |

**이제 여섯 규칙이 전부 한 번씩 참이 되어 봤다.**

재현은 "다시 돌리는 법" 6번 블록에 있다.

---

## 절차에서 또 하나 — `--force-recreate` 는 의존 서비스까지 다시 만든다

배치를 다른 스키마로 옮기려고

```bash
DB_NAME=coupon_clean docker compose -f base.yml -f batch.yml up -d --force-recreate batch
```

를 돌렸더니 **`mysql` 컨테이너까지 재생성됐다.** `SET GLOBAL local_infile=1` 이 조용히
풀려 다음 시드 적재가 죽었고, 원인이 안 보였다.

**처음에는 원인을 잘못 짚었다.** `base.yml` 의 `MYSQL_DATABASE` 가 `${DB_NAME:-app}` 이라
같은 변수를 쓰는 것이 원인인 줄 알고 배치 전용 변수를 만들었는데, **그걸로는 안 고쳐졌다.**
가려서 재 보니 원인은 변수가 아니라 `--force-recreate` 가 `depends_on` 을 타고 번지는 것이었다.

| 명령 | mysql |
|---|---|
| `DB_NAME=… up -d --force-recreate batch` | **재생성됨** |
| `BATCH_DB_NAME=… up -d --force-recreate batch` | **재생성됨** (변수는 원인이 아니다) |
| `… up -d --force-recreate --no-deps batch` | **그대로** |

**`--no-deps` 를 붙인다.** 볼륨이 있어서 이번엔 데이터가 살았지만, 없는 환경이면
데이터셋이 통째로 날아가는 자리다.

---

## 다시 돌리는 법

**이 블록만 따라 하면 끝나야 한다.** 위 "절차에서 걸린 것" 을 다시 찾아 읽게 만들면
그건 절차가 아니다 — 한때 `local_infile` 과 `GRANT` 가 이 블록 밖에만 있어서 두 번 죽었다.

> ⚠️ **파일로 저장해서 `bash gate.sh` 로 돌린다. 터미널에 붙여 넣지 마라.**
> 두 가지가 스크립트를 전제한다. ⑴ `|| exit 1` — 대화형 셸에서는 **터미널이 닫힌다.**
> ⑵ 아래 `trap ... EXIT` — 대화형 셸에서는 블록이 끝나도 안 돌고 셸에 남는다.
> 스크립트로 돌리면 둘 다 의도대로다. 실측:
>
> | | 결과 |
> |---|---|
> | `\|\| exit 1` 로 죽을 때 EXIT trap | **돈다** (종료코드 1) |
> | Ctrl-C(SIGINT) 로 끊을 때 EXIT trap | **돈다** (종료코드 130) |
>
> 그래서 `INT`·`TERM` 을 따로 걸지 않았고, 마지막 게이트 뒤에 복구를 손으로
> 부르지도 않았다 — **§6 이 §5 뒤에 `coupon_v6` 로 재기동하므로** 거기서 trap 을
> 풀면 그 복구를 놓친다. 복구는 스크립트가 어떻게 끝나든 마지막에 한 번 돈다.

```bash
# ── 0. 서버 스위치와 권한 ────────────────────────────────────────────────
# ⚠️ mysql 컨테이너를 다시 만들면 local_infile 은 **다시 풀린다**. 그때 여기로 돌아온다.
cd ~/URECA/comprehensiveProject/cy-be
docker compose -f base.yml exec -T -e MYSQL_PWD=root mysql mysql -uroot -e "
  SET GLOBAL local_infile = 1;
  CREATE DATABASE IF NOT EXISTS coupon_clean;
  CREATE DATABASE IF NOT EXISTS coupon_corrupt;
  GRANT SELECT, INSERT, UPDATE, DELETE ON coupon_clean.*   TO 'app'@'%';
  GRANT SELECT, INSERT, UPDATE, DELETE ON coupon_corrupt.* TO 'app'@'%';
  FLUSH PRIVILEGES;"

# ── 1. 키. .env 는 gitignore 대상이다 — 두 저장소 다 PUBLIC 이다 ──────────
cd ../cy-seed
python3 -m venv .venv && ./.venv/bin/pip install -q cryptography PyMySQL Faker
printf 'AES_KEY=%s\nHMAC_KEY=%s\nSEED_DSN=mysql://root:root@127.0.0.1:3306/\n' \
  "$(openssl rand -base64 32)" "$(openssl rand -base64 32)" > .env && chmod 600 .env
set -a; . ./.env; set +a

# ── 2. 스모크. 30초면 끝난다 — 여기서 걸리면 본 실행도 걸린다 ────────────
./.venv/bin/python bin/seed.py all      --dataset clean --scale 0.002 --schema seed_smoke
./.venv/bin/python bin/seed.py verify   --dataset clean --schema seed_smoke
./.venv/bin/python bin/seed.py teardown --schema seed_smoke

# ── 3. 본 실행. **as-of 를 고정해야 결정론을 잴 수 있다** ────────────────
# 두 형식이 필요하다. 시드는 공백, API 는 T 다(오프셋은 아예 안 받는다).
# ⚠️ **고정값을 쓰면 덤프가 신선도 검사에 막힌다.** 스크립트는 `asOf` 나이를
#    REPORT_MAX_AGE(기본 21600초)로 재는데, 2026-08-26 05:00 UTC 는 이미 지났다.
#    CY-649 에서 실제로 그렇게 죽었다(run6). 그래서 실행 시각을 쓴다.
#
#    **옛 asOf 를 그대로 재현하려면 덤프를 돌리지 마라.** 신선도 검사는
#    "이게 오늘 판정인가" 를 묻는 것이고, 과거 재현은 애초에 그 답이 아니다.
#    결정론 확인은 리포트 파일이 아니라 verification_runs 조회로 한다(맨 위 표).
#    굳이 덤프까지 돌리려면 **나이를 계산해서** 넘긴다 — 고정 숫자를 적어 두면
#    그 숫자를 넘기는 날 또 막힌다(999999 는 11.6일이라 2026-09-07 이면 만료다).
#      AGE=$(python3 -c "import datetime as d;u=d.timezone.utc
#            print(int((d.datetime.now(u)
#            - d.datetime.fromisoformat('$ASOF').replace(tzinfo=u)).total_seconds())+600)")
#      REPORT_MAX_AGE="$AGE" gate ...
#    (python3 로 재는 이유는 이식성이다. `date -u -jf` 는 BSD 전용이라 리눅스
#     러너에서 `date: unrecognized option: j` 로 죽는다 — alpine 으로 확인했다.
#     gate() 가 이미 python3 로 JSON 을 파싱하므로 새 의존성이 아니다.
#     `datetime.UTC` 가 아니라 `timezone.utc` 인 것도 이식성이다 — 그 별칭은
#     3.11 부터라 3.10 에서 AttributeError 다. 3.10·3.14 양쪽에서 확인했다.)
ASOF="$(date -u +'%Y-%m-%d %H:%M:00')"
ASOF_API="${ASOF/ /T}"
./.venv/bin/python bin/seed.py all --dataset clean   --schema coupon_clean   --as-of "$ASOF"
./.venv/bin/python bin/seed.py all --dataset corrupt --schema coupon_corrupt --as-of "$ASOF"

# ── 4. 배치 메타를 두 스키마에 붓는다 ────────────────────────────────────
# 시드의 ddl/ 에는 BATCH_* 가 하나도 없다. 인덱스 둘을 빼먹으면 **기동이 거절된다**
# (CY-686). 끄고 띄우면(batch.schema-guard.require-batch-indexes=false) 게이지가 NaN 이
# 되거나 정리 잡이 매 청크 전체 스캔이 된다 — docs/14.
cd ../cy-be
for S in coupon_clean coupon_corrupt; do
  for F in V11__batch_metadata.sql \
           V2026082513__ix_batch_job_execution_lookup.sql \
           V2026082514__ix_batch_job_execution_history.sql; do
    docker compose -f base.yml exec -T -e MYSQL_PWD=root mysql mysql -uroot "$S" \
      < storage/src/main/resources/db/migration/$F
  done
done

# ── 5. 데이터셋 하나씩. 배치는 스키마를 **하나만** 본다 ──────────────────
# ⚠️ --no-deps 를 빼면 mysql 까지 재생성돼 0번으로 돌아간다.
# ⚠️ BATCH_SCHEDULING_ENABLED=false. 만료는 재고를 쓰는 유일한 잡이라, 켜진 채
#    CORRUPT 셋을 보면 심어 둔 오염을 건드려 되돌릴 방법이 없다.
gate() {                      # $1=스키마  $2=데이터셋  $3=attempt  $4=seedRunId(CORRUPT만)
  DB_NAME="$1" BATCH_SCHEDULING_ENABLED=false \
    docker compose -f base.yml -f batch.yml up -d --force-recreate --no-deps batch
  # **제한 시간을 둔다.** 배치가 기동에 실패하면 로그 문자열이 영영 안 나와
  # 이 루프가 무한히 돈다 — 무인으로 돌릴 때 그것이 제일 나쁘다.
  local n=0
  until docker compose -f base.yml -f batch.yml logs batch 2>&1 \
        | grep -q "스키마 확인 완료.*$1"; do
    n=$((n+1)); [ "$n" -gt 60 ] && {
      echo "배치가 120초 안에 안 떴다. 로그를 봐라:" >&2
      docker compose -f base.yml -f batch.yml logs --tail=30 batch >&2; return 1; }
    sleep 2
  done

  # **쿼리 파라미터다** — 본문 JSON 은 400 이다. 시드가 attempt 를 점유한다:
  # CLEAN 1·2, CORRUPT 1. 그다음 번호부터 쓴다.
  # CORRUPT 에는 seedRunId 가 **필수**다(VerifyTriggerController 가 400 으로 막는다).
  local q="asOf=${ASOF_API}&dataset=$2&scope=FULL&attempt=$3"
  [ -n "${4:-}" ] && q="$q&seedRunId=$4"
  local ex
  ex=$(docker compose -f base.yml -f batch.yml exec -T batch \
        curl -sS -X POST "http://127.0.0.1:9091/api/v1/admin/verify?$q" \
       | python3 -c "import sys,json;print(json.load(sys.stdin)['data']['executionId'])")

  # **202 비동기다.** 끝나기 전에 덤프를 돌리면 finishedAt 이 비어 있어 못 뜬다.
  # CLEAN 이 116초다. 20분이면 넉넉하고, 그 이상이면 뭔가 잘못된 것이다.
  n=0
  until docker compose -f base.yml -f batch.yml exec -T batch \
        curl -s "http://127.0.0.1:9091/api/v1/admin/verify/runs/$ex" \
        | grep -qE '"status":"(COMPLETED|FAILED|STOPPED|ABANDONED)"'; do
    n=$((n+1)); [ "$n" -gt 120 ] && {
      echo "검증이 20분 안에 안 끝났다. 마지막 응답:" >&2
      docker compose -f base.yml -f batch.yml exec -T batch \
        curl -s "http://127.0.0.1:9091/api/v1/admin/verify/runs/$ex" >&2; return 1; }
    sleep 10
  done

  # **종료 상태를 봐야 한다.** 위 루프는 FAILED·STOPPED·ABANDONED 에도 빠져나온다.
  # 그대로 덤프를 돌리면 /reports/latest 가 **직전 성공 실행**을 돌려주고 — 그 조회에는
  # 시각 하한이 없다 — 이번 실행이 죽었는데 게이트가 초록으로 끝난다.
  # REPORT_PUSH=1 이면 그 낡은 판정이 **원격에 공개**된다. 게이트가 거짓말하는 자리다.
  local st
  st=$(docker compose -f base.yml -f batch.yml exec -T batch \
        curl -s "http://127.0.0.1:9091/api/v1/admin/verify/runs/$ex" \
       | python3 -c "import sys,json;print(json.load(sys.stdin)['data']['status'])")
  [ "$st" = "COMPLETED" ] || {
    echo "검증이 $st 로 끝났다($2 attempt $3). 덤프를 돌리지 않는다." >&2; return 1; }

  # 원격까지 밀려면 이 스크립트를 REPORT_PUSH=1 로 실행한다. 기본은 로컬 커밋까지다.
  REPORT_DATASETS="$2" REPORT_PUSH="${REPORT_PUSH:-0}" bash scripts/dump-verify-report.sh
}

# seedRunId 는 **expected_findings 가 실제로 키로 쓰는 값**이다.
# verification_runs 에서 뽑으면 안 된다 — 그 둘이 같다는 보장이 없고, 시드가 여러
# 실행(FULL·INCREMENTAL)을 심으면 어느 것을 골랐는지도 모른다.
# 배치는 `expected.exists(seedRunId)` 만 본다(VerifyJobConfig.validateSeedRunId).
# **존재만 확인하므로 틀린 번호를 주면 낡은 묶음과 조용히 대조한다** — 그 javadoc 이
# "누락 800 · 오탐 800 으로 나타나 규칙을 의심하게 만든다" 고 적어 뒀다.
seed_run_of() {   # $1 = 스키마
  local v
  v=$(docker compose -f base.yml exec -T -e MYSQL_PWD=root mysql \
      mysql -uroot -N -e "SELECT DISTINCT seed_run_id FROM $1.expected_findings;" | tr -d '\r')
  # **하나가 아니면 멈춘다.** 둘 이상이면 사람이 골라야 하고, 없으면 주입을 안 돌린 것이다.
  if [ "$(printf '%s\n' "$v" | grep -c .)" -ne 1 ]; then
    echo "seed_run_id 가 정확히 하나가 아니다: [$v]" >&2; return 1
  fi
  printf '%s' "$v"
}
SEED_RUN=$(seed_run_of coupon_corrupt) || exit 1

# **복구를 trap 에 건다.** 아래 게이트들이 CORRUPT 기동으로 끝나고, 게다가
# `|| exit 1` 로 중간에 죽거나 Ctrl-C 로 끊으면 **그 자리에서 멈춘다.**
# 그대로 두면 다음 사람이 CORRUPT 스키마를 보는 기동에서 시작해 ?dataset=CLEAN 이
# 404 다. 명령을 마지막 줄에 적으면 정상 완주할 때만 도니 trap 이어야 한다.
# 변수 없이 재기동하면 batch.yml 의 ${DB_NAME:-app} 이 걸려 app 스키마를 본다.
trap 'DB_NAME=coupon_clean docker compose -f base.yml -f batch.yml up -d --force-recreate --no-deps batch' EXIT

# **`|| exit 1` 이 붙어 있어야 한다.** gate() 는 기동 실패·시간 초과·검증 실패에
# return 1 을 내는데, 이 블록에 `set -e` 가 없어 **그대로 다음 줄로 넘어간다.**
# 그러면 앞 게이트가 죽어도 뒤 게이트가 돌아 절차 전체가 성공한 것처럼 끝난다.
gate coupon_clean   CLEAN   3                || exit 1
gate coupon_clean   CLEAN   4                || exit 1   # 결정론 — 같은 asOf, attempt 만 다르게
gate coupon_corrupt CORRUPT 2 "$SEED_RUN"    || exit 1
gate coupon_corrupt CORRUPT 3 "$SEED_RUN"    || exit 1

# ── 6. 여섯째 규칙까지 보려면 ───────────────────────────────────────────
cd ../cy-seed && set -a; . ./.env; set +a
./.venv/bin/python bin/seed.py all --dataset corrupt --schema coupon_v6 \
  --as-of "$ASOF" --plant-v6
cd ../cy-be
docker compose -f base.yml exec -T -e MYSQL_PWD=root mysql mysql -uroot -e "
  GRANT SELECT, INSERT, UPDATE, DELETE ON coupon_v6.* TO 'app'@'%'; FLUSH PRIVILEGES;"
for F in V11__batch_metadata.sql \
         V2026082513__ix_batch_job_execution_lookup.sql \
         V2026082514__ix_batch_job_execution_history.sql; do
  docker compose -f base.yml exec -T -e MYSQL_PWD=root mysql mysql -uroot coupon_v6 \
    < storage/src/main/resources/db/migration/$F
done
# ⚠️ **리포트를 다른 자리에 쌓는다.** runId 는 스키마마다 따로 매겨져서,
#    coupon_v6 의 run2 와 coupon_corrupt 의 run2 가 **같은 파일명**을 만든다(실측).
#    스크립트가 dataset_fingerprint 로 그것을 막고 종료 1 을 내므로 갈라 줘야 한다.
# **$( ) 안에서 실패하면 그 종료코드가 사라진다.** 그러면 $4 가 빈 채로 gate 에
# 들어가고, seedRunId 없는 CORRUPT 요청이 400 으로 거절돼 executionId 추출부터 깨진다 —
# 원인이 두 단계 떨어져 보인다. 먼저 받아서 끊는다.
SEED_RUN_V6=$(seed_run_of coupon_v6) || exit 1
REPORT_BRANCH=reports-v6 REPORT_WORKTREE="$PWD/../cy-be-reports-v6" \
  gate coupon_v6 CORRUPT 2 "$SEED_RUN_V6"   # 정답 801 · GRADE_VIOLATION 1
```

**권한이 DML 뿐인 것은 실측했다.** `SELECT, INSERT, UPDATE, DELETE` 만 준 계정으로
검증이 끝까지 돌고 권한 거부 로그가 0이다. 배치는 Flyway 가 꺼져 있고 DDL 을 안 친다
(`TRUNCATE` 도 없다 — `asof_state` 는 `DELETE … LIMIT` 로 걷는다).

---

## 남은 것

**보조 인덱스 없이 잰 수치다.** `ddl/90_perf_indexes_optional.sql` 을 적용하면 검증
소요가 달라진다. 인덱스를 넣을지는 수치를 보고 정할 일이라 여기서는 안 넣었다 —
`docs/12` 가 만료 쪽에서 같은 기준을 세웠다.

**`asof_state` 는 시딩하지 않았다**(`--asof-state` off). Step 0(`replayStep`)이 매번 다시
만들고, 그 비용이 **36초 · 31%** 다 — 위 Step 표 참고. 시딩하면 그만큼 줄겠지만
그것은 *"검증이 자기 입력을 스스로 만든다"* 는 성질을 없애는 거라 따로 판단할 일이다.

**`--plant-v6` 는 따로 쟀다** — 위 "여섯째 규칙" 절.
