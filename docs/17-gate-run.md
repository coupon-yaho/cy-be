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
| **결정론** | 같은 `asOf` 재실행 시 지문·checksum 일치 | 배치 재실행 **2회씩** (CLEAN `attempt` 3·4 · CORRUPT 2·3). 시드가 심은 기준 행까지 합쳐 `verification_runs` 4행·3행이 **지문 1개·checksum 1개로 수렴** |

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

**인덱스를 넣을 거면 `usageCountStep` 부터 본다.** 그 Step 의 `READ_COUNT` 가 0인 것도
단서다 — 청크 리더가 아니라 집계 SQL 한 방이라는 뜻이라, 청크 크기를 만져도 안 줄어든다.

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

```bash
# --no-deps 를 빼면 mysql 까지 재생성된다 — 아래 "--force-recreate" 절
# REPORT_DATASETS 는 **이 기동이 보는 것**을 선언한다. 기본값이 없다 — 아래 참고.
DB_NAME=coupon_clean   docker compose -f base.yml -f batch.yml up -d --force-recreate --no-deps batch
REPORT_DATASETS=CLEAN   bash scripts/dump-verify-report.sh
DB_NAME=coupon_corrupt docker compose -f base.yml -f batch.yml up -d --force-recreate --no-deps batch
REPORT_DATASETS=CORRUPT bash scripts/dump-verify-report.sh
```

결과는 `reports` 브랜치의 `verify/` 에 쌓인다.

```
verify/2026-08-26-clean-full-run4.json     825 B
verify/2026-08-26-corrupt-full-run3.json  1132 B
```

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
ASOF='2026-08-26 05:00:00'
ASOF_API="${ASOF/ /T}"
./.venv/bin/python bin/seed.py all --dataset clean   --schema coupon_clean   --as-of "$ASOF"
./.venv/bin/python bin/seed.py all --dataset corrupt --schema coupon_corrupt --as-of "$ASOF"

# ── 4. 배치 메타를 두 스키마에 붓는다 ────────────────────────────────────
# 시드의 ddl/ 에는 BATCH_* 가 하나도 없다. 인덱스 둘을 빼먹으면 **기동도 동작도
# 통과하는데** 게이지가 NaN 이 되거나 정리 잡이 매 청크 전체 스캔이 된다 — docs/14.
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
  until docker compose -f base.yml -f batch.yml logs batch 2>&1 \
        | grep -q "스키마 확인 완료.*$1"; do sleep 2; done

  # **쿼리 파라미터다** — 본문 JSON 은 400 이다. 시드가 attempt 를 점유한다:
  # CLEAN 1·2, CORRUPT 1. 그다음 번호부터 쓴다.
  # CORRUPT 에는 seedRunId 가 **필수**다(VerifyTriggerController 가 400 으로 막는다).
  local q="asOf=${ASOF_API}&dataset=$2&scope=FULL&attempt=$3"
  [ -n "${4:-}" ] && q="$q&seedRunId=$4"
  local ex
  ex=$(docker compose -f base.yml -f batch.yml exec -T batch \
        curl -sS -X POST "http://127.0.0.1:9090/api/v1/admin/verify?$q" \
       | python3 -c "import sys,json;print(json.load(sys.stdin)['data']['executionId'])")

  # **202 비동기다.** 끝나기 전에 덤프를 돌리면 finishedAt 이 비어 있어 못 뜬다.
  until docker compose -f base.yml -f batch.yml exec -T batch \
        curl -s "http://127.0.0.1:9090/api/v1/admin/verify/runs/$ex" \
        | grep -qE '"status":"(COMPLETED|FAILED|STOPPED|ABANDONED)"'; do sleep 10; done

  REPORT_DATASETS="$2" bash scripts/dump-verify-report.sh
}

# seedRunId 는 시드가 남긴 기준 실행의 id 다
SEED_RUN=$(docker compose -f base.yml exec -T -e MYSQL_PWD=root mysql \
  mysql -uroot -N -e "SELECT id FROM coupon_corrupt.verification_runs
                       WHERE origin='SEED' ORDER BY id DESC LIMIT 1;" | tr -d '\r')

gate coupon_clean   CLEAN   3
gate coupon_clean   CLEAN   4            # 결정론 — 같은 asOf, attempt 만 다르게
gate coupon_corrupt CORRUPT 2 "$SEED_RUN"
gate coupon_corrupt CORRUPT 3 "$SEED_RUN"

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
gate coupon_v6 CORRUPT 2 1     # 정답 801 · GRADE_VIOLATION 1
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
