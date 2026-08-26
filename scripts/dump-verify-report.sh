#!/bin/bash
# 검증 최종 판정을 떠서 reports 브랜치에 쌓는다. 오늘 실제로 돈 판정만 남긴다.
#
#   손으로:  bash scripts/dump-verify-report.sh
#   자동:    launchd — docs/15 "제출물을 뜨는 절차" 참고
#
# ── 왜 파일로 쌓나 ────────────────────────────────────────────────────────────
# 판정 이력은 이미 DB 에 영구히 있다 — 정리 잡이 verification_runs 를 안 지운다.
# 관제 페이지가 그것을 히스토리로 보여주므로 **기능은 충분하다.**
#
# 부족한 것은 증거력이다. docs/16 이 적었다 — "우리가 만든 페이지가 우리가 만든 테이블을
# 읽어 보여주는 구조라, 보는 사람에게 '정말 돌았나' 를 스스로 증명하지 못한다."
#
# ── 그래서 "안 돈 날" 을 반드시 걸러야 한다 ─────────────────────────────────
# /reports/latest 는 **가장 최근에 닫힌** run 을 준다. 언제 닫혔는지는 안 본다
# (VerificationRunJdbcAdapter 의 SELECT_LATEST_CLOSED 에 시간 하한이 없다).
#
# 그래서 오늘 검증이 안 돌았으면 **어제 판정**이 온다. 그것을 오늘 날짜로 커밋하면
# "이 날 검증이 돌았다" 를 자동으로 주장하면서 그 주장을 확인하지 않는 셈이 된다 —
# 증거가 아니라 **위증**이다. 아래 fresh() 가 그 자리를 막는다.
#
# ── 왜 전용 worktree 인가 ──────────────────────────────────────────────────
# 사람의 작업 트리를 쓰면 예약 작업이 그 사람의 인덱스·브랜치·미푸시 커밋에 얹힌다.
# 실제로 셋 다 사고가 된다:
#   · git commit 은 pathspec 이 없으면 **인덱스 전체**를 커밋한다 — 사람이 add 해 둔 것까지
#   · git push 는 **브랜치 전체**를 민다 — 일부러 안 민 로컬 커밋까지 공개 저장소로
#   · 체크아웃돼 있던 브랜치가 지워지면 증적도 함께 사라진다
# 예약 작업이라 사람이 그 순간을 못 본다. worktree 를 따로 두면 셋이 구조적으로 없어진다.
#
# ── 왜 컨테이너 안에서 curl 하나 ──────────────────────────────────────────
# 업무 포트 9090 은 batch.yml 이 **호스트로 안 내보낸다** — batch-expose.yml 을 얹어야 열리고,
# 그 포트에는 인증이 없다. 자동화를 위해 그것을 상시 열어 두는 것은 전제를 바꾸는 일이다.
# compose exec 로 컨테이너 안에서 치면 호스트 포트를 안 열어도 된다.
# docs/16 §3 이 막은 것은 "컨테이너에 GitHub 자격증명을 넣는 것" 이지 이것이 아니다.
set -uo pipefail

# 스크립트 위치 기준으로 저장소를 잡는다. cwd 에 기대면 다른 클론에서 부를 때 엉뚱한 곳에 쓴다.
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" || exit 1
cd "$REPO" || exit 1

# **맨 위에 둔다.** 아래 어느 검사에서든 부를 수 있어야 한다 — 한때 이것이
# 락 블록 옆에 있어서 그보다 먼저 도는 검증이 `fail: command not found` 로 죽었다.
# 종료코드는 1 로 나갔지만 **이유가 통째로 사라졌다.** 무인 실행에서 제일 나쁜 모양이다.
fail() { echo "  ✗ $*"; }

BRANCH="${REPORT_BRANCH:-reports}"
WORKTREE="${REPORT_WORKTREE:-${REPO}/../cy-be-reports}"
TICKET="${REPORT_TICKET:-CY-590}"
PUSH="${REPORT_PUSH:-0}"          # 무인 푸시는 **사람이 켠다.** 기본은 커밋까지만.
# **배열이다.** 문자열로 두면 `-f $COMPOSE` 에서 따옴표를 벗겨야 하고, 그 자리마다
# 정적 검사를 무시하게 된다 — 그 습관이 경로에 공백이 낀 다른 자리에서 사고가 된다.
# (줄 첫머리에 `#` + shellcheck 를 쓰면 그 도구가 **지시어**로 읽는다. 쓰지 말 것.)
read -r -a COMPOSE_FILES <<< "${REPORT_COMPOSE:-base.yml batch.yml}"
COMPOSE_ARGS=()
for f in "${COMPOSE_FILES[@]}"; do COMPOSE_ARGS+=(-f "$f"); done

# **한 번만 잰다.** dump() 안에서 각각 date 를 부르면 두 조합이 서로 다른 순간을 기준으로
# 판정한다 — 자정을 걸치면 앞엣것만 통과하고 뒤엣것이 죽어, all-or-nothing 때문에
# 성공한 쪽까지 버려진다.
NOW_EPOCH="$(date -u +%s)"
MAX_AGE="${REPORT_MAX_AGE:-21600}"          # 6시간
HTTP_TIMEOUT="${REPORT_HTTP_TIMEOUT:-120}"  # 아래 dump() 의 주석 참고

# **숫자가 아니면 여기서 죽어야 한다.** 아래 check_age 의 `[ "$age" -gt "$MAX_AGE" ]` 는
# 숫자가 아니면 "integer expression expected" 로 **종료코드 2** 를 내는데, `set -e` 가
# 없으므로 if 가 그것을 그냥 거짓으로 읽는다 — **낡은 판정이 통과한다**(재현했다).
# 신선도 검사를 통째로 무력화하는 자리라 조용히 넘어가면 안 된다.
for pair in "REPORT_MAX_AGE:$MAX_AGE" "REPORT_HTTP_TIMEOUT:$HTTP_TIMEOUT"; do
  case "${pair#*:}" in
    ''|*[!0-9]*)
      echo "  ✗ ${pair%%:*} 는 0 이상의 정수여야 한다: ${pair#*:}" >&2
      exit 1 ;;
  esac
done

# ISO 8601 문자열을 epoch 로. BSD(date -j) 와 GNU(date -d) 를 둘 다 받는다 —
# 이 스크립트는 macOS 호스트에서 돌지만, CI 나 리눅스에서 손으로 돌릴 수도 있다.
to_epoch() {
  local iso="${1:0:19}"
  date -u -j -f '%Y-%m-%dT%H:%M:%S' "$iso" +%s 2>/dev/null \
    || date -u -d "${iso/T/ }" +%s 2>/dev/null
}

# 판정 시각 하나를 창 안에서 보는지 본다. 성공하면 아무것도 안 찍는다.
check_age() {
  local what="$1" iso="$2" epoch age
  if [ -z "$iso" ]; then fail "${what} 이 비어 있다"; return 1; fi
  epoch="$(to_epoch "$iso")" || true
  if [ -z "$epoch" ]; then
    # **여기서 죽는 것이 중요하다.** jsr310 이 빠지면 LocalDateTime 이
    # {"year":2026,...} 로 나가는데, 그러면 앞 19자가 날짜가 아니다.
    fail "${what} 형식이 예상과 다르다(ISO 8601 이 아니다): ${iso}"
    return 1
  fi
  age=$(( NOW_EPOCH - epoch ))
  if [ "$age" -lt 0 ]; then
    # 미래 시각. 컨테이너 TZ 가 UTC 가 아니게 되면 이렇게 온다 — 통과시키면
    # 시간대 어긋남이 영영 안 보인다.
    fail "${what} 이 미래다(${age}초). 컨테이너 시간대를 봐라: ${iso}"
    return 1
  fi
  if [ "$age" -gt "$MAX_AGE" ]; then
    fail "${what} 이 ${age}초 됐다(허용 ${MAX_AGE}초): ${iso}"
    return 1
  fi
}
SERVICE="${REPORT_SERVICE:-batch}"

# ── 이 기동이 **서비스하는** 데이터셋을 선언한다 ─────────────────────────────
#
# 배치는 DB_NAME 으로 스키마를 하나만 잡으므로(coupon_clean · coupon_corrupt) 한 기동이
# 둘 다 보는 일은 없다. 둘을 다 남기려면 스키마를 바꿔 기동하고 이 스크립트를 다시 돌린다.
#
# ⚠️ **기본값을 두지 않는다.** 한때 `CLEAN CORRUPT` 를 기본으로 두고 404 를
#    *"이 기동이 안 보는 데이터셋"* 으로 읽어 건너뛰었는데, **그 해석이 틀렸다.**
#    이 API 는 어느 데이터셋을 서비스하는지 모른다 — 404(`RUN_NOT_FOUND`)는 현재 스키마의
#    `verification_runs` 에서 그 조합의 **닫힌 실행을 못 찾았다**는 뜻이고, 원인이 최소 셋이다:
#      ⑴ 그 데이터셋이 다른 스키마에 있다   ⑵ 시드 재주입으로 판정이 사라졌다
#      ⑶ 오늘 검증이 아직 안 끝났거나 실패했다
#    ⑵·⑶ 을 건너뛰면 이 파일 첫머리가 세운 **"안 돈 날을 거른다"** 가 통째로 무너진다.
#
# 선언하게 하면 그 셋이 다시 갈린다 — 선언한 것은 **전부 200 이어야 하고**, 없는 것은
# 애초에 목록에 안 들어온다.
read -r -a DATASETS <<< "${REPORT_DATASETS:-}"
if [ "${#DATASETS[@]}" -eq 0 ]; then
  fail "REPORT_DATASETS 를 선언해라 — 이 기동이 보는 스키마의 데이터셋이다"
  fail "  예: REPORT_DATASETS=CLEAN  (DB_NAME=coupon_clean 으로 띄웠을 때)"
  exit 1
fi
for ds in "${DATASETS[@]}"; do
  case "$ds" in
    CLEAN|CORRUPT) ;;
    # 콤마로 쓰는 실수를 여기서 잡는다. 안 잡으면 서버가 400 을 내고
    # 로그에는 `HTTP 400` 만 남아 원인이 "구분자" 라는 것이 안 보인다.
    *) fail "알 수 없는 dataset: [${ds}] — 공백으로 구분한 CLEAN·CORRUPT 만 받는다"; exit 1 ;;
  esac
done

# 동시 실행 막기. macOS 에 flock(1) 이 없어 mkdir 로 한다(원자적이다).
#
# ⚠️ **죽은 락을 걷어내야 한다.** 해제가 trap EXIT 하나뿐이면 SIGKILL·강제 재부팅·
#    launchd 타임아웃 kill 에서 락이 남는다. macOS 의 사용자 TMPDIR 은 재부팅으로 안
#    지워진다(3일 무접근 청소). 그러면 **그 뒤 모든 실행이 조용히 아무것도 안 한다.**
#    게다가 커밋 메시지 자체가 "커밋 없는 날을 실패로 읽지 말라" 고 적어 놔서,
#    아무도 그 공백을 이상하게 안 여긴다 — 증적이 무기한 끊긴 채로.
#    그래서 주인 PID 가 살아 있는지 본다.
LOCK="${TMPDIR:-/tmp}/cy-verify-report.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  owner="$(cat "$LOCK/pid" 2>/dev/null || echo 0)"
  case "$owner" in ''|*[!0-9]*) owner=0 ;; esac
  if [ "$owner" -gt 0 ] && kill -0 "$owner" 2>/dev/null; then
    # **종료 0 이 아니다.** 0 으로 나가면 launchd 로그에서 정상 완료와 구분이 안 된다.
    echo "이미 돌고 있다(pid=${owner}). 건너뛴다."
    exit 75   # EX_TEMPFAIL
  fi
  echo "죽은 락을 걷어낸다(pid=${owner})."
  rm -rf "$LOCK"
  mkdir "$LOCK" || { fail "락을 못 만든다: $LOCK"; exit 1; }
fi
echo $$ > "$LOCK/pid"
trap 'rm -rf "$LOCK" 2>/dev/null' EXIT

# ── 전용 worktree 를 준비한다 ───────────────────────────────────────────────
# 디렉터리를 손으로 지워도 git 등록은 남는다. 그 상태로 add 하면
# "missing but already registered worktree" 로 죽고, 그 뒤로 영영 못 만든다.
git worktree prune

if [ -d "$WORKTREE/.git" ] || [ -f "$WORKTREE/.git" ]; then
  # **그 자리에 있는 것이 이 저장소인지 확인한다.** REPORT_WORKTREE 는 임의 경로를 받고,
  # 아래에서 `git -C "$WORKTREE" push origin ...` 을 친다 — 그 자리에 다른 클론이
  # 남아 있으면 판정 파일이 **엉뚱한 저장소로 밀린다.** 예약 작업이라 사람이 못 본다.
  want="$(git rev-parse --path-format=absolute --git-common-dir)"
  got="$(git -C "$WORKTREE" rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)"
  if [ "$want" != "$got" ]; then
    fail "$WORKTREE 는 이 저장소의 worktree 가 아니다"
    fail "  여기: ${want}"
    fail "  거기: ${got:-<git 저장소가 아니다>}"
    exit 1
  fi
else
  echo "리포트 worktree 를 만든다 — $WORKTREE"
  # **다른 worktree 가 그 브랜치를 잡고 있으면 영영 못 만든다.** git 은
  # "already used by worktree at ..." 로 죽는데, 무인 실행이라 사람이 그 줄을 못 본다.
  # 어디가 잡고 있는지를 먼저 말해 준다.
  HOLDER="$(git worktree list --porcelain \
    | awk -v b="refs/heads/${BRANCH}" '/^worktree /{w=$2} $0=="branch "b{print w}')"
  if [ -n "$HOLDER" ] && [ "$HOLDER" != "$WORKTREE" ]; then
    fail "'${BRANCH}' 를 다른 worktree 가 잡고 있다 — ${HOLDER}"
    fail "그것을 정리하거나(git worktree remove) REPORT_WORKTREE 를 그 경로로 맞춰라"
    exit 1
  fi

  if git show-ref --verify --quiet "refs/heads/${BRANCH}"; then
    # 로컬에 이미 있다 — 그대로 붙인다
    git worktree add -q "$WORKTREE" "$BRANCH" || exit 1
  elif git ls-remote --exit-code --heads origin "$BRANCH" >/dev/null 2>&1; then
    git worktree add -q --track -b "$BRANCH" "$WORKTREE" "origin/$BRANCH" || exit 1
  else
    # 아무 데도 없으면 **빈 뿌리**에서 시작한다. 코드 히스토리와 섞이면 리포트 diff 가
    # 코드 변경에 묻히고, 그 브랜치를 main 에 머지할 일도 없다.
    git worktree add -q --detach "$WORKTREE" || exit 1
    git -C "$WORKTREE" checkout -q --orphan "$BRANCH" || exit 1
    git -C "$WORKTREE" rm -rq --cached . 2>/dev/null
    find "$WORKTREE" -mindepth 1 -maxdepth 1 ! -name '.git' -exec rm -rf {} + 2>/dev/null
  fi
fi

OUT_DIR="$WORKTREE/verify"
mkdir -p "$OUT_DIR" || { fail "$OUT_DIR 를 못 만든다"; exit 1; }

# ── 한 조합을 뜬다 ──────────────────────────────────────────────────────────
# **이름은 뜬 날짜가 아니라 판정이 끝난 날짜다.** 뜬 날짜로 지으면 안 돈 날에도 새 파일이
# 생겨 "그 날 돌았다" 가 된다.
#
# ⚠️ 한때 여기 "안 돈 날에는 같은 이름이 나오고 내용도 같아 저절로 조용해진다" 고 적혀
#    있었는데 **그 경로는 도달 불가능하다.** 위 check_age 가 안 돈 날을 먼저 걷어내므로
#    PENDING 에 들어오는 것은 전부 창 안의 판정이다. 아래 cmp 가 막는 것은 "안 돈 날" 이
#    아니라 **같은 실행을 두 번 뜬 경우의 중복 커밋**이다.
#    이 구분이 중요하다 — 죽은 방어를 믿고 신선도 검사를 느슨하게 하면, 그 순간
#    옛 판정이 옛 날짜 파일명으로 되살아나 과거 커밋을 덮는다.
#
# **파일 이름에 runId 를 넣는다.** 날짜만 쓰면 같은 날 판정이 둘 나올 때 뒤엣것이 앞엣것을
# 덮는다 — 오전 FAIL 을 고쳐 오후 PASS 가 나면, 그날 트리에는 PASS 만 남는다.
# runId 를 넣으면 **실행 하나가 파일 하나**라 덮어쓸 일이 없고, 같은 실행을 두 번 떠도
# 같은 이름·같은 내용이라 아래 cmp 가 조용히 건너뛴다.
declare -a PENDING=()

dump() {
  local dataset="$1" scope="$2"
  local d s
  d="$(echo "$dataset" | tr '[:upper:]' '[:lower:]')"
  s="$(echo "$scope" | tr '[:upper:]' '[:lower:]')"
  local raw; raw="$(mktemp)"

  # -sSf: 4xx/5xx 를 종료코드로 만든다. 이게 없으면 에러 본문이 그대로 제출물이 된다.
  #
  # ⚠️ 타임아웃이 넉넉해야 한다. 대조 SQL 이 조인 양쪽에 CAST(... AS BINARY) 를 씌워
  #    uk_expected·uk_run_finding 을 못 탄다(어댑터 javadoc 이 그 대가를 적었다).
  #    검출이 수만으로 튀는 **바로 그 날** 가장 느린데, 그날이 리포트가 제일 필요한 날이다.
  # -w 로 상태코드를 따로 받는다. -f 만 쓰면 404 와 "배치가 안 떴다" 가 같은 종료코드라
  # **구조적으로 없는 것**과 **고장난 것**을 못 가른다 — 아래 갈림의 근거가 그 코드다.
  local code
  code="$(docker compose "${COMPOSE_ARGS[@]}" exec -T "$SERVICE" \
      curl -sS --max-time "$HTTP_TIMEOUT" -o /dev/stdout -w '\n%{http_code}' \
      "http://127.0.0.1:9090/api/v1/admin/verify/reports/latest?dataset=${dataset}&scope=${scope}" \
      2>/dev/null | tee "$raw" | tail -1)"

  # **형태를 먼저 본다.** curl 이 -w 를 내기 전에 죽으면(SIGKILL 등) tail 이 본문의
  # 마지막 줄을 준다. 그 값이 우연히 숫자면 그것을 상태코드로 읽는다 — 조용히 성공으로
  # 접히는 방향의 실패라 아무도 못 본다. (연결 거부는 curl 이 000 을 내므로 여기 안 걸린다.)
  case "$code" in
    ''|*[!0-9]*) fail "${dataset} ${scope} — 상태코드를 못 읽었다: [${code}]"
                 rm -f "$raw"; return 1 ;;
  esac
  if [ "${#code}" -ne 3 ]; then
    fail "${dataset} ${scope} — 상태코드가 3자리가 아니다: [${code}]"
    rm -f "$raw"; return 1
  fi

  # 마지막 줄(상태코드)을 떼어 낸다. **`sed -i` 를 안 쓴다** — BSD/GNU 접미사 분기가
  # 생기고, 폴백 갈래는 실패 조건을 만들 수 없어 검증도 못 한다. 한 갈래로 둔다.
  if ! { sed '$d' "$raw" > "${raw}.t" && mv "${raw}.t" "$raw"; }; then
    fail "${dataset} ${scope} — 상태코드 줄을 못 떼어냈다(TMPDIR 을 봐라)"
    rm -f "$raw" "${raw}.t"; return 1
  fi

  if [ "$code" = "404" ]; then
    # **선언한 것이 없으면 고장이다.** 위 REPORT_DATASETS 주석 참고 — 404 는
    # "다른 스키마에 있다" 만 뜻하지 않는다. 판정이 사라졌거나 오늘 안 끝난 경우도 같은 코드다.
    fail "${dataset} ${scope} — 닫힌 판정이 없다(404). 셋 중 하나다:"
    fail "  ⑴ DB_NAME 이 이 데이터셋의 스키마가 아니다 (그렇다면 REPORT_DATASETS 를 맞춰라)"
    fail "  ⑵ 시드 재주입으로 verification_runs 가 비었다"
    fail "  ⑶ 오늘 verifyJob 이 아직 안 끝났거나 실패했다"
    rm -f "$raw"; return 1
  fi
  if [ "$code" != "200" ]; then
    fail "${dataset} ${scope} — HTTP ${code}. 배치가 떠 있는지, 그 조합의 판정이 있는지 봐라"
    rm -f "$raw"; return 1
  fi

  # jq 가 **잘린 본문**도 잡는다. 200 인데 중간에 끊긴 응답이 제출물이 되면 안 된다.
  local body; body="$(mktemp)"
  if ! jq . "$raw" > "$body" 2>/dev/null; then
    fail "${dataset} ${scope} — JSON 이 아니다(잘렸거나 형식이 다르다)"
    rm -f "$raw" "$body"; return 1
  fi
  rm -f "$raw"

  local finished asof runId
  finished="$(jq -r '.data.run.finishedAt // empty' "$body")"
  asof="$(jq -r '.data.run.asOf // empty' "$body")"
  runId="$(jq -r '.data.run.id // empty' "$body")"

  if [ -z "$finished" ]; then
    fail "${dataset} ${scope} — 판정이 안 끝난 실행이다"
    rm -f "$body"; return 1
  fi
  if [ -z "$runId" ]; then
    fail "${dataset} ${scope} — 실행 번호가 없다. 응답 형식이 바뀌었나"
    rm -f "$body"; return 1
  fi

  # **신선도는 절대 나이로 본다 — 캘린더 날짜가 아니다.**
  # 한때 finishedAt 앞 10자를 date -u +%F 와 비교했는데, 그 창은 폭이 0~24시간으로 변한다:
  #   · 00:05 UTC 판정을 23:55 UTC 에 떠도 통과한다 — 24시간 된 판정을 오늘 것으로 커밋
  #   · 23:58 UTC 판정을 00:02 UTC 에 뜨면 4분 된 판정이 거부된다
  #   · 예약을 09:00 KST(= 00:00 UTC)로 옮기면 **매일 실패**한다
  # 나이로 재면 셋 다 없어진다.
  if ! check_age "${dataset} ${scope} 의 finishedAt" "$finished"; then
    rm -f "$body"; return 1
  fi

  # **asOf 도 본다.** finishedAt 만 보면 "과거 데이터를 오늘 재실행한 판정" 이 통과한다 —
  # attempt 를 바꿔 결정론을 확인하는 것이 이 과제의 핵심 실험이라 실제로 일어난다.
  # 그날 정기 배치가 실패했으면, 어제 데이터의 판정이 오늘 증적으로 커밋된다.
  # 그것이 이 스크립트가 막으려는 위증 그 자체다.
  if ! check_age "${dataset} ${scope} 의 asOf" "$asof"; then
    rm -f "$body"; return 1
  fi

  local day="${finished:0:10}"
  PENDING+=("${body}:${OUT_DIR}/${day}-${d}-${s}-run${runId}.json")
}

echo "검증 판정을 뜬다 — ${SERVICE} 컨테이너 안에서"
ok=1
for ds in "${DATASETS[@]}"; do
  dump "$ds" FULL || ok=0
done

# **선언한 것 중 하나라도 실패하면 아무것도 안 옮긴다.** 하나만 옮기면 그 파일이
# 커밋 안 된 채 남고, 다음 성공일 커밋에 남의 날짜 리포트가 섞인다 —
# 커밋 날짜와 담긴 판정의 날짜가 어긋난다.
if [ "$ok" -eq 0 ]; then
  for e in "${PENDING[@]:-}"; do [ -n "$e" ] && rm -f "${e%%:*}"; done
  exit 1
fi

declare -a WROTE=()
for e in "${PENDING[@]}"; do
  tmp="${e%%:*}"; path="${e#*:}"
  if [ -f "$path" ] && cmp -s "$tmp" "$path"; then
    echo "  = $(basename "$path") (같은 판정이다)"
    rm -f "$tmp"; continue
  fi
  # **같은 이름인데 다른 데이터셋이면 덮으면 안 된다.** 파일명은
  # {날짜}-{dataset}-{scope}-run{runId} 인데 runId 는 **스키마마다 따로 매겨진다** —
  # coupon_corrupt 와 coupon_v6 가 둘 다 CORRUPT/FULL/run2 를 갖는 것을 실측했다.
  # 그대로 두면 나중에 뜬 쪽이 앞엣것을 조용히 지운다.
  #
  # dataset_fingerprint 가 데이터셋의 신원이다. 그것이 같으면 같은 데이터의 같은 실행이라
  # 덮어도 된다(응답 형식이 바뀐 경우). 다르면 **다른 데이터셋이 이름만 겹친 것**이다.
  if [ -f "$path" ]; then
    old_fp="$(jq -r '.data.run.datasetFingerprint // empty' "$path" 2>/dev/null)"
    new_fp="$(jq -r '.data.run.datasetFingerprint // empty' "$tmp"  2>/dev/null)"

    # **못 읽으면 덮지 않는다.** 한때 둘 다 비어 있지 않을 때만 비교했는데, 그러면
    # 필드가 빠지거나 기존 파일이 깨진 순간 **검사 자체를 건너뛰고 덮어썼다** —
    # 방어가 가장 필요한 상황에서 방어가 없어지는 모양이다.
    if [ -z "$old_fp" ] || [ -z "$new_fp" ]; then
      fail "$(basename "$path") 가 이미 있는데 신원을 못 읽는다 — 덮지 않는다"
      fail "  있던 것: [${old_fp:-<없음>}]"
      fail "  새 것:   [${new_fp:-<없음>}]"
      fail "  둘이 같은 데이터셋인지 확인할 수 없다. 손으로 봐라: $path"
      rm -f "$tmp"; exit 1
    fi
    if [ "$old_fp" != "$new_fp" ]; then
      fail "$(basename "$path") 가 이미 있는데 데이터셋이 다르다 — 덮지 않는다"
      fail "  있던 것: ${old_fp:0:16}…"
      fail "  새 것:   ${new_fp:0:16}…"
      fail "  스키마가 다른 두 DB 가 같은 runId 를 쓴 것이다. REPORT_BRANCH·REPORT_WORKTREE 를 갈라라"
      rm -f "$tmp"; exit 1
    fi
  fi

  if ! mv "$tmp" "$path"; then
    fail "$(basename "$path") 를 못 썼다"; rm -f "$tmp"; exit 1
  fi
  echo "  + $(basename "$path")"
  WROTE+=("verify/$(basename "$path")")
done

[ "${#WROTE[@]}" -eq 0 ] && { echo "바뀐 것이 없다. 커밋하지 않는다."; exit 0; }

# ── 커밋 ────────────────────────────────────────────────────────────────────
# worktree 전용이라 인덱스에 남의 것이 섞일 일이 없다. 그래도 경로를 명시한다 —
# 이 스크립트가 언젠가 공용 트리로 돌아갈 때 그 한 줄이 사고를 막는다.
# **디렉터리가 아니라 이번 실행이 쓴 파일만 stage 한다.** `add verify` 로 넓히면 앞선
# 실행이 비정상 종료로 남긴 파일과 사람이 손으로 둔 파일까지 이 증적 커밋에 섞인다 —
# 커밋 메시지는 "오늘 끝난 검증 FULL 의 판정" 이라고 말하는데 내용은 그렇지 않게 된다.
#
# **|| 를 빼면 안 된다.** add 가 죽어도(인덱스 잠김·권한·디스크) 다음 줄은 "스테이징된
# 변경이 없다" 로 읽고 **종료 0** 을 낸다 — 재현했다(add 종료 128, 그다음 exit 0).
# 파일은 이미 mv 로 디스크에 있으므로, 다음날 같은 판정이면 cmp 가 건너뛰어 **영영** 안 올라간다.
git -C "$WORKTREE" add -- "${WROTE[@]}" \
  || { fail "add 실패. 인덱스를 봐라: $WORKTREE"; exit 1; }

# 좁게 stage 한 대가로 **남는 것이 생길 수 있다.** 조용히 두면 그것도 사고이므로 말한다 —
# 앞선 실행이 남긴 판정이면 손으로 커밋하고, 아니면 지우면 된다.
stray="$(git -C "$WORKTREE" status --porcelain -- verify | grep -v '^A ' || true)"
if [ -n "$stray" ]; then
  echo "  ! verify/ 에 이번 실행이 안 쓴 것이 있다. 손으로 봐라:"
  while IFS= read -r line; do echo "      $line"; done <<< "$stray"
fi

if git -C "$WORKTREE" diff --cached --quiet -- verify; then
  echo "스테이징된 변경이 없다."; exit 0
fi

# ⚠️ `-m` 은 `--` 앞에 와야 한다. 뒤에 두면 git 이 그것을 pathspec 으로 읽어
#    "pathspec '-m' did not match any file(s)" 로 죽는다 — 실제로 그렇게 짰다가 잡혔다.
#
# ⚠️ pathspec 도 `verify` 가 아니라 **이번에 쓴 파일 목록**이다. 디렉터리로 넓히면
#    좁게 stage 한 뜻이 사라진다 — --only 는 준 경로의 작업 트리 상태를 커밋한다.
git -C "$WORKTREE" commit -q --only -m "$(cat <<MSG
docs/${TICKET} 검증 판정을 남긴다

오늘 끝난 검증 FULL 의 판정을 그대로 떠서 커밋한다. 판정 이력은 DB 에도 영구히 있지만
(정리 잡이 verification_runs 를 안 지운다), 그것은 우리 화면 안의 데이터다 —
커밋 이력은 소급해서 못 꾸민다.

⚠️ 커밋이 없는 날을 "검증 실패" 로 읽으면 안 된다. 배치가 안 돈 날 · 머신이 꺼진 날 ·
   판정이 어제 것뿐인 날이 커밋 로그에서 전부 같은 모양이다. 실패 축은 docs/16 몫이다.

scripts/dump-verify-report.sh 가 만들었다.
MSG
)" -- "${WROTE[@]}" || { fail "커밋 실패"; exit 1; }
echo "커밋했다: $(git -C "$WORKTREE" log --oneline -1)"

[ "$PUSH" != "1" ] && { echo "푸시는 안 한다(REPORT_PUSH=1 이어야 한다)."; exit 0; }

if ! git -C "$WORKTREE" push -q origin "HEAD:refs/heads/${BRANCH}"; then
  # **조용히 넘어가면 안 된다.** 로컬 커밋만 남고, 다음날 같은 판정이면 cmp 가
  # 건너뛰어 영영 안 올라간다 — "매일 증거가 쌓인다" 는 전제가 깨진 채로.
  fail "푸시 실패. 로컬 커밋이 origin 과 어긋났다 — 손으로 확인해라"
  exit 1
fi
echo "푸시했다."
