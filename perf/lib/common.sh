#!/usr/bin/env bash
# 하네스 공용 함수. 각 스크립트가 source 한다.
set -euo pipefail

PERF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$PERF_DIR/.." && pwd)"

# 설정 읽기 — perf.env 가 없으면 예제에서 만들라고 말하고 죽는다.
PERF_ENV_FILE="${PERF_ENV_FILE:-$PERF_DIR/env/perf.env}"
if [[ ! -f "$PERF_ENV_FILE" ]]; then
  echo "perf.env 가 없다. cp perf/env/perf.env.example perf/env/perf.env 후 값을 채울 것." >&2
  exit 1
fi
# source 하지 않는다 — 값에 <플레이스홀더> 가 있으면 셸이 리다이렉션으로 읽어 깨진다(실측, .env 에서 밟음).
while IFS='=' read -r k v; do
  case "$k" in ''|\#*) continue;; esac
  v="${v%%[[:space:]]#*}"          # 줄 끝 주석을 뗀다. 안 떼면 값에 그대로 들어간다
  v="${v%"${v##*[![:space:]]}"}"   # 뒤 공백
  # 이미 셸에 있는 값은 덮지 않는다. 파일이 이기면 한 회차만 값을 바꿔 보려고
  # 앞에 붙인 환경변수가 조용히 무시된다(실측 — PERF_STEP_RATES 를 앞에 붙였는데
  # perf.env 의 값으로 돌았다).
  [[ -n "${!k:-}" ]] && continue
  export "$k=$v"
done < <(grep -vE '^\s*#|^\s*$' "$PERF_ENV_FILE")

: "${A_HOST:?perf.env 에 A_HOST 가 필요하다}"
: "${COMPOSE_PROJECT:?}"

# A(발급 경로 호스트)에서 명령을 돌린다. PERF_A_SSH 가 비어 있으면 로컬이다.
a_exec() {
  if [[ -n "${PERF_A_SSH:-}" ]]; then
    ssh "$PERF_A_SSH" "cd ${PERF_A_REPO:-$REPO_ROOT} && $*"
  else
    ( cd "$REPO_ROOT" && eval "$*" )
  fi
}

dc() { a_exec "docker compose -p $COMPOSE_PROJECT -f compose.yml -f perf/env/compose.perf.yml $*"; }

# compose 네트워크 안에서 HTTP 를 친다. prometheus 이미지의 busybox wget 을 쓴다 —
# api·batch 이미지(temurin jre)에는 curl·wget 이 없다.
net_get()  { dc "exec -T prometheus wget -qO- '$1'"; }
net_post() { dc "exec -T prometheus wget -qO- --post-data='' '$1'"; }

# Prometheus 질의. 식은 반드시 인코딩한다 — 공백·중괄호·> 가 들어가면 400 이 난다(실측).
# 실패를 0 으로 삼키지 않는다. 실패하면 값을 안 내고 0 이 아닌 코드로 끝난다.
promq() {
  local ep="$1" expr="$2"; shift 2
  local e; e=$(python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))' "$expr") \
    || { echo "promq: 식 인코딩 실패 — $expr" >&2; return 1; }
  local qs="query=$e"; local p; for p in "$@"; do qs="$qs&$p"; done
  local out rc
  out=$(net_get "http://localhost:9090/api/v1/$ep?$qs" 2>/dev/null); rc=$?
  if (( rc != 0 )) || [[ -z "$out" ]]; then
    echo "promq: 가져오기 실패(rc=$rc). 0 이 아니라 <측정 실패>로 기록할 것 — $expr" >&2
    return 1
  fi
  case "$out" in
    *'"status":"success"'*) printf '%s' "$out" ;;
    *) echo "promq: Prometheus 가 오류를 반환했다 — $out" >&2; return 1 ;;
  esac
}

promq_scalar() { promq query "$1" | jq -r '.data.result[0].value[1] // empty'; }

mysql_exec() {
  # -N -B: 헤더 없이 탭 구분. 스크립트가 파싱한다.
  # MYSQL_PWD 로 넘긴다. -p 로 주면 매 호출마다 경고가 stderr 로 나와 로그를 덮는다.
  dc "exec -T mysql sh -c 'MYSQL_PWD=\"\$MYSQL_PASSWORD\" exec mysql -u\"\$MYSQL_USER\" \"\$MYSQL_DATABASE\" -N -B'" <<<"$1"
}

mysql_root_exec() {
  dc "exec -T mysql sh -c 'MYSQL_PWD=\"\$MYSQL_ROOT_PASSWORD\" exec mysql -uroot \"\$MYSQL_DATABASE\" -N -B'" <<<"$1"
}

# compose 네트워크 안의 URL 이 200 을 낼 때까지 기다린다.
# ⚠️ compose 의 --wait 은 healthcheck 가 있는 서비스에만 쓸모가 있다. api·batch 에는
#    healthcheck 가 없어서 "Started" 는 컨테이너가 떴다는 뜻이지 애플리케이션이 받는다는
#    뜻이 아니다. batch 기동에 실측 114초가 걸렸고, 그 사이 워밍업 호출은 Connection
#    refused 로 죽는다.
wait_http() {
  local url="$1" limit="${2:-180}" i=0
  while (( i < limit )); do
    if net_get "$url" >/dev/null 2>&1; then return 0; fi
    sleep 2; i=$((i + 2))
  done
  return 1
}

log()  { printf '\033[1m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die()  { printf '\033[31m[실패]\033[0m %s\n' "$*" >&2; exit 1; }
