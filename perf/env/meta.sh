#!/usr/bin/env bash
# 환경 메타를 JSON 으로 낸다. 이게 없으면 회차 간 비교가 불가능하다 — 같은 이미지·같은
# 300/s 를 세 번 쟀는데 med 가 3ms · 82ms · 224ms 로 흔들린 적이 있다(실측). 무엇이
# 달랐는지 되짚으려면 회차마다 조건이 남아 있어야 한다.
#
# 안 잰 항목은 추정으로 채우지 않는다. null 로 두고 note 에 이유를 적는다.
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

notes=()
jnum() { [[ -n "${1:-}" ]] && printf '%s' "$1" || printf 'null'; }
jstr() { [[ -n "${1:-}" ]] && jq -Rn --arg v "$1" '$v' || printf 'null'; }

SHA=$(cd "$REPO_ROOT" && git rev-parse HEAD)
BRANCH=$(cd "$REPO_ROOT" && git rev-parse --abbrev-ref HEAD)
DIRTY=$(cd "$REPO_ROOT" && git status --porcelain | wc -l | tr -d ' ')

API_TAG="${API_IMAGE_TAG:-}"; BATCH_TAG="${BATCH_IMAGE_TAG:-}"
API_IMAGE_ID=$(a_exec "docker inspect --format '{{.Image}}' \$(docker compose -p $COMPOSE_PROJECT ps -q api | head -1)" 2>/dev/null || true)

# 컨테이너 수 — 선언값(PERF_API_REPLICAS)과 실제가 다르면 그 자체가 회차 무효 사유다.
API_RUNNING=$(a_exec "docker compose -p $COMPOSE_PROJECT ps -q api" 2>/dev/null | grep -c . || true)

# 톰캣 수용 한계는 컨테이너에 선언된 값을 읽는다. actuator 의 env·configprops 는
# 노출 allowlist 에서 제외돼 있어(비밀값 유출 방지) 그쪽으로는 못 읽는다.
api_env() {
  a_exec "docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' \
    \$(docker compose -p $COMPOSE_PROJECT ps -q api | head -1)" 2>/dev/null \
    | grep "^$1=" | head -1 | cut -d= -f2- || true
}
TOMCAT_MAX_CONN=$(api_env TOMCAT_MAX_CONNECTIONS)
TOMCAT_ACCEPT=$(api_env TOMCAT_ACCEPT_COUNT)
TOMCAT_THREADS=$(api_env TOMCAT_THREADS_MAX)
DB_POOL=$(api_env DB_POOL_SIZE)
JAVA_OPTS=$(api_env JAVA_TOOL_OPTIONS)

# Prometheus 쪽은 선언이 아니라 실제 빈에서 나온 값이다. 둘이 다르면 그 사실이 결과다.
UP_API=$(promq_scalar 'count(up{job="api"})' 2>/dev/null || true)
UP_API_HEALTHY=$(promq_scalar 'sum(up{job="api"})' 2>/dev/null || true)
UP_BATCH=$(promq_scalar 'sum(up{job="batch"})' 2>/dev/null || true)
HIKARI_TOTAL=$(promq_scalar 'sum(hikaricp_connections_max{job="api"})' 2>/dev/null || true)
HEAP_MAX_MIN=$(promq_scalar 'min(sum by (instance) (jvm_memory_max_bytes{job="api",area="heap"}))' 2>/dev/null || true)
CPU_COUNT=$(promq_scalar 'min(system_cpu_count{job="api"})' 2>/dev/null || true)
[[ -z "$UP_API" ]] && notes+=("Prometheus 질의 실패 — 아래 up_* 는 0 이 아니라 <측정 실패>다")

MYSQL_MAX_CONN=$(mysql_exec "SELECT @@max_connections;" 2>/dev/null || true)
MYSQL_VERSION=$(mysql_exec "SELECT VERSION();" 2>/dev/null || true)

# ── 부하기(B) 쪽 ────────────────────────────────────────────────────────────
K6_VERSION=$(k6 version 2>/dev/null | head -1 || true)
PORT_FIRST=$(sysctl -n net.inet.ip.portrange.first 2>/dev/null || true)
PORT_LAST=$(sysctl -n net.inet.ip.portrange.last 2>/dev/null || true)
TCP_MSL=$(sysctl -n net.inet.tcp.msl 2>/dev/null || true)
TIME_WAIT=$(netstat -an -p tcp 2>/dev/null | grep -c TIME_WAIT || true)

# B->A 왕복. 무선 구간이라 회차마다 잰다. 지터가 크면 그 회차는 버린다.
PING_N="${PERF_PING_COUNT:-200}"
ping_out=$(ping -c "$PING_N" -i 0.2 "$A_HOST" 2>/dev/null | tail -2 || true)
PING_LOSS=$(printf '%s' "$ping_out" | grep -oE '[0-9.]+% packet loss' | grep -oE '^[0-9.]+' || true)
PING_STATS=$(printf '%s' "$ping_out" | grep -oE '= [0-9./]+ ms' | tr -d '= ms' || true)
PING_MIN=$(cut -d/ -f1 <<<"$PING_STATS"); PING_AVG=$(cut -d/ -f2 <<<"$PING_STATS")
PING_MAX=$(cut -d/ -f3 <<<"$PING_STATS"); PING_STDDEV=$(cut -d/ -f4 <<<"$PING_STATS")
[[ -z "$PING_AVG" ]] && notes+=("ping 실패 — A_HOST=$A_HOST 에 ICMP 가 막혔거나 도달 불가")

cat <<JSON
{
  "captured_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "repo": {
    "commit": "$SHA",
    "branch": "$BRANCH",
    "dirty_files": $DIRTY
  },
  "images": {
    "api_tag": $(jstr "$API_TAG"),
    "batch_tag": $(jstr "$BATCH_TAG"),
    "api_image_id": $(jstr "$API_IMAGE_ID")
  },
  "topology": {
    "declared_api_replicas": $(jnum "${PERF_API_REPLICAS:-}"),
    "running_api_containers": $(jnum "${API_RUNNING:-}"),
    "prometheus_api_targets": $(jnum "$UP_API"),
    "prometheus_api_up": $(jnum "$UP_API_HEALTHY"),
    "prometheus_batch_up": $(jnum "$UP_BATCH"),
    "a_host": "$A_HOST"
  },
  "api_declared": {
    "tomcat_max_connections": $(jnum "$TOMCAT_MAX_CONN"),
    "tomcat_accept_count": $(jnum "$TOMCAT_ACCEPT"),
    "tomcat_threads_max": $(jnum "$TOMCAT_THREADS"),
    "db_pool_size_per_instance": $(jnum "$DB_POOL"),
    "java_tool_options": $(jstr "$JAVA_OPTS")
  },
  "api_measured": {
    "hikari_max_total": $(jnum "$HIKARI_TOTAL"),
    "jvm_heap_max_bytes_min": $(jnum "$HEAP_MAX_MIN"),
    "system_cpu_count": $(jnum "$CPU_COUNT")
  },
  "mysql": {
    "max_connections": $(jnum "$MYSQL_MAX_CONN"),
    "version": $(jstr "$MYSQL_VERSION")
  },
  "load_generator": {
    "k6_version": $(jstr "$K6_VERSION"),
    "ephemeral_port_first": $(jnum "$PORT_FIRST"),
    "ephemeral_port_last": $(jnum "$PORT_LAST"),
    "ephemeral_port_count": $( [[ -n "$PORT_FIRST" && -n "$PORT_LAST" ]] && echo $((PORT_LAST - PORT_FIRST + 1)) || echo null ),
    "tcp_msl_ms": $(jnum "$TCP_MSL"),
    "time_wait_before_run": $(jnum "$TIME_WAIT")
  },
  "ping_b_to_a": {
    "count": $PING_N,
    "loss_percent": $(jnum "$PING_LOSS"),
    "min_ms": $(jnum "$PING_MIN"),
    "avg_ms": $(jnum "$PING_AVG"),
    "max_ms": $(jnum "$PING_MAX"),
    "stddev_ms": $(jnum "$PING_STDDEV")
  },
  "profile": {
    "engine": $(jstr "${PERF_ENGINE:-}"),
    "profile": $(jstr "${PERF_PROFILE:-}"),
    "round_stock": $(jnum "${PERF_ROUND_STOCK:-}"),
    "warmup_rate": $(jnum "${PERF_WARMUP_RATE:-}"),
    "warmup_seconds": $(jnum "${PERF_WARMUP_SECONDS:-}")
  },
  "notes": $(printf '%s\n' "${notes[@]:-}" | jq -Rs 'split("\n") | map(select(length > 0))')
}
JSON
