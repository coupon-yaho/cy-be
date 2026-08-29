#!/usr/bin/env bash
# 회차 전 점검. 하나라도 걸리면 회차를 열지 않는다.
# 여기서 막는 것들은 전부 실제로 밟아 회차를 버린 적이 있는 항목이다.
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

META=$("$PERF_DIR/env/meta.sh") || die "메타 수집 실패"
q() { printf '%s' "$META" | jq -r "$1"; }

fail=0
ok()   { printf '  \033[32m✓\033[0m %s\n' "$*"; }
bad()  { printf '  \033[31m✗\033[0m %s\n' "$*"; fail=1; }
warn() { printf '  \033[33m!\033[0m %s\n' "$*"; }

echo "── 토폴로지"
want="${PERF_API_REPLICAS:-4}"
targets=$(q '.topology.prometheus_api_targets'); running=$(q '.topology.running_api_containers')
[[ "$running" == "$want" ]] && ok "api 컨테이너 $running 대" || bad "api 컨테이너가 $running 대다 (선언 $want)"
# ⚠️ static_configs: ['api:9090'] 이면 이 값이 항상 1 이다 — 4대 중 한 대만 긁는다.
#    perf/env/prometheus.perf.yml 의 dns_sd 가 안 걸린 것이다.
[[ "$targets" == "$want" ]] && ok "Prometheus api 타깃 $targets 개" \
  || bad "Prometheus api 타깃이 $targets 개다 (기대 $want). prometheus.perf.yml 이 안 물렸다"
[[ "$(q '.topology.prometheus_api_up')" == "$want" ]] && ok "api up=1 이 $want 개" \
  || bad "up{job=\"api\"} 합이 $(q '.topology.prometheus_api_up') 이다"
[[ "$(q '.topology.prometheus_batch_up')" == "1" ]] && ok "batch up" || warn "batch up 이 1 이 아니다 — 도메인 Gauge 가 통째로 batch 에서 나온다"

echo "── 서버 수용 한계"
# 기본 4000 + 1000 = 5,000 상한. VU 2만을 쏘면 그 위는 연결이 아예 안 되고,
# 응답이 아니라 duration 0s 로 찍힌다.
mc=$(q '.api_declared.tomcat_max_connections'); ac=$(q '.api_declared.tomcat_accept_count')
need="${PERF_SPIKE_REQUESTS:-20000}"
if [[ "$mc" != null && $((mc + ac)) -ge $need ]]; then
  ok "톰캣 수용 $((mc + ac)) (max-connections $mc + accept-count $ac) ≥ 요청 $need"
else
  bad "톰캣 수용 상한이 요청 $need 보다 작다 (max-connections $mc · accept-count $ac).
      기본값이면 5,000 이고 그 위는 duration 0s 로 찍힌다"
fi

echo "── JVM 힙"
jopts=$(q '.api_declared.java_tool_options')
if [[ "$jopts" == *-Xmx* ]]; then
  ok "힙 명시 — $jopts (실측 heap max $(q '.api_measured.jvm_heap_max_bytes_min') bytes)"
else
  # 컨테이너 메모리의 25% 가 기본이다. 3g 제한이면 힙 768MB 이고, 이것 때문에 api 가
  # OOMKilled 로 세 번 죽었다.
  bad "-Xmx 가 없다. 컨테이너 메모리의 25% 가 기본이라 3g 제한이면 힙 768MB 다"
fi

echo "── DB"
mmc=$(q '.mysql.max_connections')
[[ "$mmc" == "${PERF_MYSQL_MAX_CONNECTIONS:-50}" ]] && ok "MySQL max_connections $mmc" \
  || warn "MySQL max_connections $mmc — 선언 ${PERF_MYSQL_MAX_CONNECTIONS:-50} 과 다르다. 결과에 그대로 기록된다"
hik=$(q '.api_measured.hikari_max_total')
# ⚠️ 이 합계에는 운영 풀과 관측 풀이 함께 들어 있다. 인스턴스당 운영 풀 x 대수와
#    다른 것이 정상이다 — 관측 쿼리가 운영 풀을 점유하지 않게 분리해 뒀기 때문이다.
ok "Hikari max 합계 $hik (운영 풀 인스턴스당 $(q '.api_declared.db_pool_size_per_instance') x ${want}대 + 관측 풀)"

echo "── 부하기(B) 와 무선 구간"
# 발급 1건이 api→MySQL 왕복을 3~4회 낸다. 그 구간이 무선을 타면 재는 게 v1/v2 차이가
# 아니라 와이파이 품질이 된다. B→A 구간만 무선이고, 그 품질을 회차마다 남긴다.
loss=$(q '.ping_b_to_a.loss_percent'); sd=$(q '.ping_b_to_a.stddev_ms'); avg=$(q '.ping_b_to_a.avg_ms')
jmax="${PERF_PING_JITTER_MAX_MS:-10}"
if [[ "$avg" == null ]]; then
  bad "ping 을 못 쟀다. A_HOST=$A_HOST · 윈도우 방화벽 인바운드와 네트워크 프로필(공용이면 규칙이 있어도 막힌다)을 본다"
else
  [[ "$loss" == "0" || "$loss" == "0.0" ]] && ok "ping 손실 0% (avg ${avg}ms)" || bad "ping 손실 ${loss}%"
  awk -v s="$sd" -v m="$jmax" 'BEGIN{exit !(s+0 <= m+0)}' \
    && ok "ping 지터 stddev ${sd}ms ≤ ${jmax}ms" \
    || bad "ping 지터 stddev ${sd}ms > ${jmax}ms — 이 회차는 버린다"
fi

ports=$(q '.load_generator.ephemeral_port_count'); tw=$(q '.load_generator.time_wait_before_run')
# macOS 기본 49152~65535 = 16,384개. 앞 회차의 TIME_WAIT 가 남아 다음 회차의 가용 포트를 깎는다.
# cy-631 회차의 통과 요청 16,777 이 이 숫자와 거의 같았다 — 서버 처리량이 아니라 클라이언트 한계였다.
if [[ "$ports" != null && "$ports" -lt "$need" ]]; then
  bad "임시 포트가 $ports 개인데 요청은 $need 이다. 클라이언트가 먼저 병목이 된다.
      sudo sysctl -w net.inet.ip.portrange.first=16384
      sudo sysctl -w net.inet.tcp.msl=1000"
else
  ok "임시 포트 $ports 개 ≥ 요청 $need"
fi
[[ "$tw" != null && "$tw" -gt 2000 ]] \
  && warn "TIME_WAIT 이 $tw 개 남아 있다. 앞 회차 잔여다 — msl 만큼 기다리거나 회차 간격을 늘린다" \
  || ok "TIME_WAIT $tw 개"

echo "── 재현성"
d=$(q '.repo.dirty_files')
[[ "$d" == "0" ]] && ok "워크트리 깨끗 ($(q '.repo.commit' | cut -c1-8))" \
  || warn "워크트리에 미커밋 $d 건. 이미지 태그의 SHA 가 내용을 안 가리킨다"

echo
(( fail == 0 )) && log "preflight 통과" || die "preflight 실패 — 위 ✗ 를 고치기 전에는 회차를 열지 않는다"
