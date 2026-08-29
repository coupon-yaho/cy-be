#!/usr/bin/env bash
# ④ 측정 1회. 회차를 새로 만들고(워밍업용 + 측정용) k6 를 돌린 뒤 결과와 환경 메타를 남긴다.
#
#   perf/run/run-round.sh --rate 6667 --seconds 3 --engine V2 --out perf/results/<run>/r1
#
# 워밍업 회차를 따로 두는 이유 — 워밍업 트래픽도 발급이라 같은 회차를 쓰면 재고를
# 먹는다. 재고 1만짜리 회차에 300/s x 25s 를 먼저 쏘면 측정 구간은 전부 매진 거절이다.
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

RATE=""; SECONDS_=""; ENGINE="${PERF_ENGINE:-V2}"; OUT=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --rate) RATE="$2"; shift 2;;
    --seconds) SECONDS_="$2"; shift 2;;
    --engine) ENGINE="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    *) die "모르는 인자: $1";;
  esac
done
: "${RATE:?--rate 가 필요하다}"; : "${SECONDS_:?--seconds 가 필요하다}"; : "${OUT:?--out 이 필요하다}"
mkdir -p "$OUT"

REQUESTS=$(( RATE * SECONDS_ ))
WARMUP_REQUESTS=$(( ${PERF_WARMUP_RATE:-300} * ${PERF_WARMUP_SECONDS:-25} ))
members=$(mysql_exec "SELECT COUNT(*) FROM members;")
need=$(( REQUESTS > WARMUP_REQUESTS ? REQUESTS : WARMUP_REQUESTS ))
(( members >= need )) || die "회원이 $members 명인데 한 구간에 $need 요청이다.
  1인 1매가 회차 단위라 요청 수만큼 서로 다른 회원이 필요하다. perf/seed/seed.sh 로 채운다."

log "워밍업 회차 생성 (재고 ${PERF_WARMUP_STOCK:-1000000})"
WARMUP_ROUND=$(PERF_ENGINE="$ENGINE" PERF_ROUND_STOCK="${PERF_WARMUP_STOCK:-1000000}" \
  "$PERF_DIR/seed/new-round.sh" --engine "$ENGINE" --stock "${PERF_WARMUP_STOCK:-1000000}" --label 워밍업회차)
log "측정 회차 생성 (재고 ${PERF_ROUND_STOCK:-10000})"
TARGET_ROUND=$("$PERF_DIR/seed/new-round.sh" --engine "$ENGINE" --stock "${PERF_ROUND_STOCK:-10000}" --label 측정회차)

# 등급 헤더는 회차의 eligible_grades_mask 에서 뽑는다. k6 에 하드코딩하면 마스크를 바꾼
# 회차에서 전량 등급 거절이 나고 결과에는 "거절 N건"으로만 보인다.
grade_of() {
  mysql_exec "SELECT g.code FROM grades g JOIN coupons c ON c.id = $1
              WHERE (c.eligible_grades_mask & g.bit_value) <> 0
              ORDER BY g.bit_value DESC LIMIT 1;"
}
TARGET_GRADE=$(grade_of "$TARGET_ROUND")
WARMUP_GRADE=$(grade_of "$WARMUP_ROUND")
[[ -n "$TARGET_GRADE" && -n "$WARMUP_GRADE" ]] \
  || die "회차의 eligible_grades_mask 에 맞는 등급이 grades 에 없다. 시드를 다시 본다."
log "등급 헤더 — 측정 $TARGET_GRADE · 워밍업 $WARMUP_GRADE (회차 마스크에서 뽑음)"

# 회차 시작 전 TIME_WAIT. meta.sh 는 k6 뒤에 도니까 그 값에는 이번 회차 몫이 섞인다.
TIME_WAIT_BEFORE=$(netstat -an -p tcp 2>/dev/null | grep -c TIME_WAIT || echo "")
START_EPOCH=$(date +%s)
log "k6 — 설정 도착률 ${RATE}/s x ${SECONDS_}s (설정값이다. 달성치는 결과의 perf.achieved_arrival_rps 를 본다 — http_reqs.rate 가 아니다)"
set +e
k6 run "$PERF_DIR/k6/issue.js" \
  -e "BASE_URL=http://$A_HOST:${PERF_LB_PORT:-8080}" \
  -e "WARMUP_ROUND_ID=$WARMUP_ROUND" \
  -e "TARGET_ROUND_ID=$TARGET_ROUND" \
  -e "WARMUP_RATE=${PERF_WARMUP_RATE:-300}" \
  -e "WARMUP_SECONDS=${PERF_WARMUP_SECONDS:-25}" \
  -e "TARGET_RATE=$RATE" \
  -e "TARGET_SECONDS=$SECONDS_" \
  -e "MEMBER_BASE=${PERF_MEMBER_BASE:-1}" \
  -e "MEMBER_GRADE=$TARGET_GRADE" \
  -e "WARMUP_MEMBER_GRADE=$WARMUP_GRADE" \
  -e "WARMUP_MEMBER_BASE=${PERF_WARMUP_MEMBER_BASE:-1}" \
  -e "HTTP_TIMEOUT=${PERF_HTTP_TIMEOUT:-60s}" \
  -e "OUT_JSON=$OUT/k6-summary.json" \
  2>&1 | tee "$OUT/k6.log"
K6_RC=${PIPESTATUS[0]}
set -e
END_EPOCH=$(date +%s)
[[ -s "$OUT/k6-summary.json" ]] || die "k6 요약이 비어 있다. $OUT/k6.log 를 본다 (rc=$K6_RC)"
# ⚠️ 요약 파일이 있어도 k6 가 0 이 아닌 코드로 끝났으면 그 회차는 온전하지 않다.
#    여기서 죽이지는 않는다 — 남은 반복은 계속 돌아야 하고, 로그도 남겨야 한다.
#    대신 round.json 에 코드를 남기고 summarize.py 가 그 반복을 중앙값에서 뺀다.
(( K6_RC == 0 )) || log "⚠️ k6 가 rc=$K6_RC 로 끝났다. 이 반복은 요약에서 제외된다 — $OUT/k6.log"

log "환경 메타"
PERF_ENGINE="$ENGINE" "$PERF_DIR/env/meta.sh" > "$OUT/meta.json"

log "회차 사후 상태"
# 측정 구간만 잘라서 본다. 유휴가 섞이면 평균이 낙관적으로 나온다.
# ⚠️ k6 프로세스 전체(START_EPOCH~END_EPOCH)를 쓰면 안 된다 — 워밍업 25초와 대기 12초,
#    gracefulStop 이 다 들어간다. k6 가 낸 측정 구간 실제 창을 쓴다.
MW_START=$(jq -r '.perf.measure_window_start_epoch // empty' "$OUT/k6-summary.json")
MW_END=$(jq -r '.perf.measure_window_end_epoch // empty' "$OUT/k6-summary.json")
WINDOW_SOURCE=measure
# 표본이 있는데 창이 0초로 접히는 경우(모든 이터레이션이 같은 밀리초 + 그 값이 정확히
# 초 경계)에는 창을 버리지 않고 1초로 넓힌다. 표본이 있는데 안 재는 것이 더 나쁘다.
if [[ -n "$MW_START" && -n "$MW_END" && "$MW_END" -le "$MW_START" ]]; then
  MW_END=$(( MW_START + 1 ))
fi
if [[ -z "$MW_START" || -z "$MW_END" ]]; then
  # 측정 구간에 이터레이션이 하나도 안 돌면 창이 안 나온다. 추정으로 채우지 않는다.
  log "⚠️ 측정 구간 창을 못 얻었다. scrape 지표를 <측정 실패>로 남긴다"
  WINDOW_SOURCE=unavailable
fi
W=$(( MW_END > MW_START ? MW_END - MW_START : 0 ))
scrape() {
  [[ "$WINDOW_SOURCE" == measure ]] || { echo ""; return; }
  promq_scalar "$1" "time=$MW_END" 2>/dev/null || echo ""
}
jn() { [[ -n "${1:-}" ]] && printf '%s' "$1" || printf 'null'; }

read -r ISSUED STOCK_TOTAL ACTIVE < <(mysql_exec "
  SELECT (SELECT COUNT(*) FROM issuances WHERE coupon_id = $TARGET_ROUND),
         cs.total_quantity, cs.active_count
  FROM coupon_stocks cs WHERE cs.coupon_id = $TARGET_ROUND;")
DUP=$(mysql_exec "SELECT COUNT(*) FROM (SELECT member_id FROM issuances
  WHERE coupon_id = $TARGET_ROUND GROUP BY member_id HAVING COUNT(*) > 1) t;")

cat > "$OUT/round.json" <<JSON
{
  "engine": "$ENGINE",
  "member_grade": "$TARGET_GRADE",
  "warmup_round_id": $WARMUP_ROUND,
  "target_round_id": $TARGET_ROUND,
  "configured_rate_per_sec": $RATE,
  "configured_seconds": $SECONDS_,
  "configured_requests": $REQUESTS,
  "window": {
    "k6_process_start_epoch": $START_EPOCH,
    "k6_process_end_epoch": $END_EPOCH,
    "measure_start_epoch": $(jn "${MW_START:-}"),
    "measure_end_epoch": $(jn "${MW_END:-}"),
    "measure_seconds": $W,
    "source": "$WINDOW_SOURCE"
  },
  "k6_exit_code": $K6_RC,
  "time_wait_before_k6": $(jn "${TIME_WAIT_BEFORE:-}"),
  "db_after": {
    "issuances": $(jn "$ISSUED"),
    "stock_total": $(jn "$STOCK_TOTAL"),
    "active_count": $(jn "$ACTIVE"),
    "over_issued": $(( ${ISSUED:-0} > ${STOCK_TOTAL:-0} ? 1 : 0 )),
    "members_with_two_or_more": $(jn "$DUP")
  },
  "scrape_health": {
    "_note": "avg_over_time 류는 인스턴스마다 한 줄씩 나온다. 감싸지 않으면 아무 한 대만 기록된다 — 전 인스턴스를 포괄하도록 min/avg/max 로 감쌌다",
    "api_up_avg_worst": $(jn "$(scrape "min(avg_over_time(up{job=\"api\"}[${W}s]))")"),
    "api_up_avg_mean": $(jn "$(scrape "avg(avg_over_time(up{job=\"api\"}[${W}s]))")"),
    "api_scrape_duration_p99_worst": $(jn "$(scrape "max(quantile_over_time(0.99, scrape_duration_seconds{job=\"api\"}[${W}s]))")"),
    "api_scrape_samples_max": $(jn "$(scrape "max(max_over_time(scrape_samples_scraped{job=\"api\"}[${W}s]))")"),
    "api_cpu_max": $(jn "$(scrape "max(max_over_time(process_cpu_usage{job=\"api\"}[${W}s]))")"),
    "api_cpu_max_mean": $(jn "$(scrape "avg(max_over_time(process_cpu_usage{job=\"api\"}[${W}s]))")")
  }
}
JSON
log "결과 → $OUT"
