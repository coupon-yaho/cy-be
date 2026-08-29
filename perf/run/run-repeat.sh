#!/usr/bin/env bash
# 같은 조건을 PERF_REPEATS 회 반복하고 중앙값을 낸다.
#
# ⚠️ 표본 하나로 두 변형을 비교하지 않는다. 같은 이미지·같은 300/s 를 세 번 쟀는데
#    med 가 3ms · 82ms · 224ms 로 흔들렸다(실측). 표본 하나면 잡음과 차이를 구분할 수 없다.
#
#   perf/run/run-repeat.sh                 # perf.env 의 PERF_PROFILE 대로
#   perf/run/run-repeat.sh --engine V1     # 같은 스크립트로 v1 회차
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

ENGINE="${PERF_ENGINE:-V2}"; PROFILE="${PERF_PROFILE:-spike}"; REPEATS="${PERF_REPEATS:-5}"
RUN_ID=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --engine) ENGINE="$2"; shift 2;;
    --profile) PROFILE="$2"; shift 2;;
    --repeats) REPEATS="$2"; shift 2;;
    --id) RUN_ID="$2"; shift 2;;
    *) die "모르는 인자: $1";;
  esac
done
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)-$ENGINE-$PROFILE}"
RUN_DIR="$PERF_DIR/results/$RUN_ID"
mkdir -p "$RUN_DIR"
log "회차 묶음 $RUN_ID — engine=$ENGINE profile=$PROFILE repeats=$REPEATS"

"$PERF_DIR/env/preflight.sh" | tee "$RUN_DIR/preflight.log"

case "$PROFILE" in
  spike)
    # 재고 1만 · 요청 2만 · 도착 1~3초 (docs/12 §10.2 의 스파이크 축)
    REQ="${PERF_SPIKE_REQUESTS:-20000}"; SEC="${PERF_SPIKE_SECONDS:-3}"
    RATES=$(( (REQ + SEC - 1) / SEC )); SECS="$SEC"
    STEPS=("$RATES:$SECS")
    ;;
  steps)
    # 계단은 단계마다 회차가 다르다. run-round.sh 가 호출마다 회차를 새로 만든다.
    SECS="${PERF_STEP_SECONDS:-20}"
    STEPS=()
    IFS=, read -ra rs <<< "${PERF_STEP_RATES:-200,400,600,800,1000}"
    for r in "${rs[@]}"; do STEPS+=("$r:$SECS"); done
    ;;
  *) die "모르는 프로필: $PROFILE (spike | steps)";;
esac

for step in "${STEPS[@]}"; do
  rate="${step%%:*}"; secs="${step##*:}"
  for ((i = 1; i <= REPEATS; i++)); do
    out="$RUN_DIR/rate-$rate/rep-$i"
    log "── rate=$rate/s x ${secs}s · 반복 $i/$REPEATS"
    PERF_ENGINE="$ENGINE" "$PERF_DIR/run/run-round.sh" \
      --rate "$rate" --seconds "$secs" --engine "$ENGINE" --out "$out"
    # 앞 회차의 TIME_WAIT 가 다음 회차의 가용 포트를 깎는다. msl 만큼은 비운다.
    sleep "${PERF_COOLDOWN_SECONDS:-30}"
  done
done

python3 "$PERF_DIR/run/summarize.py" "$RUN_DIR" | tee "$RUN_DIR/summary.txt"
log "완료 → $RUN_DIR"
