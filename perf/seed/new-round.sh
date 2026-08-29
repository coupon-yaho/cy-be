#!/usr/bin/env bash
# 회차 하나를 새로 만들어 열고, 회차 id 를 stdout 으로 낸다(로그는 전부 stderr).
#
#   ROUND_ID=$(perf/seed/new-round.sh --engine V2 --stock 10000)
#
# ⚠️ 회차를 재사용하지 않는 이유 — 1인 1매가 회차 단위다. 같은 회차를 두 번 쓰면
#    두 번째부터 전부 ALREADY_ISSUED(COUPON-305)가 나고, 그건 발급 경로가 아니라
#    멱등 경로의 지연을 재는 것이 된다.
# ⚠️ coupons 에 uk_template_open (template_id, open_at) 이 있다. 회차를 여러 개 만들려면
#    open_at 이 서로 달라야 한다 — 아래에서 MAX(open_at)+1s 로 단조 증가시킨다.
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

ENGINE="${PERF_ENGINE:-V2}"
STOCK="${PERF_ROUND_STOCK:-10000}"
LEAD="${PERF_ROUND_OPEN_LEAD:-25}"    # 워밍업은 NOW < open_at 일 때만 된다
LABEL="측정회차"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --engine) ENGINE="$2"; shift 2;;
    --stock)  STOCK="$2";  shift 2;;
    --label)  LABEL="$2";  shift 2;;
    *) die "모르는 인자: $1";;
  esac
done
[[ "$ENGINE" == V1 || "$ENGINE" == V2 ]] || die "engine 은 V1 또는 V2 다 (받은 값: $ENGINE)"

TID="${PERF_TEMPLATE_ID:-9001}"
BID="${PERF_BRAND_ID:-9001}"

read -r ROUND_ID OPEN_EPOCH < <(mysql_exec "
  SET @oid  = (SELECT COALESCE(MAX(id), 1000000) + 1 FROM coupons);
  SET @open = GREATEST(
        NOW() + INTERVAL $LEAD SECOND,
        COALESCE((SELECT MAX(open_at) FROM coupons WHERE template_id = $TID), '2000-01-01')
          + INTERVAL 1 SECOND);
  INSERT INTO coupons(id, template_id, brand_id, name, policy_type, discount_rate,
    max_discount_amount, min_order_amount, valid_days, eligible_grades_mask,
    open_at, close_at, status, generated_at, created_at,
    issuance_engine_version, issuance_engine_locked)
  VALUES (@oid, $TID, $BID, '$LABEL-$ENGINE', 'PERCENT_CAPPED', 10, 1000, 0, 30, 15,
    @open, @open + INTERVAL 2 HOUR, 'SCHEDULED', NOW(6), NOW(6), '$ENGINE', 0);
  INSERT INTO coupon_stocks(coupon_id, total_quantity, active_count, updated_at)
  VALUES (@oid, $STOCK, 0, NOW());
  -- FLOOR 를 씌운다. @open 이 datetime(6) 이라 그냥 두면 '...000000' 이 붙어
  -- 셸 산술이 깨진다.
  SELECT @oid, FLOOR(UNIX_TIMESTAMP(@open));")

[[ -n "${ROUND_ID:-}" ]] || die "회차 생성 실패"
log "회차 $ROUND_ID 생성 — engine=$ENGINE stock=$STOCK open_at=$(date -r "$OPEN_EPOCH" '+%T' 2>/dev/null || echo "$OPEN_EPOCH")"

if [[ "$ENGINE" == V2 ]]; then
  # ⚠️ 워밍업은 열리기 전에만 된다. 열린 뒤에는 ROUND_ALREADY_OPENED 로 거절되고,
  #    그 회차의 Redis 키를 다시 만들 통로가 지금은 없다(v2 프로토타입 확인 §3).
  out=$(net_post "http://batch:${BATCH_PORT:-9091}/internal/v1/coupon-rounds/$ROUND_ID/warmup") \
    || die "워밍업 호출 실패 — batch 가 떠 있는지 본다"
  case "$out" in
    *'"status":"WARMED"'*) log "워밍업 OK — $out";;
    *) die "워밍업이 WARMED 가 아니다: $out";;
  esac
fi

now=$(date +%s)
if (( OPEN_EPOCH > now )); then
  log "open_at 까지 $((OPEN_EPOCH - now))초 대기"
  sleep $((OPEN_EPOCH - now + 1))
fi
mysql_exec "UPDATE coupons SET status = 'OPEN' WHERE id = $ROUND_ID;" >/dev/null
log "회차 $ROUND_ID OPEN"
echo "$ROUND_ID"
