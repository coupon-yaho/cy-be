#!/usr/bin/env bash
# ② 시드 — 회원 · 더미 발급. 회차는 만들지 않는다(회차는 new-round.sh 가 회차마다 새로).
#
# 멱등이다. 이미 있는 만큼은 건너뛰고 모자란 만큼만 채운다.
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

MEMBERS="${PERF_MEMBERS:-200000}"
DUMMY="${PERF_DUMMY_ISSUANCES:-300000}"
TID="${PERF_TEMPLATE_ID:-9001}"
BID="${PERF_BRAND_ID:-9001}"
DUMMY_ROUND_ID="${PERF_DUMMY_ROUND_ID:-9000}"

log "기반 데이터 (grades · brands · templates)"
mysql_exec "SET @BRAND_ID=$BID; SET @TEMPLATE_ID=$TID; $(cat "$PERF_DIR/seed/00-base.sql")"

# ⚠️ COUNT 가 아니라 MAX(id) 로 이어 붙인다. COUNT 로 정하면 기존 데이터에 구멍이 있을 때
#    이미 있는 id 와 부딪히거나, 채운 뒤에도 1..N 이 연속이 아니게 된다 — k6 는 1 부터
#    연속된 id 를 쓰므로 그 구멍이 전량 500(members FK 위반) 으로 나온다.
have=$(mysql_exec "SELECT COALESCE(MAX(id), 0) FROM members;")
log "members MAX(id) $have / 목표 $MEMBERS"
if (( have < MEMBERS )); then
  # 재귀 CTE 로 번호를 만든다. 기본 재귀 상한이 1000 이라 반드시 올려야 한다 —
  # 안 올리면 'Recursive query aborted' 로 죽고, 그 메시지가 원인을 안 알려 준다.
  mysql_exec "
    SET SESSION cte_max_recursion_depth = 100000000;
    INSERT INTO members(id, membership_grade, created_at)
    WITH RECURSIVE n(x) AS (
      SELECT $((have + 1)) UNION ALL SELECT x + 1 FROM n WHERE x < $MEMBERS
    )
    SELECT x, ELT(1 + (x % 4), 'WELCOME','SILVER','GOLD','VIP'), NOW(6) FROM n;"
  log "members 채움 → COUNT $(mysql_exec 'SELECT COUNT(*) FROM members;')"
fi

# 1..MEMBERS 가 실제로 빈틈없이 있는지 확인한다. 개수만 세면 구멍을 못 잡는다.
read -r cnt mn mx < <(mysql_exec "SELECT COUNT(*), MIN(id), MAX(id) FROM members WHERE id BETWEEN 1 AND $MEMBERS;")
if [[ "$cnt" != "$MEMBERS" || "$mn" != "1" ]]; then
  die "members 의 1..$MEMBERS 구간에 구멍이 있다 (있는 행 $cnt · 최소 id $mn · 최대 id $mx).
  k6 는 1 부터 연속된 id 를 쓴다. 구멍이 있으면 그 요청은 members FK 위반으로 500 이 난다."
fi
log "members 1..$MEMBERS 연속 확인"

log "더미 발급용 회차 $DUMMY_ROUND_ID"
mysql_exec "
  INSERT INTO coupons(id, template_id, brand_id, name, policy_type, discount_rate,
    max_discount_amount, min_order_amount, valid_days, eligible_grades_mask,
    open_at, close_at, status, generated_at, created_at,
    issuance_engine_version, issuance_engine_locked)
  VALUES ($DUMMY_ROUND_ID, $TID, $BID, '더미발급용', 'PERCENT_CAPPED', 10, 1000, 0, 30, 15,
    '2000-01-01 00:00:00', '2000-01-02 00:00:00', 'CLOSED', NOW(6), NOW(6), 'V1', 0)
  ON DUPLICATE KEY UPDATE name = VALUES(name);
  INSERT INTO coupon_stocks(coupon_id, total_quantity, active_count, updated_at)
  VALUES ($DUMMY_ROUND_ID, $DUMMY, 0, NOW())
  ON DUPLICATE KEY UPDATE total_quantity = GREATEST(total_quantity, VALUES(total_quantity));"

haved=$(mysql_exec "SELECT COUNT(*) FROM issuances WHERE coupon_id = $DUMMY_ROUND_ID;")
log "더미 issuances 현재 $haved / 목표 $DUMMY"
if (( haved < DUMMY )); then
  per=$(( (DUMMY + MEMBERS - 1) / MEMBERS ))   # 회원당 몇 장을 얹을지
  # code 는 char(16) UNIQUE 다. 랜덤으로 만들면 30만 행에서 충돌이 나고, 충돌은
  # INSERT 전체를 되돌린다. 결정적으로 만든다 — LPAD(member,10) + LPAD(seq,6) = 16자.
  mysql_exec "
    SET SESSION cte_max_recursion_depth = 100000000;
    INSERT IGNORE INTO issuances(coupon_id, member_id, code, issued_grade, status,
      issued_at, expires_at, updated_at)
    SELECT $DUMMY_ROUND_ID, m.id,
           CONCAT(LPAD(m.id, 10, '0'), LPAD(s.x, 6, '0')),
           m.membership_grade, 'ISSUED',
           NOW(6), NOW(6) + INTERVAL 30 DAY, NOW(6)
    FROM members m
    CROSS JOIN (
      WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM s WHERE x < $per)
      SELECT x FROM s
    ) s
    LIMIT $DUMMY;"
  log "더미 issuances → $(mysql_exec "SELECT COUNT(*) FROM issuances WHERE coupon_id = $DUMMY_ROUND_ID;")"
fi

log "시드 완료"
