#!/usr/bin/env bash
# 대조 1회. 반복 디렉터리의 독립 기록과 DB 를 맞대 reconcile-<시각>.json 을 남긴다 (CY-960).
#
#   perf/run/reconcile.sh perf/results/<run-id>/rate-6667/rep-1
#
# **사람이 부를 때만 돈다.** 주기 실행도, 전용 서버도, 화면도 만들지 않는다 — 부하 뒤에
# 한 번, 그리고 복구를 기다린 뒤 한 번 더 부르는 것이 쓰임새 전부다. 두 번째 대조가
# 첫 번째의 "미해결" 을 지워 주면 그것은 늦게 들어온 등록이고, 안 지워지면 유실이다.
#
# **DB 에 쓰지 않는다.** 읽기 질의 셋뿐이고, 결과를 파일로 떨군 뒤 판정은
# reconcile.py 가 한다. 판정을 파이썬에 두는 이유는 픽스처 파일만으로 시험할 수
# 있어서다 — 양쪽 동시 누락도, 재기동도, 늦은 취소 경합도 DB 없이 태울 수 있다.
#
# 종료코드는 reconcile.py 의 것이 그대로 나간다: 0 정상 · 1 결함 · 3 판정 불가 ·
# 4 보류. 덤프가 실패해도 여기서 죽지 않는다 — 반쪽이라도 판정할 수 있는 것은
# 판정하고, 못 하는 유형만 판정 불가로 남기는 쪽이 낫다.
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

REP="${1:-}"
[[ -n "$REP" ]] || die "반복 디렉터리가 필요하다 — perf/run/reconcile.sh <rep 디렉터리>"
[[ -f "$REP/round.json" ]] || die "$REP/round.json 이 없다. 회차 결과 디렉터리가 맞나"

ROUND=$(jq -r '.target_round_id' "$REP/round.json")
[[ "$ROUND" =~ ^[0-9]+$ ]] || die "round.json 의 target_round_id 가 숫자가 아니다 — $ROUND"

# 덤프가 중간에 끊긴 것과 DB 가 빈 것은 다르다. 끝에 `#EOF<탭><행수>` 를 붙여
# 읽는 쪽이 그것을 가르게 한다. 안 붙이면 **잘린 파일이 곧 전량 유실로 보인다.**
dump() {
  local name="$1" sql="$2" rows
  local out="$REP/$name"
  if ! rows=$(mysql_exec "$sql"); then
    log "⚠️ $name 덤프 실패 — 그 유형은 판정 불가로 남는다"
    rm -f "$out"
    return 0
  fi
  # 빈 결과에서 `wc -l` 이 1 이 되지 않게 한다. printf 로 줄을 직접 센다.
  if [[ -z "$rows" ]]; then
    printf '#EOF\t0\n' > "$out"
  else
    printf '%s\n' "$rows" > "$out"
    printf '#EOF\t%s\n' "$(printf '%s\n' "$rows" | wc -l | tr -d ' ')" >> "$out"
  fi
  log "$name — $(grep -c '' "$out")줄 (센티널 포함)"
}

log "대상 회차 $ROUND 의 발급을 읽는다"
dump db-issuances.tsv "
  SELECT id, coupon_id, member_id, status
  FROM issuances WHERE coupon_id = $ROUND ORDER BY id;"

# 발급마다 접수 키가 하나 붙어 있다. **이것이 조인 축이다.**
#
# IssuanceHistory.issue(issuanceId, requestId, ...) 에 멱등 키가 그대로 들어간다 —
# V2CouponIssueService · CouponIssueService 둘 다 그렇다. 스키마 주석도 "F4 —
# idempotency_records.idem_key 와 연결" 이라고 적어 뒀다.
#
# ⚠️ request_id 로 긁지 않는다. 거기엔 인덱스가 없다(있는 것은 created_at 과
#    issuance_id 로 시작하는 둘뿐). 회차의 발급 id 로 조인해야 그 인덱스를 탄다.
log "발급마다 붙은 접수 키를 읽는다 (ISSUE 이력만 — 상태 전이까지 끌면 발급 하나에 여러 줄이다)"
dump db-histories.tsv "
  SELECT h.issuance_id, IFNULL(h.request_id, '')
  FROM issuance_histories h
  JOIN issuances i ON i.id = h.issuance_id
  WHERE i.coupon_id = $ROUND AND h.event_type = 'ISSUE'
  ORDER BY h.issuance_id;"

# 멱등 레코드는 두 갈래로 모은다.
#
#   ① 완료된 것 — 발급을 통해 회차로 건다.
#   ② 완료가 아닌 것 — **회차로 걸 수가 없다.** ck_idempotency_status_targets 가
#      IN_PROGRESS 행의 member_id·issuance_id 를 NULL 로 못박아서, 이 행을 회차에
#      잇는 유일한 끈은 idem_key 뿐이고 그 키를 아는 것은 독립 기록이다.
#      그래서 전부 가져와 파이썬이 자기 키 집합으로 거른다.
#
# ② 는 인덱스가 없어 풀 스캔이다. 정상 회차에서 이 행은 0에 가깝고(발급이 끝나면
# DONE 으로 바뀐다), 수십만 행이 나온다면 그 사실 자체가 검출이다. 그래서 상한을
# 두되 **상한에 닿았는지를 알 수 있게** 한 줄 더 받아 온다.
log "멱등 레코드를 읽는다 (완료분은 회차로, 미완료분은 전량 — 인덱스가 없어 풀 스캔이다)"
dump db-idempotency.tsv "
  SELECT r.idem_key, r.status, IFNULL(r.member_id, ''), IFNULL(r.issuance_id, '')
  FROM idempotency_records r
  JOIN issuances i ON i.id = r.issuance_id
  WHERE i.coupon_id = $ROUND
  UNION ALL
  SELECT r.idem_key, r.status, IFNULL(r.member_id, ''), IFNULL(r.issuance_id, '')
  FROM idempotency_records r
  WHERE r.status <> 'DONE'
  LIMIT 100001;"

if [[ -f "$REP/db-idempotency.tsv" ]] \
   && (( $(grep -c '' "$REP/db-idempotency.tsv") > 100001 )); then
  log "⚠️ 멱등 덤프가 상한 100000 에 닿았다. 미완료 행이 그만큼 쌓였다는 뜻이고,
      그 자체가 검출이다 — 대조 전에 그쪽부터 본다. 덤프를 지워 판정 불가로 남긴다"
  rm -f "$REP/db-idempotency.tsv"
fi

python3 "$PERF_DIR/run/reconcile.py" "$REP"
