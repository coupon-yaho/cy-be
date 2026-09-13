#!/usr/bin/env bash
# 대조 1회. 반복 디렉터리의 독립 기록과 DB 를 맞대 reconcile-<시각>.json 을 남긴다 (CY-960).
#
#   perf/run/reconcile.sh perf/results/<run-id>/rate-6667/rep-1   반복 하나
#   perf/run/reconcile.sh perf/results/<run-id>                    묶음 전체
#
# **사람이 부를 때만 돈다.** 주기 실행도, 전용 서버도, 화면도 만들지 않는다 — 부하 뒤에
# 한 번, 그리고 복구를 기다린 뒤 한 번 더 부르는 것이 쓰임새 전부다.
#
# ⚠️ 두 대조의 차이는 **상태가 변했다는 사실**이지 변한 이유가 아니다. 미해결이
#    지워진 이유는 늦게 들어온 등록일 수도, 다른 경로의 보상일 수도, 사람이 손댄
#    것일 수도 있다 — 스냅샷 둘로는 못 가른다. 이유를 알려면 그 키의 처리 이력
#    (issuance_histories 의 상태 전이)을 따로 봐야 한다.
#
# **DB 에 쓰지 않는다.** 읽기 질의 셋뿐이고, 결과를 파일로 떨군 뒤 판정은
# reconcile.py 가 한다. 판정을 파이썬에 두는 이유는 픽스처 파일만으로 시험할 수
# 있어서다 — 양쪽 동시 누락도, 재기동도, 늦은 취소 경합도 DB 없이 태울 수 있다.
#
# 종료코드는 reconcile.py 의 것이 그대로 나간다: 0 정상 · 1 결함 · 3 판정 불가 ·
# 4 보류. 덤프가 실패해도 여기서 죽지 않는다 — 반쪽이라도 판정할 수 있는 것은
# 판정하고, 못 하는 유형만 판정 불가로 남기는 쪽이 낫다.
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

TARGET="${1:-}"
[[ -n "$TARGET" ]] || die "디렉터리가 필요하다 — perf/run/reconcile.sh <반복 또는 묶음>"

# **무엇을 받았는지 추측하지 않는다.** round.json 이 있으면 반복이고, rate-*/rep-*
# 아래에 그것이 있으면 묶음이다. 둘 다 아니면 죽는다 — 잘못 짚고 0건을 돌면
# "대조했고 깨끗하다" 로 보인다.
REPS=()
if [[ -f "$TARGET/round.json" ]]; then
  REPS=("$TARGET")
else
  while IFS= read -r r; do REPS+=("$(dirname "$r")"); done \
    < <(find "$TARGET" -mindepth 3 -maxdepth 3 -path '*/rate-*/rep-*/round.json' | sort)
  (( ${#REPS[@]} > 0 )) \
    || die "$TARGET 이 반복(round.json)도 묶음(rate-*/rep-*/round.json)도 아니다"
  log "묶음 대조 — 반복 ${#REPS[@]}개"
fi

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

# 반복마다 DB 를 뜬다. **판정과 집계는 여기서 하지 않는다** — 셸에 둔 집계는
# 픽스처로 시험할 수가 없다. 덤프만 떨구고 파이썬을 한 번 부른다.
for REP in "${REPS[@]}"; do
  (( ${#REPS[@]} > 1 )) && log "덤프 — ${REP#"$TARGET"/}"
  ROUND=$(jq -r '.target_round_id' "$REP/round.json")
  [[ "$ROUND" =~ ^[0-9]+$ ]] || die "round.json 의 target_round_id 가 숫자가 아니다 — $ROUND"

  # 회차 밖으로 나간 발급을 되짚을 유일한 끈은 **성공 응답이 알려 준 예약번호**다.
  #
  # 회차로만 뜨면, 내 접수 키의 발급이 **다른 회차** 앞으로 만들어진 경우 그 행이 덤프에
  # 아예 없어서 대조가 TARGET_MISMATCH 가 아니라 LOST 로 읽는다 — 결함은 잡되 이름이
  # 틀린다. 클라이언트가 받은 번호로 기본 키를 직접 짚으면 인덱스 한 번이라 싸다.
  #
  # ⚠️ 이 보강은 **성공 응답을 받은 건에만** 닿는다. 결과 불명이거나 거절인데 발급이
  #    다른 회차에 생긴 경우는 짚을 번호 자체가 없어 여전히 안 보인다. request_id 에
  #    인덱스가 없어 키로 훑는 길은 열지 않았다.
  REPORTED=""
  if [[ -f "$REP/requests.log" ]]; then
    REPORTED=$(awk -F'\t' '$1 == "CY960" && $2 == "OK" && $4 ~ /^[0-9]+$/ { print $4 }' \
      "$REP/requests.log" | sort -un | paste -sd, -)
  fi
  if [[ -n "$REPORTED" ]]; then
    ID_CLAUSE=" OR i.id IN ($REPORTED)"
    log "성공 응답이 알려 준 예약번호 $(awk -F, '{print NF}' <<<"$REPORTED")개로 회차 밖도 짚는다"
  else
    ID_CLAUSE=""
  fi

  log "대상 회차 $ROUND 의 발급을 읽는다"
  # issued_grade 도 뜬다 — 서버가 보는 **요청 내용**의 한 칸이고
  # (canonicalRequest 가 해시하는 셋 중 하나), 독립 기록과 맞대야 한다.
  dump db-issuances.tsv "
    SELECT i.id, i.coupon_id, i.member_id, i.status, i.issued_grade
    FROM issuances i WHERE i.coupon_id = $ROUND$ID_CLAUSE ORDER BY i.id;"

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
    WHERE (i.coupon_id = $ROUND$ID_CLAUSE) AND h.event_type = 'ISSUE'
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
  # ⚠️ 괄호로 묶는다. MySQL 에서 괄호 없는 LIMIT 은 **UNION ALL 결과 전체**에 걸린다 —
  #    그러면 완료 행 10만 개짜리 정상 회차가 상한에 닿은 것으로 보이고, 아래 검사가
  #    멀쩡한 덤프를 지운다. 상한은 **끝이 없는 두 번째 갈래에만** 필요하다.
  dump db-idempotency.tsv "
    (SELECT r.idem_key, r.status, IFNULL(r.member_id, ''), IFNULL(r.issuance_id, '')
     FROM idempotency_records r
     JOIN issuances i ON i.id = r.issuance_id
     WHERE i.coupon_id = $ROUND)
    UNION ALL
    (SELECT r.idem_key, r.status, IFNULL(r.member_id, ''), IFNULL(r.issuance_id, '')
     FROM idempotency_records r
     WHERE r.status <> 'DONE'
     LIMIT 100001);"

  # 상한에 닿았는지도 **미완료 행만** 센다. 완료 행과 센티널까지 세면 같은 착각이 난다.
  if [[ -f "$REP/db-idempotency.tsv" ]]; then
    pending=$(awk -F'\t' '$1 !~ /^#EOF/ && $2 != "DONE"' "$REP/db-idempotency.tsv" | wc -l)
    if (( pending > 100000 )); then
      log "⚠️ 미완료 멱등 행이 상한 100000 에 닿았다. 그 자체가 검출이다 —
        대조 전에 그쪽부터 본다. 덤프를 지워 판정 불가로 남긴다"
      rm -f "$REP/db-idempotency.tsv"
    fi
  fi
done

# 반복을 전부 넘긴다. 하나가 결함이어도 나머지를 판정하고, 종료코드는 가장 나쁜 것이다.
python3 "$PERF_DIR/run/reconcile.py" "${REPS[@]}"
