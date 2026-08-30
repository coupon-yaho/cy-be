#!/usr/bin/env bash
# 검증용 셋에 Spring Batch 메타 스키마를 붓는다.
#
# 왜 필요한가
#   cy-seed 의 ddl/ 은 BATCH_* 를 **일부러** 안 만든다 — 잡 실행 이력은 도메인이 아니고
#   그 DDL 의 주인은 이 저장소의 V11 이다(seed-ddl/README.md 가 그 경계를 적는다).
#   그래서 재시드는 스키마를 새로 만들면서 이 테이블들을 매번 지운다.
#
#   그 자체는 설계대로다. 문제는 **다시 붓는 것을 사람이 기억해야 한다**는 점이고,
#   실제로 2026-08-30 에 두 번 잊었다. 두 번 다 증상이 같았다 —
#   Table 'coupon_clean.BATCH_JOB_EXECUTION' doesn't exist 로 배치가 안 뜨고,
#   관제 화면에서 그 데이터셋 카드가 통째로 사라진다.
#
#   SchemaPresenceGuard 는 이미 처방을 파일 이름까지 정확히 알려 준다. 부족한 것은
#   진단이 아니라 **절차가 한 번에 끝나지 않는다**는 것이라, 그 절차를 여기 묶는다.
#
# 왜 cy-seed 가 아니라 여기인가
#   DDL 을 cy-seed 로 복사하면 V11 이 두 벌이 되고, 그것은 seed-ddl 사본 대조 장치가
#   막으려는 바로 그 상태다. 이 스크립트는 **이 저장소의 마이그레이션 파일을 그대로 읽는다** —
#   사본을 안 만든다.
#
# 쓰는 법
#   scripts/pour-batch-meta.sh coupon_clean
#   MYSQL_CONTAINER=cy-mysql-1 scripts/pour-batch-meta.sh coupon_clean coupon_corrupt
set -euo pipefail

CONTAINER="${MYSQL_CONTAINER:-cy-mysql-1}"
MIGRATIONS_DIR="$(cd "$(dirname "$0")/.." && pwd)/storage/src/main/resources/db/migration"

# SchemaPresenceGuard.META_MIGRATIONS 와 **같은 셋**이어야 한다. 갈리면 이 스크립트를
# 돌리고도 가드가 거절하고, 그때 원인이 스크립트에 있다는 것을 아무도 못 본다.
FILES=(
  "V11__batch_metadata.sql"
  "V2026082513__ix_batch_job_execution_lookup.sql"
  "V2026082514__ix_batch_job_execution_history.sql"
)

# 가드가 실제로 이름으로 묻는 테이블(VerificationRuleJdbcAdapter.BATCH_META_TABLES).
# "BATCH_ 로 시작하는 것이 아홉 개" 로 세면 안 된다 — 엉뚱한 BATCH_* 가 자리를 채워도
# 통과한다. 가드는 이름을 묻지 개수를 안 센다.
REQUIRED_TABLES=(
  "BATCH_JOB_INSTANCE"
  "BATCH_JOB_EXECUTION"
  "BATCH_STEP_EXECUTION"
  "BATCH_JOB_EXECUTION_PARAMS"
)

# 가드의 셋째 축(CRITICAL_INDEXES). 이름만이 아니라 **선두 컬럼**을 본다 —
# 같은 이름의 다른 모양이 통과하면 되읽기는 여전히 전체를 훑는다(CY-686).
REQUIRED_INDEXES=(
  "BATCH_JOB_EXECUTION|IX_JOB_EXEC_STATUS_END|STATUS,END_TIME"
  "BATCH_JOB_EXECUTION|IX_JOB_EXEC_CREATE_TIME|CREATE_TIME"
)

if [ $# -eq 0 ]; then
  echo "사용법: $0 <스키마> [스키마...]" >&2
  exit 2
fi

# **비밀번호를 호스트 셸로 안 꺼낸다.** $(...) 로 받아 -e MYSQL_PWD=<값> 으로 넘기면
# 그 값이 호스트 docker 프로세스의 명령행에 실려 ps 로 보인다. 컨테이너 **안에서**
# MYSQL_ROOT_PASSWORD 를 MYSQL_PWD 로 옮긴다.
mysql_in() {
  local schema="$1"; shift
  docker exec -i "$CONTAINER" sh -c \
    'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --default-character-set=utf8mb4 "$@"' \
    _ "$schema" "$@"
}

# 적용 전용. --force 로 끝까지 달린다 — 오류 분류는 호출부가 한다.
mysql_force() {
  local schema="$1"
  docker exec -i "$CONTAINER" sh -c \
    'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --force --default-character-set=utf8mb4 "$1"' \
    _ "$schema"
}

has_table() {
  local schema="$1" table="$2"
  [ "$(mysql_in "$schema" -N -e \
      "SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema=DATABASE() AND table_name='$table';" 2>/dev/null | tr -d ' \r')" = "1" ]
}

# 선두 컬럼이 기대와 같은지까지 본다. 뒤에 컬럼이 더 붙은 것은 그 질의를 그대로
# 태우므로 통과시킨다 — SchemaPresenceGuard.satisfies 와 같은 규칙이다.
index_prefix() {
  local schema="$1" table="$2" index="$3" n="$4"
  mysql_in "$schema" -N -e \
    "SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index)
       FROM information_schema.statistics
      WHERE table_schema=DATABASE() AND table_name='$table'
        AND index_name='$index' AND is_visible='YES' AND seq_in_index <= $n;" 2>/dev/null | tr -d ' \r'
}

verify_schema() {
  local schema="$1" t idx name cols got n
  for t in "${REQUIRED_TABLES[@]}"; do
    has_table "$schema" "$t" || { echo "    ✗ 테이블 없음: $t" >&2; return 1; }
  done
  for idx in "${REQUIRED_INDEXES[@]}"; do
    t="${idx%%|*}"; name="$(echo "$idx" | cut -d'|' -f2)"; cols="${idx##*|}"
    n=$(( $(echo "$cols" | tr ',' '\n' | wc -l) ))
    got="$(index_prefix "$schema" "$t" "$name" "$n")"
    [ "$got" = "$cols" ] || {
      echo "    ✗ 인덱스가 다르다: $t.$name 기대=$cols 실제=${got:-없음}" >&2; return 1; }
  done
  return 0
}

for schema in "$@"; do
  echo "▶ $schema"

  # 재시드 뒤 습관적으로 부르는 것이 목적이라 멱등이어야 한다. 이미 온전하면
  # 아무것도 안 건드리고 넘어간다.
  if verify_schema "$schema" 2>/dev/null; then
    echo "  이미 온전하다 — 건너뛴다"
    continue
  fi

  # **부분 적용에서도 다시 달릴 수 있어야 한다.** 사전 확인만으로는 부족하다 —
  # V11 이 문장 단위로 반쯤 적용된 상태에서 다시 부르면 첫 CREATE TABLE 이
  # "이미 있다" 로 죽고 나머지를 못 만든다. 스스로 만든 상태에서 못 빠져나온다.
  #
  # 그래서 --force 로 **끝까지 달리되 나온 것을 분류한다.** 넘기는 것은 이 넷뿐이다.
  #   1050 Table already exists   · 1061 Duplicate key name
  #   1007 Database exists        · 1826 Duplicate foreign key
  # --force 를 그냥 쓰면 **모든** SQL 오류를 삼켜 중간 실패가 뒤의 성공에 묻힌다.
  #
  # **종료 코드도 본다.** stderr 만 보면 docker 가 컨테이너를 못 찾은 경우처럼
  # ERROR 로 시작하지 않는 실패가 성공으로 새어 나간다 — 리뷰가 그것을 잡았다.
  # 그래서 두 축으로 가른다.
  #   ⑴ 넘길 넷을 지우고 남은 줄이 있으면 → 실패 (docker 오류 문구도 여기서 걸린다)
  #   ⑵ 남은 줄이 없는데 종료 코드가 0 도 아니고 MySQL 오류도 안 났으면 → 실패
  #      (조용히 죽는 경우. 원인이 안 보이므로 종료 코드를 그대로 싣는다)
  err_file="$(mktemp)"
  for f in "${FILES[@]}"; do
    path="$MIGRATIONS_DIR/$f"
    [ -f "$path" ] || {
      echo "  ✗ 마이그레이션이 없다: $path" >&2
      echo "    이름이 바뀌었으면 이 스크립트와 SchemaPresenceGuard.META_MIGRATIONS 를" >&2
      echo "    함께 고쳐야 한다." >&2
      rm -f "$err_file"; exit 1; }

    : > "$err_file"
    set +e
    mysql_force "$schema" < "$path" 2>"$err_file"
    rc=$?
    set -e

    # ⑴ 넘길 넷과 빈 줄만 지운다. 한 줄이라도 남으면 그게 진짜 실패다.
    grep -vE "^ERROR (1050|1061|1007|1826) " "$err_file" \
      | grep -vE "^[[:space:]]*$" > "${err_file}.fatal" || true
    if [ -s "${err_file}.fatal" ]; then
      echo "  ✗ $f 적용 실패 (종료 코드 $rc)" >&2
      sed 's/^/    /' "${err_file}.fatal" >&2
      rm -f "$err_file" "${err_file}.fatal"; exit 1
    fi
    rm -f "${err_file}.fatal"

    # ⑵ 아무 말 없이 죽은 경우. mysql --force 는 넘긴 오류가 있으면 0 이 아니므로
    #    그때만 정상으로 본다.
    skipped=$(grep -cE "^ERROR" "$err_file" || true)
    if [ "$rc" -ne 0 ] && [ "$skipped" -eq 0 ]; then
      echo "  ✗ $f 적용 실패 — 종료 코드 $rc, 오류 문구 없음" >&2
      echo "    컨테이너($CONTAINER)가 떠 있는지부터 본다." >&2
      rm -f "$err_file"; exit 1
    fi

    if [ "$skipped" -gt 0 ]; then
      echo "  ✓ $f (이미 있는 객체 ${skipped}건 넘김)"
    else
      echo "  ✓ $f"
    fi
  done
  rm -f "$err_file"

  # 부었다고 믿지 않는다. 가드가 보는 축 그대로 다시 잰다.
  if ! verify_schema "$schema"; then
    echo "  ✗ 부었는데도 가드 조건을 못 만족한다" >&2
    exit 1
  fi
  echo "  가드 조건 충족 — 테이블 ${#REQUIRED_TABLES[@]} · 인덱스 ${#REQUIRED_INDEXES[@]}"
done

echo "완료. 배치를 재기동하면 SchemaPresenceGuard 가 통과한다."
