#!/usr/bin/env bash
# 검증용 셋에 Spring Batch 메타 스키마를 붓는다.
#
# 왜 필요한가
#   cy-seed 의 ddl/ 은 BATCH_* 를 **일부러** 안 만든다 — 잡 실행 이력은 도메인이 아니고
#   그 DDL 의 주인은 cy-be 의 V11 이다(seed-ddl/README.md 가 그 경계를 적는다).
#   그래서 재시드는 스키마를 새로 만들면서 이 테이블들을 매번 지운다.
#
#   그 자체는 설계대로다. 문제는 **다시 붓는 것을 사람이 기억해야 한다**는 점이고,
#   실제로 2026-08-30 에 두 번 잊었다. 두 번 다 증상이 같았다 —
#   Table 'coupon_clean.BATCH_JOB_EXECUTION' doesn't exist 로 배치가 안 뜨고,
#   관제 화면에서 그 데이터셋 카드가 통째로 사라진다.
#
#   SchemaPresenceGuard 는 이미 처방을 정확히 알려 준다(파일 이름까지). 부족한 것은
#   진단이 아니라 **절차가 한 번에 끝나지 않는다**는 것이라, 그 절차를 여기 묶는다.
#
# 왜 cy-seed 가 아니라 여기인가
#   DDL 을 cy-seed 로 복사하면 V11 이 두 벌이 되고, 그것은 seed-ddl 사본 대조 장치가
#   막으려는 바로 그 상태다. 이 스크립트는 **이 저장소의 마이그레이션 파일을 그대로 읽는다** —
#   사본을 안 만든다.
#
# 인덱스 둘을 함께 붓는 이유
#   테이블만 부으면 가드의 셋째 축(인덱스)이 그 자리에서 다시 거절한다(CY-686).
#
# 쓰는 법
#   scripts/pour-batch-meta.sh coupon_clean
#   scripts/pour-batch-meta.sh coupon_clean coupon_corrupt coupon_bench
#
#   컨테이너 이름과 root 비밀번호는 환경에 맞게 넘긴다.
#   MYSQL_CONTAINER=cy-mysql-1 scripts/pour-batch-meta.sh coupon_clean
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

if [ $# -eq 0 ]; then
  echo "사용법: $0 <스키마> [스키마...]" >&2
  echo "예:    $0 coupon_clean coupon_corrupt" >&2
  exit 2
fi

# root 비밀번호를 호스트 셸 변수로 꺼내지 않는다 — 컨테이너 안에서 읽어 그대로 쓴다.
root_pw() {
  docker exec "$CONTAINER" printenv MYSQL_ROOT_PASSWORD
}

for schema in "$@"; do
  echo "▶ $schema"
  for f in "${FILES[@]}"; do
    path="$MIGRATIONS_DIR/$f"
    if [ ! -f "$path" ]; then
      echo "  ✗ 마이그레이션이 없다: $path" >&2
      echo "    파일 이름이 바뀌었으면 이 스크립트와 SchemaPresenceGuard.META_MIGRATIONS 를" >&2
      echo "    함께 고쳐야 한다." >&2
      exit 1
    fi
    # --force 로 "이미 있음" 은 넘긴다. 이 스크립트는 여러 번 돌려도 안전해야 한다 —
    # 재시드 뒤에 습관적으로 부르는 것이 목적이라 멱등이 아니면 쓰기가 꺼려진다.
    if ! docker exec -i -e MYSQL_PWD="$(root_pw)" "$CONTAINER" \
        mysql -uroot --force --default-character-set=utf8mb4 "$schema" < "$path" 2>/tmp/pour-err.$$; then
      echo "  ✗ $f 적용 실패" >&2
      head -3 /tmp/pour-err.$$ >&2
      rm -f /tmp/pour-err.$$
      exit 1
    fi
    rm -f /tmp/pour-err.$$
    echo "  ✓ $f"
  done

  # 부었다고 믿지 않고 센다. 가드가 보는 것과 같은 축이다.
  n=$(docker exec -e MYSQL_PWD="$(root_pw)" "$CONTAINER" mysql -uroot -N -e \
    "SELECT COUNT(*) FROM information_schema.tables
      WHERE table_schema='$schema' AND table_name LIKE 'BATCH\\_%';" 2>/dev/null | tr -d ' ')
  if [ "${n:-0}" -lt 9 ]; then
    echo "  ✗ BATCH_* 가 ${n:-0}개다. 아홉 개여야 한다." >&2
    exit 1
  fi
  echo "  BATCH_* ${n}개 확인"
done

echo "완료. 배치를 재기동하면 SchemaPresenceGuard 가 통과한다."
