#!/usr/bin/env bash
# `ix_notification_outbox_kind` 의 컬럼 순서를 고른 근거를 다시 잽니다.
#
# 마이그레이션 V2026091001 의 표(5 질의 × 3 후보)를 재현합니다. 그 표는 **한 기기의
# 한 실행**에서 본 값이라, 다른 기기에서는 자릿수만 같고 수는 다릅니다.
#
#   bash docs/measurements/outbox-kind-index.sh
#
# 필요한 것: 도커(Testcontainers 가 MySQL 을 띄웁니다). 몇 분 걸립니다.
#
# 무엇을 재나 — `EXPLAIN` 의 type/rows 가 아니라 **`Handler_read_*`**(스토리지 엔진
# 읽기 호출)입니다. 이 저장소는 같은 자리에서 접근 방식 **이름**으로 비용을 말했다가
# 반려당한 적이 있습니다(`BacklogPlanContractTest` javadoc).
#
# ⚠️ 연결을 고정해야 합니다. `Handler_read_*` 는 **세션 상태**라 풀에서 매번 다른
#    연결을 받으면 앞뒤 측정이 서로 다른 세션에서 나옵니다.
set -euo pipefail
cd "$(dirname "$0")/../.."

echo "== 종류별 선점이 상대 종류의 적체에 안 붙는지 =="
./gradlew :storage:test --tests '*OutboxKindPlanContractTest*' --rerun --info \
  2>&1 | grep -E '읽은 호출|상대 적체|BUILD'

echo
echo "== 몫이 양방향으로 지켜지는지 =="
./gradlew :storage:test --tests '*NotificationOutboxQuotaTest*' --rerun \
  2>&1 | grep -E 'FAILED|BUILD'

echo
echo "후보 인덱스를 바꿔 가며 재려면 V2026091001 의 CREATE INDEX 컬럼 순서를 고치고"
echo "위 두 명령을 다시 돌립니다. (status) 를 선두로 두면 plan 계약이 깨져야 정상입니다."
