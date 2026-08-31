#!/usr/bin/env bash
# ① 환경 기동. A 에서 돌린다.
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

[[ -f "$REPO_ROOT/.env" ]] || die ".env 가 없다. cp .env.example .env 후 비밀값을 채울 것."
[[ -f "$REPO_ROOT/application.yml" ]] || die "application.yml 이 없다. cp application.yml.example application.yml.
  ⚠️ 이걸 빼먹고 up 하면 Docker 가 application.yml 이라는 디렉터리를 만들어 마운트한다(실측).
     설정이 통째로 비는데 에러에는 그 원인이 안 나온다."

if [[ -z "${PERF_SKIP_BUILD:-}" ]]; then
  SHA=$("$PERF_DIR/env/build-images.sh" | tail -1)
else
  SHA="${PERF_IMAGE_SHA:?PERF_SKIP_BUILD 를 쓰면 PERF_IMAGE_SHA 를 직접 준다}"
fi
export COUPON_IMAGE="${PERF_IMAGE_REPO:-coupon-yaho-perf}"
export API_IMAGE_TAG="api-$SHA" BATCH_IMAGE_TAG="batch-$SHA"
log "이미지 $COUPON_IMAGE:$API_IMAGE_TAG · $COUPON_IMAGE:$BATCH_IMAGE_TAG"

# 이미지 pull 정책은 perf/env/compose.perf.yml 이 missing 으로 덮는다.
export COMPOSE_PARALLEL_LIMIT=4

log "mysql · redis"
dc "up -d mysql redis"
dc "up -d --wait mysql redis"

log "런타임 설정 시드 (config:runtime)"
dc "--profile runtime-config-seed run --rm runtime-config-seed" || die "runtime-config-seed 실패"

log "api x ${PERF_API_REPLICAS:-4} · batch · nginx · prometheus"
dc "up -d api batch nginx prometheus"

log "기동 대기 — api·batch 에는 healthcheck 가 없어 actuator 로 직접 기다린다"
wait_http "http://api:9090/actuator/health"   "${PERF_BOOT_TIMEOUT:-300}" \
  || die "api 가 제한 시간 안에 안 떴다. docker compose -p $COMPOSE_PROJECT logs api"
wait_http "http://batch:9092/actuator/health" "${PERF_BOOT_TIMEOUT:-300}" \
  || die "batch 가 제한 시간 안에 안 떴다. 워밍업이 batch 에 있으므로 이게 없으면 v2 회차를 못 연다"

# 관측 계정 GRANT 는 Flyway 뒤에만 된다 — 테이블 단위 GRANT 라 테이블이 먼저 있어야 한다.
log "관측 계정 GRANT"
dc "--profile obs-grants run --rm obs-grants" || log "obs-grants 실패(무시하고 계속. 관측 쿼리가 막힐 수 있다)"

log "기동 완료. 다음: perf/env/preflight.sh"
