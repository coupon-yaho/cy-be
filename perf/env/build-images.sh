#!/usr/bin/env bash
# api·batch 이미지를 커밋 SHA 태그로 빌드한다. 태그가 곧 회차 메타의 일부다.
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

SHA=$(cd "$REPO_ROOT" && git rev-parse --short HEAD)
DIRTY=$(cd "$REPO_ROOT" && git status --porcelain | head -1)
if [[ -n "$DIRTY" ]]; then
  # 더러운 워크트리로 빌드하면 태그의 SHA 가 이미지 내용을 안 가리킨다. 회차 간
  # 비교가 그 순간 불가능해진다.
  log "⚠️ 워크트리가 더럽다. 태그 SHA 가 이미지 내용을 가리키지 않는다 — 결과에 그대로 기록된다."
  SHA="$SHA-dirty"
fi
REPO="${PERF_IMAGE_REPO:-coupon-yaho-perf}"

for m in api batch; do
  log "빌드 $REPO:$m-$SHA"
  ( cd "$REPO_ROOT" && docker build --build-arg "APP_MODULE=$m" -t "$REPO:$m-$SHA" . )
done
echo "$SHA"
