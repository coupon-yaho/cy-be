#!/usr/bin/env bash
# api·batch 이미지를 커밋 SHA 태그로 빌드한다. 태그가 곧 회차 메타의 일부다.
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"

# ⚠️ 빌드는 compose 가 도는 호스트(A)에서 해야 한다. B 에서 빌드하면 그 이미지는 B 의
#    도커에만 있고 A 는 같은 태그를 레지스트리에서 찾다가 죽는다.
SHA=$(cd "$REPO_ROOT" && git rev-parse --short HEAD)
if [[ -n "${PERF_A_SSH:-}" ]]; then
  A_SHA=$(a_exec "git rev-parse --short HEAD" | tr -d '\r\n')
  [[ "$A_SHA" == "$SHA" ]] \
    || die "A 의 저장소가 다른 커밋이다 — A=$A_SHA · B=$SHA. 같은 커밋에서 재야 비교가 성립한다."
fi
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
  a_exec "docker build --build-arg APP_MODULE=$m -t $REPO:$m-$SHA ."
done
echo "$SHA"
