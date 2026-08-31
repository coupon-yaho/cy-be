#!/usr/bin/env bash
# 회차 스택을 내린다. -v 를 주면 볼륨까지 지운다(다음 회차를 빈 DB 로 시작할 때).
source "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
dc "down ${1:-}"
