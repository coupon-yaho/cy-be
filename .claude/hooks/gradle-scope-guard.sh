#!/bin/bash
# PreToolUse(Bash) — 모듈 범위 없는 전체 gradle 실행을 막는다.
# 이 저장소의 전체 build 는 Testcontainers 때문에 회당 4~5분이다.
cmd=$(cat | /usr/bin/python3 -c 'import json,sys; print(json.load(sys.stdin).get("tool_input",{}).get("command",""))' 2>/dev/null) || exit 0

# gradlew 호출이 아니면 통과
[[ "$cmd" == *gradlew* ]] || exit 0

# 태스크에 모듈 경로(:api:test 등)가 붙어 있으면 통과
grep -qE '(^|[[:space:]])(-p[[:space:]]|--project-dir)' <<<"$cmd" && exit 0
grep -qE '(^|[[:space:]]):[a-zA-Z0-9_-]+(:[a-zA-Z0-9_-]+)*:[a-zA-Z0-9]' <<<"$cmd" && exit 0

# 전 모듈로 퍼지는 태스크만 막는다
if grep -qE '(^|[[:space:]])(build|test|check|clean)([[:space:]]|$)' <<<"$cmd"; then
  cat >&2 <<'MSG'
차단: 모듈 범위 없는 전체 gradle 실행이다. 이 저장소의 전체 build 는 회당 4~5분이다.

손댄 모듈만 지정해 다시 실행할 것:
  ./gradlew :api:test --tests '*SomeTest*'
  ./gradlew :infra:mq:test
  ./gradlew :core:compileJava

사용자가 "전체 빌드" 를 명시적으로 요청했다면 그 사실을 말하고 사용자에게 직접 실행을 요청한다.
MSG
  exit 2
fi
exit 0
