#!/bin/bash
# PreToolUse(Bash) — 모듈 범위 없는 전체 gradle 실행을 막는다.
# 이 저장소의 전체 build 는 Testcontainers 때문에 회당 4~5분이다.
#
# ⚠️ 이 파일만으로는 아무 일도 안 한다. .claude/settings.json 의 PreToolUse(Bash) 에
#    등록돼 있어야 돈다. 등록을 빼면 가드가 조용히 사라진다 — 오류가 안 난다.
#
# 판정은 <태스크 인수를 하나씩> 본다. 예전에는 ":api:test" 나 "-p" 가 하나라도 보이면
# 즉시 통과시켰는데, 그러면 "./gradlew :api:test build" 처럼 섞어 쓸 때 build 가
# 전 모듈로 돌면서도 통과했다(실측).
set -u

cmd=$(cat | /usr/bin/python3 -c 'import json,sys; print(json.load(sys.stdin).get("tool_input",{}).get("command",""))' 2>/dev/null) || exit 0

[[ "$cmd" == *gradlew* ]] || exit 0

# 전 모듈로 퍼지는 태스크. 모듈 경로가 안 붙으면 root 에서 모든 subproject 로 내려간다.
GLOBAL_TASKS=(build test check clean)

# ⚠️ **완전 일치로는 못 막는다(실측).** Gradle 은 태스크명 축약을 지원해서
#    `./gradlew che` 가 `check` 로 풀린다 — `--dry-run` 으로 확인하면 :api :core :storage
#    :infra:mq :infra:redis 가 전부 딸려 나온다. 완전 일치만 보던 시절 그 명령이
#    **가드를 그대로 통과했다.** 그래서 접두사로 본다.
#
#    접두사로 넓혀도 잃는 것이 없다 — build·test·check·clean 의 접두사 중 이 저장소에서
#    쓰는 다른 루트 태스크와 겹치는 것이 없고, 겹치는 접두사(예: `c`)는 Gradle 자신이
#    모호하다며 거절한다. `tasks`·`wrapper`·`help` 처럼 값싼 루트 태스크는 접두사가 아니라
#    그대로 통과한다.
is_global_task() {
    local token="$1" task
    [[ -n "$token" ]] || return 1
    for task in "${GLOBAL_TASKS[@]}"; do
        # 글롭 해석을 피하려고 자른 문자열끼리 비교한다 — 토큰에 * 가 들어와도 안전하다.
        [[ "${task:0:${#token}}" == "$token" ]] && return 0
    done
    return 1
}
# 값을 따로 받는 옵션 — 그 값은 태스크가 아니므로 건너뛴다.
VALUE_OPTS='^(--tests|--project-dir|-p|--include-build|-I|--init-script|-c|--settings-file|-b|--build-file)$'

# read -ra 는 셸 인용부호를 해석하지 않는다. 그대로 비교하면 "build" 가 build 와 달라
# 전체 빌드가 통과한다(실측). 토큰마다 감싼 따옴표를 벗겨서 본다.
unquote() {
    local v="$1"
    while [[ ${#v} -ge 2 ]]; do
        case "$v" in
            \"*\") v="${v#\"}"; v="${v%\"}" ;;
            \'*\') v="${v#\'}"; v="${v%\'}" ;;
            *) break ;;
        esac
    done
    printf '%s' "$v"
}

read -ra raw_args <<<"$cmd"
args=()
for a in ${raw_args+"${raw_args[@]}"}; do
    args+=("$(unquote "$a")")
done
scoped_dir=""
skip_next=0
global_task=""

for arg in "${args[@]}"; do
    if (( skip_next )); then
        # -p / --project-dir 의 값은 범위 지정으로 쓸 수 있으니 기억해 둔다.
        [[ "$scoped_dir" == "__pending__" ]] && scoped_dir="$arg"
        skip_next=0
        continue
    fi
    if [[ "$arg" =~ $VALUE_OPTS ]]; then
        [[ "$arg" == "-p" || "$arg" == "--project-dir" ]] && scoped_dir="__pending__"
        skip_next=1
        continue
    fi
    # 나머지 옵션은 태스크가 아니다.
    [[ "$arg" == -* ]] && continue
    [[ "$arg" == *gradlew* ]] && continue
    # 모듈 경로가 붙은 태스크(:api:test)는 범위가 있다.
    [[ "$arg" == *:* ]] && continue
    if is_global_task "$arg"; then
        global_task="$arg"
    fi
done

# -p 가 실제 하위 디렉터리를 가리키면 그 범위로 도는 것이므로 통과시킨다.
# "-p ." 과 "-p <저장소 루트>" 는 범위 제한이 아니다 — root 는 여섯 subproject 를 포함한다.
if [[ -n "$global_task" && -n "$scoped_dir" && "$scoped_dir" != "__pending__" ]]; then
    case "$scoped_dir" in
        .|./|"$PWD"|"${CLAUDE_PROJECT_DIR:-}") ;;   # 루트 = 제한 아님
        *) exit 0 ;;
    esac
fi

if [[ -n "$global_task" ]]; then
    cat >&2 <<MSG
차단: 모듈 범위 없는 전체 gradle 실행이다("$global_task"). 이 저장소의 전체 build 는 회당 4~5분이다.

손댄 모듈만 지정해 다시 실행할 것:
  ./gradlew :api:test --tests '*SomeTest*'
  ./gradlew :infra:mq:test
  ./gradlew :core:compileJava

모듈 태스크와 섞어도 통과하지 않는다 — ':api:test build' 의 build 는 전 모듈로 돈다.
사용자가 "전체 빌드" 를 명시적으로 요청했다면 그 사실을 말하고 사용자에게 직접 실행을 요청한다.
MSG
    exit 2
fi
exit 0
