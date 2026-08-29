#!/bin/sh
# 관측 전용 계정을 만든다. **compose 와 테스트 컨테이너가 같은 파일을 쓴다.**
#
# 왜 SQL 이 아니라 셸인가 — 계정과 비밀번호를 파일에 박지 않기 위해서다.
# MySQL 의 initdb.d 는 .sql 을 그대로 실행할 뿐 환경변수를 치환하지 않는다.
# 셸이면 compose 의 env_file(.env)과 테스트 컨테이너의 withEnv 가 그대로 들어온다.
#
# 왜 여기(main resources)인가 — 예전에는 이 파일이 testFixtures 에만 있었다.
# 그래서 "운영 권한의 정본" 이라고 선언해 놓고 **실행되는 곳은 테스트 컨테이너뿐**이었다.
# 신규 클론이 compose 를 띄우면 첫 관측 쿼리에서 Access denied 가 났고,
# 기존 볼륨을 쓰는 사람은 재현이 안 돼 "내 로컬은 되는데" 가 됐다.
#
# ⚠️ initdb.d 는 **데이터 디렉터리가 비어 있을 때만** 돈다. 이미 볼륨이 있는 환경에서는
#    이 파일을 고쳐도 아무 일이 없다. 그때는 아래 GRANT 를 손으로 한 번 실행하거나
#    볼륨을 지우고 다시 띄운다.
#
# ⚠️ 이 파일은 **계정만 만든다. 권한은 주지 않는다.**
#    예전에는 여기서 `GRANT SELECT ON app.*` 를 줬다. 스키마 단위였던 것은 취향이 아니라
#    여기서는 그것밖에 안 되기 때문이었다 — initdb 는 Flyway 보다 먼저 돌아서 이 시점에는
#    테이블이 하나도 없고, 테이블 단위로 적으면 그 자리에서 죽는다(실측):
#      ERROR 1146 (42S02) at line 2: Table 'app.issuances' doesn't exist
#    → 컨테이너가 exit=1 로 아예 안 뜬다.
#
#    그 대가가 members 였다. 관측 경로는 그 테이블을 한 곳도 읽지 않는데 계정은 읽을 수
#    있었고, 그 테이블만 빼는 것은 MySQL 에서 불가능하다(실측):
#      REVOKE SELECT ON app.members FROM 'obs'@'%';
#      → ERROR 1147: There is no such grant defined ... on table 'members'
#
#    그래서 권한 부여를 **Flyway 이후에 도는 자리로 옮겼다**:
#      infra/mysql/obs-grants/apply.sh  (목록은 같은 디렉터리의 allowlist.txt)
#    compose 에서는 `--profile obs-grants` 일회성 서비스가, 테스트에서는
#    MySqlContainerConfig 가 같은 파일을 돌린다.
#
#    ⚠️ 그 결과 **이 파일만 돌면 obs 계정은 아무것도 못 읽는다**(USAGE 뿐이다). 그게 의도다 —
#       권한이 없는 상태는 시끄럽게 드러난다 — 관측 풀이 커넥션조차 못 연다(실측:
#       ERROR 1044 Access denied for user 'obs'@'%' to database 'app'. JDBC URL 이 스키마를
#       지정하므로 질의가 아니라 접속 단계다). 반대로 스키마 GRANT 가 남아 있는 상태는
#       아무 증상이 없다. 실행 시점과 미실행 시 증상은 README 에 있다.
set -eu

: "${MYSQL_ROOT_PASSWORD:?initdb 는 root 로 돈다. 이 값이 없으면 계정을 만들 수 없다}"
: "${MYSQL_DATABASE:?계정 이름과 함께 검증한다. 권한은 obs-grants/apply.sh 가 준다}"
: "${DB_OBS_USERNAME:?관측 계정 이름. .env 또는 컨테이너 env 로 준다}"
: "${DB_OBS_PASSWORD:?관측 계정 비밀번호. .env 또는 컨테이너 env 로 준다}"

# ── 이 값들은 root 로 도는 문장에 그대로 박힌다 ──
#
# 식별자(계정 이름 · 스키마 이름)는 **화이트리스트로 통째로 닫는다.** 이스케이프보다
# 강하고, 정상 값이 그 문자 집합을 벗어날 이유가 없다. 어긋나면 기동에서 죽인다 —
# 조용히 이상한 계정을 만드는 것보다 낫다.
require_identifier() {
    case "$2" in
        '') echo "$1 이 비어 있다" >&2; exit 1 ;;
        *[!A-Za-z0-9_]*) echo "$1 에 영숫자·밑줄 외 문자가 있다: $2" >&2; exit 1 ;;
    esac
}

# 리터럴(비밀번호)은 화이트리스트로 닫을 수 없다. 두 문자를 **둘 다** 처리해야 한다.
#
# ⚠️ 백슬래시를 빠뜨리면 안 된다. MySQL 은 NO_BACKSLASH_ESCAPES 가 꺼진 기본값에서
#    \ 를 이스케이프 문자로 읽으므로, 비밀번호가 \ 로 끝나면 닫는 따옴표가 escape 되어
#    문자열이 다음 줄까지 삼킨다. 실측: DB_OBS_PASSWORD=pw\ 로 띄우면
#      ERROR 1064 ... near 'obs'@'%'  →  컨테이너 exit=1
#    즉 자동 생성 비밀번호 하나로 DB 가 아예 안 뜬다.
#
#    (주입까지 가지는 않는다 — 아래 따옴표 배가가 공격자의 ' 를 리터럴로 만들어
#     문자열을 못 닫게 한다. 실측으로 확인했다. 그래도 가용성은 실재하는 사고다.)
escape_sql_literal() {
    printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e "s/'/''/g"
}

require_identifier "DB_OBS_USERNAME" "${DB_OBS_USERNAME}"
require_identifier "MYSQL_DATABASE" "${MYSQL_DATABASE}"

obs_user="${DB_OBS_USERNAME}"
obs_password="$(escape_sql_literal "${DB_OBS_PASSWORD}")"

# MYSQL_PWD 로 넘긴다. -p"..." 는 컨테이너 안 `ps` 에 root 비밀번호가 그대로 보인다.
MYSQL_PWD="${MYSQL_ROOT_PASSWORD}" mysql -uroot <<SQL
CREATE USER IF NOT EXISTS '${obs_user}'@'%' IDENTIFIED BY '${obs_password}';
-- CREATE ... IF NOT EXISTS 는 계정이 이미 있으면 비밀번호를 **조용히 무시한다**(exit 0).
-- README 가 안내하는 "기존 볼륨에 손으로 준다" 경로에서 비밀번호를 바꾸려 해도
-- 아무 일이 안 일어난다. 그래서 한 줄 더 둔다 — 이미 있으면 값을 맞춘다.
ALTER USER '${obs_user}'@'%' IDENTIFIED BY '${obs_password}';
-- 권한은 여기서 주지 않는다. 위 ⚠️ 참조 — infra/mysql/obs-grants/apply.sh 가 준다.
FLUSH PRIVILEGES;
SQL
