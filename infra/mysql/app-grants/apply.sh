#!/bin/sh
# 앱 계정의 스키마 단위 권한을 걷고 테이블 단위로 다시 준다.
#
# ⚠️ **좁은 GRANT 를 얹는 것만으로는 아무것도 못 막는다.** MySQL 권한은 가산적이라
#    `GRANT SELECT, INSERT ON app.issuance_histories` 를 얹어도 도커 이미지가 만든
#    `GRANT ALL ON app.*` 가 그대로 살아 있다. 테이블 단위 REVOKE 로도 못 걷는다
#    (AGENTS.md 의 실측: ERROR 1147). **먼저 걷고 다시 주는 순서가 전부다.**
#
# ⚠️ **Flyway 뒤에 돌려야 한다.** 마이그레이션이 이 계정으로 돌고(storage.yml 의 @Primary
#    풀), 새 테이블은 이 스크립트가 다시 돌기 전까지 DML 권한이 없다. 그래서 목록을
#    파일에 박지 않고 information_schema 로 **실행 시점에 열거한다** — 재실행만 하면
#    새 테이블이 자동으로 들어온다.
set -eu

: "${MYSQL_ROOT_PASSWORD:?GRANT 는 root 로만 줄 수 있다}"
: "${MYSQL_DATABASE:?어느 스키마의 테이블인지 정해야 한다}"
: "${DB_USERNAME:?권한을 받을 앱 계정 이름}"

append_only_file="$(dirname "$0")/append-only.txt"
[ -f "${append_only_file}" ] || {
    echo "append-only 목록이 없다: ${append_only_file}" >&2
    exit 1
}

require_identifier() {
    case "$2" in
        '') echo "$1 이 비어 있다" >&2; exit 1 ;;
        *[!A-Za-z0-9_]*) echo "$1 에 영숫자·밑줄 외 문자가 있다: $2" >&2; exit 1 ;;
    esac
}
require_identifier "DB_USERNAME" "${DB_USERNAME}"
require_identifier "MYSQL_DATABASE" "${MYSQL_DATABASE}"

append_only="$(sed -e 's/#.*//' -e 's/[[:space:]]//g' "${append_only_file}" | grep -v '^$' || true)"
[ -n "${append_only}" ] || {
    echo "append-only 목록이 비어 있다. 그러면 이 스크립트를 돌릴 이유가 없다" >&2
    exit 1
}
for table in ${append_only}; do
    require_identifier "append-only 항목" "${table}"
done

query_as_root() {
    MYSQL_PWD="${MYSQL_ROOT_PASSWORD}" mysql -uroot -h "${MYSQL_HOST:-127.0.0.1}" -N -B -e "$1"
}

# obs 쪽과 같은 이유로 막는다 — 암묵 부여된 역할은 REVOKE 로 못 걷고 SHOW GRANTS 에도 안 보인다.
mandatory_roles="$(query_as_root "SELECT @@GLOBAL.mandatory_roles" | tr -d '[:space:]')"
if [ -n "${mandatory_roles}" ]; then
    echo "거부: 서버에 mandatory_roles 가 설정돼 있다(${mandatory_roles})." >&2
    echo "  그 역할은 모든 계정에 암묵 부여되어 REVOKE 로 걷을 수 없다." >&2
    exit 1
fi

tables="$(query_as_root "SELECT TABLE_NAME FROM information_schema.TABLES
                          WHERE TABLE_SCHEMA = '${MYSQL_DATABASE}' AND TABLE_TYPE = 'BASE TABLE'")"
[ -n "${tables}" ] || {
    echo "스키마에 테이블이 없다. Flyway 보다 먼저 돌았을 수 있다 — 마이그레이션 뒤에 돌려라" >&2
    exit 1
}

statements="
REVOKE IF EXISTS ALL PRIVILEGES, GRANT OPTION FROM '${DB_USERNAME}'@'%';
-- Flyway 가 이 계정으로 돈다. DDL 은 스키마 단위로 준다 — 새 테이블을 만들어야 하므로
-- 테이블 단위로는 줄 수 없다. append-only 는 DML 축의 제약이고 DDL 은 다른 축이다.
GRANT CREATE, ALTER, DROP, INDEX, REFERENCES ON \`${MYSQL_DATABASE}\`.* TO '${DB_USERNAME}'@'%';
"

for table in ${tables}; do
    case "${table}" in
        *[!A-Za-z0-9_]*)
            echo "거부: 스키마에 영숫자·밑줄 외 문자를 가진 테이블이 있다: ${table}" >&2
            exit 1 ;;
    esac
    privileges="SELECT, INSERT, UPDATE, DELETE"
    for locked in ${append_only}; do
        if [ "${table}" = "${locked}" ]; then
            privileges="SELECT, INSERT"
            break
        fi
    done
    statements="${statements}
GRANT ${privileges} ON \`${MYSQL_DATABASE}\`.\`${table}\` TO '${DB_USERNAME}'@'%';"
done

statements="${statements}
FLUSH PRIVILEGES;"

printf '%s\n' "${statements}" | MYSQL_PWD="${MYSQL_ROOT_PASSWORD}" \
    mysql -uroot -h "${MYSQL_HOST:-127.0.0.1}"

echo "앱 계정 권한을 테이블 단위로 재부여했다. append-only: ${append_only}"
