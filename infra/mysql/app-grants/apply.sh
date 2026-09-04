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

# 계정에 **부여된 역할**도 걷는다. REVOKE ALL PRIVILEGES 는 역할을 떼지 않으므로,
# 역할이 UPDATE·DELETE 를 주고 있으면 append-only 가 통째로 무효가 된다.
# obs 쪽 apply.sh 가 같은 이유로 같은 것을 한다.
app_roles="$(query_as_root "SELECT CONCAT(QUOTE(FROM_USER), '@', QUOTE(FROM_HOST))
                              FROM mysql.role_edges
                             WHERE TO_USER = '${DB_USERNAME}' AND TO_HOST = '%'")"

# ② 마이그레이션이 끝날 때까지 기다린다.
#
#    거부가 아니라 **대기**인 이유 — compose 의 service_started 는 프로세스 시작만 기다리고
#    api 에는 healthcheck 가 없다. 그래서 이 스크립트가 Flyway 와 경쟁할 수 있는데,
#    `restart: no` 라 한 번 실패하면 재시도되지 않는다. 스스로 기다리는 편이 낫다.
#
#    **끝났다는 것을 추론하지 않고 명시적으로 확인한다** — 이 빌드가 싣고 온 마이그레이션
#    버전이 하나도 빠짐없이 flyway_schema_history 에 success=1 로 들어와 있는가.
#    Flyway 는 마이그레이션이 성공한 **뒤에** 그 행을 넣으므로, 마지막 버전의 행이 보이면
#    그 앞은 전부 끝난 것이다.
#
#    ⚠️ 한때 이 자리에서 **락이 잡혀 있는지**를 봤는데 **틀렸다.**
#       락이 없다는 것은 "끝났다" 이기도 하지만 **"아직 시작도 안 했다"** 이기도 하다.
#       기존 스키마가 있는 재배포에서는 Flyway 가 뜨기 전에 그대로 통과해 버려서,
#       새로 생길 테이블이 DML 권한을 못 받고 앱이 런타임에 1142 로 죽는다.
#       게다가 Flyway 의 락 **이름**은 내부에서 만들어 재현할 수 없어 "아무 사용자 락"으로
#       셀 수밖에 없었는데, 그러면 **무관한 세션의 GET_LOCK() 하나가 적용을 통째로 막는다.**
#       버전 대조는 둘 다 성립하지 않는다 — 이름이 특정되고, 안 끝난 상태와 안 시작한
#       상태를 구분한다.
#
#    재배포가 새 마이그레이션을 하나도 안 싣고 왔다면 "안 시작" 과 "끝남" 이 내용상
#    구분되지 않지만, 그 경우엔 **구분할 필요가 없다** — 스키마가 이미 최종형이라
#    지금 열거하는 테이블 목록이 완전하다.
migration_dir="${APP_GRANTS_MIGRATION_DIR:-/migrations}"
[ -d "${migration_dir}" ] || {
    echo "거부: 마이그레이션 디렉터리가 없다: ${migration_dir}" >&2
    echo "  이 스크립트는 이 빌드의 마이그레이션 목록과 대조해서 Flyway 종료를 판정한다." >&2
    echo "  compose 가 storage 의 db/migration 을 읽기 전용으로 마운트해야 한다." >&2
    exit 1
}

# 반복 마이그레이션(R__)은 version 이 NULL 이라 이 대조에 안 잡힌다. 지금은 0개인데,
# 나중에 생기면 조용히 새는 대신 여기서 멈춘다.
repeatable="$(find "${migration_dir}" -maxdepth 1 -name 'R__*.sql' | head -1)"
[ -z "${repeatable}" ] || {
    echo "거부: 반복 마이그레이션이 있다(${repeatable})." >&2
    echo "  version 이 NULL 이라 버전 대조로는 종료를 판정할 수 없다. 검사를 고쳐야 한다." >&2
    exit 1
}

# V<버전>__<설명>.sql 의 <버전> 이 flyway_schema_history.version 에 그대로 들어간다(실측).
expected_versions="$(find "${migration_dir}" -maxdepth 1 -name 'V*__*.sql' -exec basename {} \; \
                     | sed -e 's/^V//' -e 's/__.*//' | sort -u)"
[ -n "${expected_versions}" ] || {
    echo "거부: ${migration_dir} 에 마이그레이션이 하나도 없다. 마운트가 잘못됐다." >&2
    exit 1
}
expected_count="$(printf '%s\n' "${expected_versions}" | wc -l | tr -d '[:space:]')"

# SQL 리터럴로 만든다. 버전 문자열은 Flyway 문법상 숫자와 점뿐이므로 그것만 통과시킨다.
version_list=""
while IFS= read -r version; do
    case "${version}" in
        '' ) continue ;;
        *[!0-9.]* )
            echo "거부: 마이그레이션 버전에 숫자·점 외 문자가 있다: ${version}" >&2
            exit 1 ;;
    esac
    version_list="${version_list}${version_list:+,}'${version}'"
done <<VERSIONS
${expected_versions}
VERSIONS

wait_seconds="${APP_GRANTS_WAIT_SECONDS:-120}"
waited=0
while :; do
    history_exists="$(query_as_root "SELECT COUNT(*) FROM information_schema.TABLES
                                      WHERE TABLE_SCHEMA = '${MYSQL_DATABASE}'
                                        AND TABLE_NAME = 'flyway_schema_history'")"
    if [ "${history_exists}" = "1" ]; then
        failed="$(query_as_root "SELECT COUNT(*) FROM \`${MYSQL_DATABASE}\`.flyway_schema_history
                                  WHERE success = 0")"
        if [ "${failed}" != "0" ]; then
            echo "거부: 실패한 마이그레이션이 ${failed}건 있다. 스키마가 확정되지 않았다." >&2
            exit 1
        fi
        applied="$(query_as_root "SELECT COUNT(DISTINCT version)
                                    FROM \`${MYSQL_DATABASE}\`.flyway_schema_history
                                   WHERE success = 1 AND version IN (${version_list})")"
        if [ "${applied}" = "${expected_count}" ]; then
            break
        fi
    else
        applied=0
    fi

    if [ "${waited}" -ge "${wait_seconds}" ]; then
        echo "거부: ${wait_seconds}초 동안 마이그레이션이 끝나지 않았다" >&2
        echo "  (이 빌드의 ${expected_count}건 중 ${applied}건 적용됨)." >&2
        echo "  이 스크립트는 Flyway 뒤에 돌아야 한다 — 앞서 돌면 테이블 목록이 불완전해" >&2
        echo "  빠진 테이블에 DML 권한이 안 가고 앱이 런타임에 1142 로 죽는다." >&2
        exit 1
    fi
    echo "마이그레이션을 기다리는 중… ${applied}/${expected_count} (${waited}/${wait_seconds}초)"
    sleep 2
    waited=$((waited + 2))
done

tables="$(query_as_root "SELECT TABLE_NAME FROM information_schema.TABLES
                          WHERE TABLE_SCHEMA = '${MYSQL_DATABASE}' AND TABLE_TYPE = 'BASE TABLE'")"
[ -n "${tables}" ] || {
    echo "스키마에 테이블이 없다. Flyway 보다 먼저 돌았을 수 있다 — 마이그레이션 뒤에 돌려라" >&2
    exit 1
}

statements="
REVOKE IF EXISTS ALL PRIVILEGES, GRANT OPTION FROM '${DB_USERNAME}'@'%';
-- Flyway 가 이 계정으로 돈다. DDL 은 스키마 단위로 준다 — 새 테이블을 만들어야 하므로
-- 테이블 단위로는 줄 수 없다.
--
-- ⚠️ **DROP 을 주지 않는다.** TRUNCATE TABLE 이 DROP 권한으로 돌아서, 주면 DELETE 를
--    막아도 이력을 통째로 날릴 수 있다 — append-only 가 성립하지 않는다.
--    지금 마이그레이션 중 DROP TABLE·TRUNCATE 를 쓰는 것은 하나도 없다(실측).
--    쓰게 되면 그 마이그레이션이 여기서 막히고, 그때 **DDL 계정을 분리**해야 한다.
GRANT CREATE, ALTER, INDEX, REFERENCES ON \`${MYSQL_DATABASE}\`.* TO '${DB_USERNAME}'@'%';
"

# **줄 단위로 읽는다.** `for x in ${app_roles}` 는 공백에서 쪼개므로
# `'legacy reader'@'%'` 같은 유효한 역할명이 두 토큰이 되어 REVOKE 가 깨진다.
# 그러면 이미 권한을 걷힌 앱 계정이 DML 없이 남는다 — obs 쪽 apply.sh 가 같은 이유로
# `while IFS= read -r` 를 쓴다.
while IFS= read -r app_role; do
    [ -n "${app_role}" ] || continue
    statements="${statements}
REVOKE ${app_role} FROM '${DB_USERNAME}'@'%';"
done <<ROLES
${app_roles}
ROLES

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
