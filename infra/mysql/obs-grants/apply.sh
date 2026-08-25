#!/bin/sh
# 관측 전용 계정의 권한을 **양성 목록으로 다시 세운다.**
#
# ── 왜 initdb 가 아니라 여기인가 ────────────────────────────────────────────
#
# 테이블 단위 GRANT 는 그 테이블이 **이미 있어야** 한다. initdb.d 는 Flyway 보다 먼저
# 돌아서 그 시점에는 테이블이 하나도 없다 — 테이블 단위로 적으면 그 자리에서 죽는다(실측):
#   ERROR 1146 (42S02) at line 2: Table 'app.issuances' doesn't exist
#   → 컨테이너가 exit=1 로 아예 안 뜬다.
#
# 반대로 스키마 GRANT 를 준 뒤에 테이블 REVOKE 를 얹는 것도 안 된다(실측):
#   REVOKE SELECT ON app.members FROM 'obs'@'%';
#   → ERROR 1147: There is no such grant defined ... on table 'members'
#
# 남는 형태는 하나다 — **앱이 한 번 떠서 마이그레이션이 끝난 뒤** 도는 일회성 실행.
# compose 의 obs-grants 프로파일이 그 자리다. 테스트에서는 MySqlContainerConfig 가
# 같은 파일을 컨테이너에 복사해 Flyway·배치 스키마 초기화가 끝난 뒤 실행한다.
#
# ── 순서가 중요하다 ────────────────────────────────────────────────────────
#
# **먼저 전부 걷고, 목록만 다시 준다.** 걷기 전에 주면 남아 있던 상위 권한이 목록을
# 덮어써서 아무 효과가 없다 — 그 상태로 성공 메시지가 나가면 다음 사람은 목록이 도는 줄 안다.
#
# 그리고 이 순서는 **실패했을 때 시끄럽다.** 걷은 뒤 주다가 죽으면 관측이 즉시 멈춘다 —
# 남은 권한이 하나도 없으면 커넥션 단계에서 1044 로, 일부만 주어졌으면 그 테이블 질의에서
# 1142 로 죽는다(둘 다 실측). 반대 순서로 하다 죽으면 obs 가 members 를 계속 읽는데 아무
# 증상이 없다. 조용한 실패보다 시끄러운 실패를 고른다.
#
# ── 왜 SELECT 만이 아니라 통짜로 걷는가 ────────────────────────────────────
#
# 예전에는 `REVOKE IF EXISTS SELECT ON <db>.*` 하나만 걷었다. 이 스크립트가 대체한 산출물이
# 정확히 그 형태였기 때문이다. 그런데 그것은 **자기가 준 것만 되돌리는** 스크립트라,
# 손으로 더 준 DB 는 재부여해도 안 좁아졌다(실측):
#
#   심은 것 → GRANT SELECT ON *.* / GRANT INSERT ON `app`.*
#   구 버전 실행 후 → 둘 다 그대로 살아남음
#                    obs 로 SELECT COUNT(*) FROM members  → 0 (1142 아님, 읽힌다)
#                    obs 로 INSERT INTO issuances         → 성공 (쓰기까지 된다)
#
# 지금은 계정의 권한을 **전부** 걷고 목록만 다시 준다. 어느 상태에서 시작하든 결과가
# 양성 목록으로 수렴한다. 실측으로 확인한 성질:
#   - 전역(`ON *.*`) · 스키마 · 테이블 단위 권한이 모두 걷힌다
#   - `USAGE` 는 남는다 — 계정 자체가 사라지지 않는다
#   - 권한이 `USAGE` 뿐인 신규 계정에 걸어도 에러 없이 통과한다(exit 0)
#   - 두 번 연속 걸어도 통과한다(멱등)
#   - **계정이 없으면 ERROR 1269 로 죽는다**(exit 1). 그게 낫다 — 계정을 안 만든 채
#     이 스크립트를 돌린 것이므로, 조용히 성공하면 안 된다
#
# ⚠️ **그 문장은 역할(ROLE) 할당을 걷지 못한다.** MySQL 에서 권한과 역할은 별개 구조라
#    REVOKE ALL PRIVILEGES 는 역할 할당을 건드리지 않는다(문서에 명시돼 있고 실측으로도 확인).
#    실측 — obs 에 `GRANT SELECT ON app.*` 를 가진 역할을 붙여 두면:
#      REVOKE ALL PRIVILEGES 후에도  GRANT `legacy_reader`@`%` TO `obs`@`%`  가 남고
#      obs 가 members 를 그대로 읽는다(1142 가 아니라 결과가 나온다)
#    그래서 아래에서 mysql.role_edges 를 읽어 붙어 있는 역할을 **하나씩** 걷는다. 역할을 걷으면
#    mysql.default_roles 의 짝도 같이 사라진다(실측).
#
#    ⚠️ GROUP_CONCAT 으로 한 문장에 몰지 않는다. group_concat_max_len(기본 1024바이트)에서
#       목록이 잘리면 잘린 자리에 따라 문법 오류로 죽거나 — 더 나쁘게 — 구분자에서 깔끔히
#       잘려 **일부만 걷고 성공**한다. 뒤쪽은 조용해서 안 잡힌다.
#
# ⚠️ **mandatory_roles 는 이 스크립트가 걷을 수 없다.** 서버 전역 설정으로 모든 계정에
#    암묵 부여되는 역할이라 mysql.role_edges 에 행이 없고, REVOKE 대상도 되지 못한다.
#    실측(mandatory_roles='forced_reader' + activate_all_roles_on_login=ON):
#      mysql.role_edges 의 obs 행 → 0 개  (여기서 안 보인다)
#      root 의 SHOW GRANTS FOR obs → USAGE 뿐 (여기서도 안 보인다)
#      그런데 obs 자신의 세션에서는 CURRENT_ROLE()=forced_reader 이고
#      SELECT COUNT(*) FROM members 가 **성공한다**
#    즉 양성 목록이 통째로 무의미해진다. 걷을 수 없으므로 **거부한다** — 조용히 성공해서
#    "재부여했다" 는 기록을 남기는 것이 이 상황에서 가장 나쁘다.
#
# ⚠️ **대가 — 이 스크립트는 자기가 안 준 권한도 지운다.** obs 계정에 다른 용도를 겸하게
#    해 두었다면 그것을 조용히 끊는다. 그 계정은 관측 전용이라는 것이 이 계층의 전제이고,
#    겸용이 필요하면 계정을 따로 만드는 것이 맞다. 되돌리는 길은 손으로 다시 주는 것뿐이다.
#
# ── 되돌리기 ──────────────────────────────────────────────────────────────
#
# 이 스크립트는 되돌리는 길을 만들어 두지 않는다. 스키마 단위로 되돌리려면 손으로 친다.
# 그렇게 치면 이 계층의 목적이 사라진다는 것을 알고 치라는 뜻이고, 다음 재부여가 그것을
# 다시 걷어낸다.
set -eu

: "${MYSQL_ROOT_PASSWORD:?GRANT 는 root 로만 줄 수 있다}"
: "${MYSQL_DATABASE:?어느 스키마의 테이블인지 정해야 한다}"
: "${DB_OBS_USERNAME:?권한을 받을 관측 계정 이름}"

# 목록 파일. 스크립트와 같은 디렉터리에 있다 — 한쪽만 복사·마운트하면 여기서 죽는다.
allowlist="$(dirname "$0")/allowlist.txt"
[ -f "${allowlist}" ] || { echo "양성 목록이 없다: ${allowlist}" >&2; exit 1; }

# 식별자는 화이트리스트로 통째로 닫는다 — 20-obs-account.sh 와 같은 이유다.
# 이 값들은 root 로 도는 문장에 그대로 박힌다.
require_identifier() {
    case "$2" in
        '') echo "$1 이 비어 있다" >&2; exit 1 ;;
        *[!A-Za-z0-9_]*) echo "$1 에 영숫자·밑줄 외 문자가 있다: $2" >&2; exit 1 ;;
    esac
}

require_identifier "DB_OBS_USERNAME" "${DB_OBS_USERNAME}"
require_identifier "MYSQL_DATABASE" "${MYSQL_DATABASE}"

# 주석과 빈 줄을 걷어낸 테이블 목록.
tables="$(sed -e 's/#.*//' -e 's/[[:space:]]//g' "${allowlist}" | grep -v '^$' || true)"
[ -n "${tables}" ] || { echo "양성 목록이 비어 있다. 그러면 관측이 아무것도 못 읽는다" >&2; exit 1; }

# root 로 한 줄짜리 값을 읽는다. 아래 사전 점검과 역할 열거가 쓴다.
query_as_root() {
    MYSQL_PWD="${MYSQL_ROOT_PASSWORD}" mysql -uroot -h "${MYSQL_HOST:-127.0.0.1}" -N -B -e "$1"
}

# ── 사전 점검: 걷을 수 없는 것이 있으면 시작도 하지 않는다 ──
mandatory_roles="$(query_as_root "SELECT @@GLOBAL.mandatory_roles" | tr -d '[:space:]')"
if [ -n "${mandatory_roles}" ]; then
    echo "거부: 서버에 mandatory_roles 가 설정돼 있다(${mandatory_roles})." >&2
    echo "  그 역할은 모든 계정에 암묵 부여되어 REVOKE 로 걷을 수 없다. 그대로 재부여하면" >&2
    echo "  양성 목록 밖의 권한이 '${DB_OBS_USERNAME}' 에 남는데 SHOW GRANTS 에도 안 보인다." >&2
    echo "  먼저 SET GLOBAL mandatory_roles='' 로 걷어낸 뒤 다시 실행하라." >&2
    exit 1
fi

# 붙어 있는 역할을 **한 줄에 하나씩** 읽는다. 위 ⚠️ 참조 — GROUP_CONCAT 은 잘린다.
obs_roles="$(query_as_root "SELECT CONCAT(QUOTE(FROM_USER), '@', QUOTE(FROM_HOST))
                              FROM mysql.role_edges
                             WHERE TO_USER = '${DB_OBS_USERNAME}' AND TO_HOST = '%'")"

statements="
-- 계정의 권한을 전부 걷는다(전역·스키마·테이블 모두). 위 ⚠️ 참조.
-- IF EXISTS 라 권한이 USAGE 뿐인 신규 계정에서도 통과한다 — 그 분기를 셸 조건문으로
-- 나누면 두 경로 중 하나만 실제로 도는 상태가 만들어진다.
REVOKE IF EXISTS ALL PRIVILEGES, GRANT OPTION FROM '${DB_OBS_USERNAME}'@'%';

"

# 역할이 없으면 이 루프는 한 번도 안 돈다 — 셸 조건문으로 분기를 나누지 않는다.
while IFS= read -r obs_role; do
    [ -n "${obs_role}" ] || continue
    statements="${statements}
REVOKE ${obs_role} FROM '${DB_OBS_USERNAME}'@'%';"
done <<ROLES
${obs_roles}
ROLES

for table in ${tables}; do
    require_identifier "allowlist 의 테이블 이름" "${table}"
    statements="${statements}
GRANT SELECT ON \`${MYSQL_DATABASE}\`.\`${table}\` TO '${DB_OBS_USERNAME}'@'%';"
done

# MYSQL_PWD 로 넘긴다. -p"..." 는 컨테이너 안 `ps` 에 root 비밀번호가 그대로 보인다.
# --host 를 받는 이유 — compose 에서는 별도 컨테이너가 mysql 서비스에 붙고,
# 테스트에서는 DB 컨테이너 안에서 돌아 127.0.0.1 이다.
MYSQL_PWD="${MYSQL_ROOT_PASSWORD}" mysql -uroot -h "${MYSQL_HOST:-127.0.0.1}" <<SQL
${statements}
FLUSH PRIVILEGES;
SQL

echo "관측 계정 '${DB_OBS_USERNAME}' 재부여 완료 — \`${MYSQL_DATABASE}\` 의 다음 테이블만 SELECT:"
echo "${tables}" | sed 's/^/  /'
