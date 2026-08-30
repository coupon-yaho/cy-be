package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import org.testcontainers.mysql.MySQLContainer;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>관측 계정이 읽어야 할 것만 읽는지</b>를 양방향으로 고정한다.
 *
 * <p>[OBS-36] 예전에는 이 계정이 {@code GRANT SELECT ON app.*} 였다. 그래서 관측 경로가 한 곳도
 * 읽지 않는 {@code members} 까지 읽혔다 — 개인정보 컬럼은 암호화돼 있어 평문은 안 나오지만,
 * HMAC 블라인드 인덱스({@code email_hash}·{@code phone_hash})로 동일인 대조가 되고
 * {@code membership_grade}·{@code created_at} 은 평문이다.
 *
 * <p>지금은 {@code infra/mysql/obs-grants/allowlist.txt} 의 테이블만 준다. 그 목록을
 * {@code apply.sh} 가 GRANT 로 옮기고, compose 의 {@code obs-grants} 프로파일과
 * {@code MySqlContainerConfig} 가 <b>같은 파일</b>을 돌린다.
 *
 * <p><b>왜 양방향인가.</b> 한쪽만 보면 전부 막아 놓고도 통과한다 — 목록을 통째로 비우면
 * "members 를 못 읽는다" 는 여전히 참이다. 그래서 여섯 가지를 함께 단언한다:
 *
 * <ol>
 *   <li>{@code members} 를 읽으면 MySQL 1142 다 (막혔다)</li>
 *   <li>목록의 테이블은 <b>전부</b> 읽힌다 (안 막혔다)</li>
 *   <li>관측 풀로 나가는 질의문의 테이블이 <b>전부 목록에 있다</b> (빠뜨린 것이 없다)</li>
 *   <li>목록의 테이블을 <b>전부 누군가 읽는다</b> (선제적으로 열어 둔 것이 없다)</li>
 *   <li>배포 안내와 테스트가 스키마 단위 GRANT 를 <b>되살리지 않는다</b> (절차 밖의 구멍)</li>
 *   <li>레거시 과다 권한이 남아 있어도 재부여가 목록으로 <b>수렴한다</b> (반대 구성)</li>
 * </ol>
 *
 * <p>표시 없는 두 개가 더 있다 — 관측 계정이 {@code read-only} 플래그와 무관하게 쓰지 못하는 것,
 * 그리고 ③④⑤ 가 걷는 모듈 목록이 {@code settings.gradle} 과 같은 것.
 *
 * <p>③④ 가 목록 파일과 자바 소스라는 두 파일에 걸친 계약을 잇는다. 각각을 따로 보는 테스트로는
 * 못 잡는다 — 목록에서 한 줄을 빼도 ①② 는 통과하고, 새 질의를 추가해도 ①② 는 통과한다.
 *
 * <p><b>이것이 무엇을 못 보는지.</b> ③④ 는 {@code @Qualifier("obs")} 문자열이 있는
 * {@code src/main/java} 파일의 {@code FROM}·{@code JOIN} 만 읽는다. 뷰를 경유하거나
 * 질의가 {@code .sql} 로 나가거나 한정자 없이 주입받으면 안 보인다 —
 * {@code ObservationQueryScopeTest} 가 같은 그물코를 갖고 있고 거기 적어 뒀다.
 * <b>다만 그때도 ①② 는 살아 있다</b>: DB 가 실제로 거부하므로, 못 본 질의는 운영에서
 * 조용히 통과하는 것이 아니라 1142 로 죽는다. 그것이 소스 스캔과 계정 권한의 차이다.
 */
@SpringBootTest(properties = {
        "spring.flyway.enabled=true",
        // ⚠️ **모듈 application.yml 의 observation.datasource.enabled:true 가 여기선 안 온다.**
        //    batch/src/test/resources/application.yml 이 그 파일을 통째로 가리기 때문이다
        //    (테스트는 커밋 안 되는 application.yml 을 못 읽으므로 그 구조가 맞다).
        //    그래서 이 검사가 필요로 하는 스위치만 여기서 켠다 — 전역으로 켜면
        //    MySqlContainerConfig 를 안 import 하는 테스트가 관측 계정을 못 찾아 죽는다.
        "observation.datasource.enabled=true"
})
@Import(MySqlContainerConfig.class)
class ObservationAccountPrivilegeTest {

    /** 관측 풀을 무는 표지. {@code ObservationQueryScopeTest} 와 같은 표지다. */
    private static final String OBSERVATION_QUALIFIER = "@Qualifier(\"obs\")";

    /**
     * 걷는 모듈. {@code settings.gradle} 과 같아야 한다 — 아래 계약 테스트가 그 어긋남을 잡는다.
     */
    private static final List<String> MODULES =
            List.of("api", "batch", "core", "storage", "infra/mq", "infra/redis");

    /** 자바 텍스트 블록. 이 저장소의 긴 질의문은 전부 이 형태다. */
    private static final Pattern TEXT_BLOCK = Pattern.compile("\"\"\"(.*?)\"\"\"", Pattern.DOTALL);

    /**
     * 리터럴 사이의 이음매. {@code " + <식> + "} 와 {@code " + "} 를 지워 양쪽을 붙인다.
     * 식 자리에 따옴표·세미콜론이 오면 리터럴 경계를 잘못 읽으므로 그때는 안 합친다.
     */
    private static final Pattern LITERAL_JOIN =
            Pattern.compile("\"\\s*[+](?:[^;\"]*[+])?\\s*\"");

    /** 한 줄짜리 문자열 리터럴. 짧은 질의가 여기 들어 있다. */
    private static final Pattern STRING_LITERAL = Pattern.compile("\"([^\"\\\\\\n]|\\\\.)*\"");

    /**
     * 질의문에서 테이블 이름을 뽑는다. 별칭({@code FROM coupons c})과 서브쿼리({@code FROM (})는
     * 자연히 걸러진다 — 앞의 것은 첫 식별자만 잡고, 뒤의 것은 {@code (} 로 시작해 안 잡힌다.
     *
     * <p>{@code UPDATE}·{@code INSERT} 는 일부러 안 본다. 같은 파일에 운영 풀 쓰기 경로가 함께
     * 있기 때문이다({@code JdbcBenchmarkRunRepository} 는 조회만 관측 풀이다). 관측 계정은
     * 어차피 쓰지 못하므로 양성 목록의 관심사가 아니다.
     */
    private static final Pattern TABLE_REF =
            Pattern.compile("\\b(?:FROM|JOIN)\\s+([A-Za-z_][A-Za-z0-9_]*)", Pattern.CASE_INSENSITIVE);

    /** WITH 절이 정의한 CTE 이름. FROM·JOIN 대상에서 실제 테이블과 구분합니다. */
    private static final Pattern CTE_NAME = Pattern.compile(
            "(?:\\bWITH\\s+|,\\s*)([A-Za-z_][A-Za-z0-9_]*)\\s+AS\\s*\\(",
            Pattern.CASE_INSENSITIVE);

    /**
     * 스키마 단위 GRANT. 테이블 자리에 {@code *} 가 오는 형태 전부다.
     *
     * <p><b>권한 목록을 열거하지 않는다.</b> 예전에는 {@code GRANT SELECT ON} 만 찾아서
     * 다음 두 형태가 그대로 빠져나갔다(실측 — 둘 다 넣었을 때 ⑤ 가 통과했다):
     * <pre>
     *   GRANT INSERT, SELECT ON app.* TO 'obs'@'%'
     *   GRANT ALL ON app.* TO 'obs'@'%'
     * </pre>
     * 막으려는 것은 "무슨 권한이냐" 가 아니라 <b>"대상이 스키마 전체냐"</b> 이므로,
     * {@code GRANT} 와 {@code ON &lt;무언가&gt;.*} 사이는 안 본다.
     * {@code ON *.*}(전역)도 같은 그물에 걸린다.
     *
     * <p>테이블 단위({@code ON `app`.`issuances`})는 안 걸린다 — 대상이 {@code .*} 가 아니다.
     */
    /**
     * 이 검사에서 <b>유일하게 면제되는 파일</b> — 자기 자신이다.
     *
     * <p>⑥ 이 레거시 과다 권한을 <b>일부러 심는다.</b> 재부여가 그것을 걷어내는지 보는 것이
     * 그 테스트의 전부라, 심는 문장이 없으면 검증할 대상이 없다. 문자열을 쪼개서 이 검사를
     * 피해 가는 쪽이 훨씬 나쁘다 — 그러면 다음 사람이 같은 수법으로 진짜 회귀를 숨긴다.
     *
     * <p>면제가 안전한 이유는 ⑥ 이 <b>{@code finally} 에서 반드시 재부여를 돌리고</b>, 모든
     * 단언을 그 뒤에 하기 때문이다. 이 파일 안에서 심은 상태는 같은 메서드 안에서 되감긴다.
     */
    private static final String THIS_TEST = "ObservationAccountPrivilegeTest.java";

    private static final Pattern SCHEMA_WIDE_GRANT =
            Pattern.compile("GRANT\\b[^;]{0,200}?\\bON\\s+[^\\s;]*\\.\\s*\\*", Pattern.CASE_INSENSITIVE);

    /** 관측 계정 이름. 픽스처가 이 이름으로 계정을 만들고 재부여한다. */
    private static final String OBSERVATION_USERNAME = "obs";

    /**
     * <b>관측 계정에 주는 것만 본다.</b> 이 검사의 대상은 <i>"관측 계정이 스키마 단위 권한을
     * 되찾는가"</i> 이지 <i>"저장소에 스키마 단위 GRANT 가 있는가"</i> 가 아니다.
     *
     * <p>구분이 필요해진 계기 — {@code SchemaParityTestBase} 가 CLEAN·CORRUPT 대조용
     * <b>일회용 스키마</b>를 만들고 <b>컨테이너 자기 계정</b>({@code container.getUsername()},
     * 기본 {@code test})에 그 스키마 권한을 준다. 그 계정은 원래 전권이고 스키마도 그 테스트가
     * 만든 것이라, 관측 권한과 아무 상관이 없는데 위 정규식에 걸렸다(CY-744 합류에서 났다).
     *
     * <p>그래서 <b>수여 대상</b>을 함께 본다. 대상이 안 보이거나 관측 계정이면 잡고,
     * 다른 계정이 명시돼 있으면 넘긴다 — 관측 계정을 놓치는 쪽으로는 안 느슨해진다.
     */
    private static final Pattern GRANTS_TO_OTHER_ACCOUNT = Pattern.compile(
            "\\bTO\\s+'(?!" + OBSERVATION_USERNAME + "')[^']*'", Pattern.CASE_INSENSITIVE);

    /** ⑥ 이 심는 레거시 역할. 재부여가 이것까지 걷는지 본다. */
    private static final String LEGACY_ROLE = "obs_legacy_reader";

    /** ⑦ 이 서버 전역으로 강제하는 역할. 재부여가 걷지 못하므로 거부해야 한다. */
    private static final String FORCED_ROLE = "obs_forced_reader";

    @Autowired
    MySQLContainer mySqlContainer;

    @Autowired
    @Qualifier("obs")
    JdbcTemplate observationJdbcTemplate;

    @Autowired
    @Qualifier("obs")
    DataSource observationDataSource;

    @Test
    @DisplayName("① 관측 계정은 members 를 읽지 못한다 — MySQL 1142")
    void cannotReadMembers() {
        assertThatThrownBy(() -> observationJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM members", Integer.class))
                .as("스키마 GRANT 가 남아 있으면 이 질의가 조용히 성공한다. "
                        + "관측 경로는 members 를 한 곳도 읽지 않는다")
                .rootCause()
                .isInstanceOfSatisfying(SQLException.class, cause -> assertThat(cause.getErrorCode())
                        .as("테이블이 없어서(1146) 실패한 것이라면 이 계약은 검증되지 않았다. "
                                + "1142 = SELECT command denied")
                        .isEqualTo(1142));
    }

    @Test
    @DisplayName("② 양성 목록의 테이블은 전부 읽힌다")
    void canReadEveryAllowlistedTable() {
        List<String> allowlist = allowlist();

        assertThat(allowlist)
                .as("목록이 비면 ① 만 남는데, 그것은 전부 막아 놓고도 통과한다")
                .isNotEmpty();

        for (String table : allowlist) {
            assertThat(observationJdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM `" + table + "`", Integer.class))
                    .as("%s 를 못 읽는다. apply.sh 가 안 돌았거나 목록에 오타가 있다", table)
                    .isNotNull();
        }
    }

    @Test
    @DisplayName("③ 관측 풀로 읽는 테이블이 전부 양성 목록에 있다")
    void everyObservedTableIsAllowlisted() {
        Set<String> observed = tablesReadThroughObservationPool();
        List<String> allowlist = allowlist();

        assertThat(observed)
                .as("관측 질의를 하나도 못 찾았다. 표지(%s)가 바뀌었거나 스캔이 헛돌고 있다",
                        OBSERVATION_QUALIFIER)
                .isNotEmpty();

        List<String> missing = observed.stream()
                .filter(table -> allowlist.stream().noneMatch(table::equalsIgnoreCase))
                .toList();

        assertThat(missing)
                .as("관측 풀이 읽는데 GRANT 가 없는 테이블이다. 운영에서 첫 질의부터 1142 로 죽는다 — "
                        + "infra/mysql/obs-grants/allowlist.txt 에 추가하고 apply.sh 를 다시 돌려라")
                .isEmpty();
    }

    @Test
    @DisplayName("CTE 별칭은 제외하되 CTE 내부 실제 테이블은 찾는다")
    void cteAliasesAreNotTreatedAsObservedTables() {
        String query = """
                WITH ranked_finals AS (
                    SELECT * FROM consistency_finals
                ), benchmark_rounds AS (
                    SELECT * FROM benchmark_runs
                )
                SELECT *
                  FROM benchmark_rounds b
                  JOIN ranked_finals f ON f.coupon_id = b.coupon_id
                  JOIN coupons c ON c.id = b.coupon_id
                """;

        assertThat(tableReferences(query))
                .containsExactlyInAnyOrder("consistency_finals", "benchmark_runs", "coupons");
    }

    @Test
    @DisplayName("④ 양성 목록의 테이블은 전부 실제로 읽히고 있다")
    void everyAllowlistedTableIsActuallyRead() {
        Set<String> observed = tablesReadThroughObservationPool();

        List<String> unused = allowlist().stream()
                .filter(table -> observed.stream().noneMatch(table::equalsIgnoreCase))
                .toList();

        assertThat(unused)
                .as("아무도 안 읽는데 권한만 열려 있다. 양성 목록이 두 번째 스키마 GRANT 가 되는 길이 "
                        + "이것이다 — 읽는 코드가 사라졌으면 목록에서도 빼라")
                .isEmpty();
    }

    @Test
    @DisplayName("⑥ 레거시 과다 권한이 남아 있어도 재부여가 양성 목록으로 되돌린다")
    void reapplyingCollapsesLegacyOverGrantsBackToTheAllowlist() {
        // ①~⑤ 는 **깨끗한 DB 에서 시작했을 때** 무엇이 남는지만 본다. 손으로 넓게 준 운영 DB 는
        // 그 경로를 안 탄다 — 재부여를 돌려도 예전 권한이 살아남으면 members 는 계속 읽힌다.
        // 그 상태를 실제로 만들어 놓고 재부여가 수렴시키는지 본다.
        //
        // ⚠️ 단언은 전부 **재부여 뒤에** 한다. 심어 놓은 상태에서 단언이 깨지면 이 컨테이너를
        //    공유하는 뒤 테스트들이 과다 권한을 물려받아, 원인이 여기라는 것을 알기 어렵다.
        //
        // ⚠️ **역할(ROLE)도 함께 심는다.** MySQL 에서 권한과 역할은 별개 구조라
        //    REVOKE ALL PRIVILEGES 가 역할 할당을 안 걷는다 — 실측으로 확인했고, 그 상태에서
        //    obs 가 members 를 그대로 읽었다. 권한만 심으면 그 구멍이 초록불로 남는다.
        String seeded;
        try {
            MySqlContainerConfig.executeAsRoot(mySqlContainer,
                    "GRANT SELECT ON *.* TO '" + OBSERVATION_USERNAME + "'@'%';"
                            + " GRANT INSERT, UPDATE ON `" + mySqlContainer.getDatabaseName()
                            + "`.* TO '" + OBSERVATION_USERNAME + "'@'%';"
                            + " CREATE ROLE IF NOT EXISTS '" + LEGACY_ROLE + "';"
                            + " GRANT SELECT ON `" + mySqlContainer.getDatabaseName()
                            + "`.* TO '" + LEGACY_ROLE + "';"
                            + " GRANT '" + LEGACY_ROLE + "' TO '" + OBSERVATION_USERNAME + "'@'%';"
                            + " SET DEFAULT ROLE ALL TO '" + OBSERVATION_USERNAME + "'@'%';");
            seeded = showObservationGrants();
        } finally {
            MySqlContainerConfig.applyObservationGrants(mySqlContainer);
        }

        assertThat(seeded)
                .as("레거시 상태를 못 심었다면 이 테스트는 아무것도 검증하지 않는다")
                .contains("ON *.*").contains("INSERT").contains(LEGACY_ROLE);

        String reapplied = showObservationGrants();

        assertThat(reapplied)
                .as("전역 SELECT 가 살아남았다. obs 가 members 를 계속 읽는다 — "
                        + "재부여가 자기가 준 것만 걷으면 손으로 넓게 준 DB 는 영원히 안 좁아진다")
                .doesNotContainPattern("SELECT[^\\n]*ON [*][.][*]");

        assertThat(reapplied)
                .as("쓰기 권한이 살아남았다. 관측 계정은 SELECT 전용이어야 한다")
                .doesNotContain("INSERT").doesNotContain("UPDATE");

        assertThat(reapplied)
                .as("역할 할당이 살아남았다. 그 역할이 무엇을 주든 obs 가 그대로 물려받는다 — "
                        + "REVOKE ALL PRIVILEGES 는 역할을 안 걷으므로 따로 걷어야 한다")
                .doesNotContain(LEGACY_ROLE);

        assertThat(MySqlContainerConfig.executeAsRoot(mySqlContainer,
                "SELECT COUNT(*) FROM mysql.default_roles WHERE USER = '"
                        + OBSERVATION_USERNAME + "'").trim())
                .as("기본 역할 지정이 남아 있다")
                .isEqualTo("0");

        assertThat(reapplied.lines()
                .filter(line -> line.contains("SELECT") && !line.contains(LEGACY_ROLE)).count())
                .as("양성 목록 %s개가 그대로 다시 서 있어야 한다. 걷기만 하고 안 주면 "
                        + "관측이 통째로 멈춘다", allowlist().size())
                .isEqualTo(allowlist().size());
    }

    @Test
    @DisplayName("⑤ 배포 안내와 테스트가 스키마 단위 GRANT 를 되살리지 않는다")
    void nothingOutsideTheAllowlistPathRecreatesASchemaWideGrant() {
        // ①~④ 는 **이 저장소의 절차를 밟았을 때** 무엇이 읽히는지만 본다. 절차 밖에서
        // 스키마 GRANT 가 되살아나면 넷 다 초록불인 채 members 노출이 재발한다.
        // 실제로 두 곳에서 그렇게 살아남아 있었다(둘 다 OBS-36 리뷰에서 발견):
        //   - .env.example  : 배포자에게 스키마 단위로 주라고 **권하고** 있었다
        //   - storage 테스트: 자기 컨테이너에 스키마 단위로 **주고** 있었다. 그 저장소가
        //                     읽는 테이블이 목록 안이라 테스트는 계속 통과했다
        String env = read(repoRoot().resolve(".env.example"));

        assertThat(env)
                .as(".env.example 이 관측 계정 권한을 스키마 단위로 주라고 권하고 있다. "
                        + "배포자가 README 를 안 읽고 이대로 주면 apply.sh 경로를 아예 안 타므로 "
                        + "①~④ 가 전부 초록불인 채 members 노출이 재발한다")
                .doesNotContainPattern(SCHEMA_WIDE_GRANT.pattern());

        assertThat(env)
                .as("배포자가 .env.example 만 보고도 올바른 절차를 찾을 수 있어야 한다. "
                        + "권한 부여 경로(obs-grants)를 여기서 가리켜라")
                .contains("obs-grants");

        assertThat(javaLiteralsGrantingWholeSchema())
                .as("자바 코드가 관측 계정에 스키마 단위 SELECT 를 주고 있다. 그 계정이 읽는 "
                        + "테이블이 마침 목록 안이면 테스트는 계속 통과하므로 스스로 드러나지 "
                        + "않는다 — 테스트가 세우는 권한도 운영과 같은 양성 목록 형태여야 한다")
                .isEmpty();
    }

    @Test
    @DisplayName("⑦ 걷을 수 없는 mandatory_roles 가 걸려 있으면 재부여가 거부한다")
    void reapplyingRefusesWhenTheServerForcesARoleItCannotRevoke() {
        // mandatory_roles 는 서버 전역 설정으로 **모든 계정에 암묵 부여**되는 역할이라
        // mysql.role_edges 에 행이 없고 REVOKE 대상도 되지 못한다. 실측하면 이렇다 —
        // root 의 SHOW GRANTS FOR obs 에는 USAGE 만 보이는데, obs 자신의 세션에서는
        // 그 역할이 활성화돼 members 조회가 성공한다. 즉 ①~⑥ 이 전부 초록불인 채
        // 양성 목록이 통째로 무의미해진다.
        //
        // 걷을 수 없으므로 재부여는 **거부**해야 한다. 조용히 성공해서 "재부여했다" 는
        // 기록을 남기는 것이 이 상황에서 가장 나쁘다.
        //
        // ⚠️ 전역 설정을 건드리므로 finally 에서 반드시 되돌린다. 안 되돌리면 이 컨테이너를
        //    공유하는 뒤 테스트들이 영문 모를 상태를 물려받는다.
        MySqlContainerConfig.executeAsRoot(mySqlContainer,
                "CREATE ROLE IF NOT EXISTS '" + FORCED_ROLE + "'");
        try {
            MySqlContainerConfig.executeAsRoot(mySqlContainer,
                    "SET GLOBAL mandatory_roles = '" + FORCED_ROLE + "'");

            assertThatThrownBy(() -> MySqlContainerConfig.applyObservationGrants(mySqlContainer))
                    .as("걷을 수 없는 역할이 걸려 있는데 재부여가 성공했다. 그 성공은 거짓이다 — "
                            + "양성 목록 밖의 권한이 SHOW GRANTS 에도 안 보이는 채로 남는다")
                    .hasMessageContaining("mandatory_roles");
        } finally {
            MySqlContainerConfig.executeAsRoot(mySqlContainer, "SET GLOBAL mandatory_roles = ''");
            MySqlContainerConfig.applyObservationGrants(mySqlContainer);
        }

        assertThat(showObservationGrants())
                .as("되돌린 뒤 재부여가 정상 동작해야 뒤 테스트들이 성립한다")
                .contains("issuances");
    }

    @Test
    @DisplayName("관측 계정은 read-only 플래그를 꺼도 쓰지 못한다 — GRANT 가 막는다")
    void cannotWriteEvenWithoutReadOnlyFlag() {
        // Hikari 의 read-only 를 켠 채로 시도하면 드라이버가 클라이언트 쪽에서 먼저 거부한다
        // (TransientDataAccessResourceException: Connection is read-only). 그것은 세션 속성일
        // 뿐이라 누가 설정 한 줄을 지우면 사라진다 — 그 상태에서도 막히는지가 이 테스트의 질문이다.
        assertThatThrownBy(() -> {
            try (Connection connection = observationDataSource.getConnection()) {
                connection.setReadOnly(false);
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate(
                            "INSERT INTO BATCH_JOB_INSTANCE"
                                    + " (JOB_INSTANCE_ID, VERSION, JOB_NAME, JOB_KEY)"
                                    + " VALUES (999, 0, 'x', 'x')");
                }
            }
        })
                .isInstanceOf(SQLException.class)
                // MySQL 1142 = 명령에 대한 권한 없음. 메시지가 아니라 코드로 본다.
                .satisfies(thrown -> assertThat(((SQLException) thrown).getErrorCode())
                        .as("GRANT 가 아니라 다른 이유로 막혔다면 이 계약은 검증되지 않는다")
                        .isEqualTo(1142));
    }

    @Test
    @DisplayName("걷는 모듈 목록이 settings.gradle 과 같다")
    void moduleListMatchesSettingsGradle() {
        // 모듈이 늘었는데 여기가 안 늘면 ③ 이 그 모듈의 관측 질의를 못 본다. 대상이 줄어들 뿐
        // 실패하지 않으므로, 두 파일을 잇는 이 단언이 없으면 조용히 통과한다.
        List<String> declared = Pattern.compile("^include\\s+[\"']([^\"']+)[\"']", Pattern.MULTILINE)
                .matcher(read(repoRoot().resolve("settings.gradle")))
                .results()
                .map(match -> match.group(1).replace(':', '/'))
                .toList();

        assertThat(declared).as("settings.gradle 에서 모듈을 하나도 못 읽었다").isNotEmpty();
        assertThat(MODULES).containsExactlyInAnyOrderElementsOf(declared);
    }

    /**
     * 스키마 단위 GRANT 를 <b>실행하는</b> 자바 코드를 찾는다.
     *
     * <p>문자열 리터럴만 본다 — 주석은 대상이 아니다. 이 저장소의 문서·javadoc 은 "예전에는
     * 이랬다" 를 설명하느라 그 문장을 그대로 인용하고 있고, 그것까지 금지하면 역사를 못 적는다.
     * 실행되는 것은 리터럴뿐이다.
     */
    private static List<String> javaLiteralsGrantingWholeSchema() {
        List<String> offenders = new ArrayList<>();
        for (String module : MODULES) {
            for (String sourceSet : List.of("src/main/java", "src/test/java", "src/testFixtures/java")) {
                Path root = repoRoot().resolve(module).resolve(sourceSet);
                if (!Files.isDirectory(root)) {
                    continue;
                }
                try (Stream<Path> paths = Files.walk(root)) {
                    paths.filter(path -> path.toString().endsWith(".java"))
                            .filter(path -> !path.getFileName().toString().equals(THIS_TEST))
                            .forEach(path -> {
                        for (String literal : queryTextOf(read(path))) {
                            if (SCHEMA_WIDE_GRANT.matcher(literal).find()
                                    && !GRANTS_TO_OTHER_ACCOUNT.matcher(literal).find()) {
                                offenders.add(path.getFileName() + " → " + literal.trim());
                            }
                        }
                    });
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        return offenders;
    }

    /** 권한 상태를 서버가 보는 그대로 읽는다. 풀 커넥션은 접속 시점 캐시가 섞인다. */
    private String showObservationGrants() {
        return MySqlContainerConfig.executeAsRoot(mySqlContainer,
                "SHOW GRANTS FOR '" + OBSERVATION_USERNAME + "'@'%'");
    }

    /** {@code infra/mysql/obs-grants/allowlist.txt} 의 테이블 이름. apply.sh 와 같은 규칙으로 읽는다. */
    private static List<String> allowlist() {
        return read(repoRoot().resolve("infra/mysql/obs-grants/allowlist.txt")).lines()
                .map(line -> line.replaceAll("#.*", "").trim())
                .filter(line -> !line.isEmpty())
                .toList();
    }

    /** 관측 한정자를 쓰는 {@code src/main/java} 파일의 질의문에서 {@code FROM}·{@code JOIN} 대상을 모은다. */
    private static Set<String> tablesReadThroughObservationPool() {
        Set<String> tables = new LinkedHashSet<>();
        for (Path file : observationConsumers()) {
            for (String query : queryTextOf(read(file))) {
                tables.addAll(tableReferences(query));
            }
        }
        return tables;
    }

    private static Set<String> tableReferences(String query) {
        Set<String> cteNames = new LinkedHashSet<>();
        Matcher cteMatcher = CTE_NAME.matcher(query);
        while (cteMatcher.find()) {
            cteNames.add(cteMatcher.group(1).toLowerCase(Locale.ROOT));
        }

        Set<String> tables = new LinkedHashSet<>();
        Matcher tableMatcher = TABLE_REF.matcher(query);
        while (tableMatcher.find()) {
            String table = tableMatcher.group(1);
            if (!cteNames.contains(table.toLowerCase(Locale.ROOT))) {
                tables.add(table);
            }
        }
        return tables;
    }

    /**
     * 소스에서 질의문이 될 수 있는 조각만 뽑는다. 주석·식별자를 함께 보면 javadoc 에 적힌
     * 테이블 이름이 "읽고 있다" 로 둔갑해 ④ 가 거짓 통과한다.
     *
     * <p><b>이어 붙인 리터럴을 먼저 합친다.</b> 리터럴을 하나씩 떼어 보면 다음이 빠져나간다 —
     * 실측으로 확인했다(이 처리를 넣기 전에는 ⑤ 가 통과했다):
     * <pre>
     *   root.execute("GRANT SELECT ON " + db + ".* TO 'obs'@'%'");
     * </pre>
     * {@code "GRANT SELECT ON "} 와 {@code ".* TO ..."} 어느 쪽도 혼자서는 스키마 단위 GRANT
     * 로 보이지 않기 때문이다. 실행되는 것은 <b>합쳐진 문자열</b>이므로 합친 뒤에 본다.
     * 사이에 낀 식으로 무엇이 오든 상관없다 — 대상이 {@code .*} 인지만 보면 된다.
     */
    private static List<String> queryTextOf(String source) {
        List<String> chunks = new ArrayList<>();
        Matcher blocks = TEXT_BLOCK.matcher(source);
        while (blocks.find()) {
            chunks.add(blocks.group(1));
        }
        // 텍스트 블록은 위에서 이미 담았으므로 통째로 지운다. 남기면 그 안의 따옴표가
        // 아래 한 줄짜리 리터럴 스캔의 짝을 흐트러뜨린다.
        String flat = joinConcatenatedLiterals(TEXT_BLOCK.matcher(source).replaceAll(""));
        Matcher literals = STRING_LITERAL.matcher(flat);
        while (literals.find()) {
            chunks.add(literals.group());
        }
        return chunks;
    }

    /** {@code "a" + expr + "b"} 와 {@code "a" + "b"} 를 한 리터럴로 합친다. */
    private static String joinConcatenatedLiterals(String source) {
        String previous;
        String current = source;
        do {
            previous = current;
            current = LITERAL_JOIN.matcher(current).replaceAll("");
        } while (!current.equals(previous));
        return current;
    }

    private static List<Path> observationConsumers() {
        List<Path> found = new ArrayList<>();
        for (String module : MODULES) {
            Path main = repoRoot().resolve(module).resolve("src/main/java");
            if (!Files.isDirectory(main)) {
                continue;
            }
            try (Stream<Path> paths = Files.walk(main)) {
                paths.filter(path -> path.toString().endsWith(".java"))
                        .filter(path -> read(path).contains(OBSERVATION_QUALIFIER))
                        .forEach(found::add);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return found;
    }

    private static String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 작업 디렉터리가 모듈마다 달라 위로 올라가며 {@code settings.gradle} 로 찾는다.
     * 상대 경로를 박아 두면 다른 모듈에서 돌릴 때 조용히 대상 0개가 되어 통과한다.
     */
    private static Path repoRoot() {
        Path candidate = Path.of("").toAbsolutePath();
        while (candidate != null && !Files.isRegularFile(candidate.resolve("settings.gradle"))) {
            candidate = candidate.getParent();
        }
        if (candidate == null) {
            throw new IllegalStateException("settings.gradle 을 못 찾았다. 저장소 루트를 알 수 없다");
        }
        return candidate;
    }
}
