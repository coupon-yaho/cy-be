package com.kafkick.storage.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import com.zaxxer.hikari.HikariDataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * <b>append-only 는 규율이 아니라 제약이어야 한다.</b>
 *
 * <p>본 코드에 {@code issuance_histories} 를 {@code UPDATE}·{@code DELETE} 하는 곳은
 * 하나도 없다(실측). 그래서 이 권한 구성은 <b>동작을 바꾸지 않고 이미 사실인 것을 강제로
 * 만든다</b> — 누가 나중에 "한 줄만 고치면 되는데" 라고 생각했을 때 DB 가 막는다.
 *
 * <h2>왜 좁은 GRANT 를 얹는 것으로는 안 되나</h2>
 *
 * <p><b>MySQL 권한은 가산적이다.</b> 도커 이미지가 만드는 계정은
 * {@code GRANT ALL ON <db>.*} 를 갖는데, 거기에
 * {@code GRANT SELECT, INSERT ON <db>.issuance_histories} 를 얹어도 유효 권한은 합집합이라
 * 여전히 전권이다. <b>테이블 단위 REVOKE 로도 못 걷는다</b> — {@code AGENTS.md} 에
 * {@code ERROR 1147} 로 실측이 적혀 있다.
 *
 * <p>그래서 {@code apply.sh} 는 <b>먼저 걷고 다시 준다.</b> 이 테스트는 그 순서가 실제로
 * 효과를 내는지 <b>진짜 MySQL 에서</b> 확인한다 — 스크립트를 읽어 보는 것으로는 알 수 없다.
 *
 * <h2>왜 앱 계정이 아니라 프로브 계정인가</h2>
 *
 * <p>진짜 앱 계정의 권한을 좁히면 <b>이 스위트의 다른 테스트가 깨진다</b> — 픽스처를
 * 정리하려고 {@code DELETE FROM issuance_histories} 를 하는 곳이 넷 있다.
 * 그것을 고치는 것은 이 티켓의 범위가 아니고, 여기서 재려는 것은 <b>권한 구성이 막느냐</b>이지
 * 앱이 그 구성에서 도느냐가 아니다.
 */
@RepositoryTest
@Import(MySqlContainerConfig.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class AppAccountAppendOnlyTest {

    private static final String PROBE_USER = "app_probe";
    private static final String PROBE_PASSWORD = "probe-pw";

    /** 프로브 계정에 심는 레거시 역할. 스크립트가 역할까지 걷는지 이것으로 잰다. */
    private static final String LEGACY_ROLE = "app_probe_legacy_writer";

    /** 운영에서 도는 그 디렉터리다. 목록 파일을 같이 옮겨야 스크립트가 읽는다. */
    private static final Path SCRIPT_DIR = Path.of("..", "infra", "mysql", "app-grants");

    /**
     * 스크립트가 Flyway 종료를 판정할 때 대조하는 정답지. compose 가 마운트하는 것과 같은
     * 디렉터리다 — 컨테이너에는 Testcontainers 가 Flyway 를 이미 다 돌려 놓았으므로
     * 여기 있는 버전이 전부 {@code flyway_schema_history} 에 들어와 있어야 통과한다.
     */
    private static final Path MIGRATION_DIR =
            Path.of("src", "main", "resources", "db", "migration");

    @Autowired
    MySQLContainer mySqlContainer;

    @Autowired
    JdbcTemplate jdbcTemplate;

    private HikariDataSource probeDataSource;
    private HikariDataSource rootDataSource;

    /** 루트 풀은 <b>하나만</b> 만든다. 호출마다 만들면 커넥션과 풀 스레드가 쌓인다. */
    private final java.util.function.Supplier<JdbcTemplate> rootTemplate = () -> {
        if (rootDataSource == null) {
            rootDataSource = new HikariDataSource();
            rootDataSource.setJdbcUrl(mySqlContainer.getJdbcUrl());
            rootDataSource.setUsername("root");
            rootDataSource.setPassword(mySqlContainer.getPassword());
            rootDataSource.setMaximumPoolSize(1);
        }
        return new JdbcTemplate(rootDataSource);
    };

    /**
     * <b>운영에서 도는 그 스크립트를 그대로 돌린다.</b>
     *
     * <p>권한 SQL 을 자바로 다시 쓰면 <b>스크립트가 깨져도 이 테스트는 통과한다</b> —
     * 셸 문법, 역할 회수, 테이블 열거가 전부 검증 대상에서 빠진다. 리뷰가 그것을 짚었고,
     * 첫 판이 실제로 그랬다.
     *
     * <p>대상만 프로브 계정으로 바꾼다({@code DB_USERNAME}). 진짜 앱 계정을 좁히면
     * 픽스처를 정리하려고 이력을 {@code DELETE} 하는 다른 테스트 넷이 깨진다.
     */
    @BeforeEach
    void runTheRealScript() throws Exception {
        JdbcTemplate root = rootTemplate.get();
        root.execute("CREATE USER IF NOT EXISTS '" + PROBE_USER + "'@'%' IDENTIFIED BY '"
                + PROBE_PASSWORD + "'");
        root.execute("ALTER USER '" + PROBE_USER + "'@'%' IDENTIFIED BY '" + PROBE_PASSWORD + "'");
        // 도커 이미지가 만드는 계정과 같은 출발점 — 스키마 전권이다.
        root.execute("GRANT ALL PRIVILEGES ON `" + mySqlContainer.getDatabaseName()
                + "`.* TO '" + PROBE_USER + "'@'%'");

        // **역할로도 권한이 들어온다.** REVOKE ALL PRIVILEGES 는 역할을 떼지 않으므로,
        // 스크립트가 role_edges 를 안 걷으면 이 역할이 UPDATE·DELETE 를 그대로 준다.
        // 이 줄이 없으면 역할 회수 경로가 깨져도 아래 ②③ 이 통과한다(리뷰가 짚었다).
        root.execute("CREATE ROLE IF NOT EXISTS '" + LEGACY_ROLE + "'");
        root.execute("GRANT UPDATE, DELETE ON `" + mySqlContainer.getDatabaseName()
                + "`.* TO '" + LEGACY_ROLE + "'");
        root.execute("GRANT '" + LEGACY_ROLE + "' TO '" + PROBE_USER + "'@'%'");
        root.execute("SET DEFAULT ROLE ALL TO '" + PROBE_USER + "'@'%'");
        root.execute("FLUSH PRIVILEGES");

        mySqlContainer.copyFileToContainer(
                MountableFile.forHostPath(SCRIPT_DIR), "/app-grants");
        mySqlContainer.copyFileToContainer(
                MountableFile.forHostPath(MIGRATION_DIR), "/migrations");
        var result = mySqlContainer.execInContainer(
                "env",
                "MYSQL_ROOT_PASSWORD=" + mySqlContainer.getPassword(),
                "MYSQL_DATABASE=" + mySqlContainer.getDatabaseName(),
                "DB_USERNAME=" + PROBE_USER,
                "MYSQL_HOST=127.0.0.1",
                "sh", "/app-grants/apply.sh");
        assertThat(result.getExitCode())
                .as("apply.sh 가 실패했습니다.%nstdout=%s%nstderr=%s",
                        result.getStdout(), result.getStderr())
                .isZero();

        probeDataSource = new HikariDataSource();
        probeDataSource.setJdbcUrl(mySqlContainer.getJdbcUrl());
        probeDataSource.setUsername(PROBE_USER);
        probeDataSource.setPassword(PROBE_PASSWORD);
        probeDataSource.setMaximumPoolSize(1);
    }

    @AfterEach
    void dropProbe() {
        if (probeDataSource != null) {
            probeDataSource.close();
        }
        rootTemplate.get().execute("DROP USER IF EXISTS '" + PROBE_USER + "'@'%'");
        rootTemplate.get().execute("DROP ROLE IF EXISTS '" + LEGACY_ROLE + "'");
        if (rootDataSource != null) {
            rootDataSource.close();
            rootDataSource = null;
        }
    }

    /**
     * <b>{@code SELECT COUNT(*)} 는 INSERT 권한을 검증하지 않는다.</b> 첫 판이 그랬고,
     * 그래서 GRANT 에서 {@code INSERT} 가 빠져도 통과했다(리뷰가 짚었다).
     *
     * <p>행을 남기지 않는 {@code INSERT ... SELECT ... WHERE 1=0} 으로 <b>권한만</b> 태운다 —
     * 권한 검사는 실행 계획보다 먼저 돌므로 0행이어도 1142 가 난다.
     */
    @Test
    @DisplayName("① 넣는 것은 된다 — append-only 는 쓰기를 막는 것이 아니다")
    void canStillInsertHistory() {
        assertThatCode(() -> probe().update("""
                INSERT INTO issuance_histories
                       (issuance_id, event_type, from_status, to_status, created_at)
                SELECT 0, 'PROBE', NULL, 'PROBE', CURRENT_TIMESTAMP(6)
                  FROM DUAL WHERE 1 = 0
                """))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("② 고치는 것은 막힌다 — MySQL 1142")
    void cannotUpdateHistory() {
        assertThatThrownBy(() -> probe().update(
                "UPDATE issuance_histories SET reason = 'tampered' WHERE id = 1"))
                .as("스키마 GRANT 가 남아 있으면 이 문장이 조용히 성공한다")
                .rootCause()
                .isInstanceOfSatisfying(SQLException.class, cause -> assertThat(cause.getErrorCode())
                        .as("테이블이 없어서(1146) 실패한 것이라면 계약이 검증되지 않았다. "
                                + "1142 = command denied")
                        .isEqualTo(1142));
    }

    @Test
    @DisplayName("③ 지우는 것도 막힌다 — MySQL 1142")
    void cannotDeleteHistory() {
        assertThatThrownBy(() -> probe().update("DELETE FROM issuance_histories WHERE id = 1"))
                .rootCause()
                .isInstanceOfSatisfying(SQLException.class, cause -> assertThat(cause.getErrorCode())
                        .isEqualTo(1142));
    }

    /**
     * <b>예외가 아예 없어야 한다.</b> 첫 판은 <i>"1142 만 아니면 통과"</i> 였는데,
     * 그러면 연결 오류나 문법 오류도 성공으로 친다(리뷰가 짚었다).
     * 실재하는 컬럼을 0행 갱신한다.
     */
    @Test
    @DisplayName("④ 목록 밖 테이블은 그대로 고칠 수 있다 — 좁힌 것이 이력뿐이어야 한다")
    void otherTablesKeepFullAccess() {
        assertThatCode(() -> probe().update(
                "UPDATE issuances SET status = status WHERE 1 = 0"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("⑤ 목록에 있는 이름이 실제 테이블이어야 한다 — 오타는 조용히 아무것도 안 막는다")
    void everyAppendOnlyNameIsARealTable() {
        List<String> tables = appendOnlyTables();
        assertThat(tables).isNotEmpty();

        for (String table : tables) {
            Integer found = jdbcTemplate.queryForObject("""
                    SELECT COUNT(*) FROM information_schema.TABLES
                     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    """, Integer.class, mySqlContainer.getDatabaseName(), table);
            assertThat(found)
                    .as("append-only.txt 의 '%s' 가 스키마에 없습니다. 오타면 그 테이블은 "
                            + "아무 제약도 못 받고, 아무도 모릅니다", table)
                    .isEqualTo(1);
        }
    }

    private JdbcTemplate probe() {
        return new JdbcTemplate(probeDataSource);
    }

    /** 스크립트가 읽는 것과 <b>같은 파일</b>을 읽는다. 목록을 여기 옮겨 적으면 갈린다. */
    private static List<String> appendOnlyTables() {
        Path file = SCRIPT_DIR.resolve("append-only.txt");
        try {
            return Files.readAllLines(file).stream()
                    .map(line -> line.replaceAll("#.*", "").trim())
                    .filter(line -> !line.isEmpty())
                    .toList();
        } catch (Exception e) {
            throw new IllegalStateException("append-only 목록을 못 읽었습니다: " + file.toAbsolutePath(), e);
        }
    }

    /**
     * <b>DELETE 를 막아도 DROP 권한이 있으면 이력을 통째로 날릴 수 있다.</b>
     * {@code TRUNCATE TABLE} 이 {@code DROP} 권한으로 돌기 때문이다 — 리뷰가 짚었고,
     * 첫 판은 Flyway 때문에 {@code DROP} 을 스키마 단위로 주고 있어서 <b>append-only 가
     * 성립하지 않았다.</b>
     *
     * <p>지금 마이그레이션 중 {@code DROP TABLE}·{@code TRUNCATE} 를 쓰는 것은 하나도 없다
     * (실측). 쓰게 되면 그 마이그레이션이 막히고, 그때 DDL 계정을 분리해야 한다.
     */
    @Test
    @DisplayName("⑥ 비우는 것도 막힌다 — TRUNCATE 는 DROP 권한으로 돈다")
    void cannotTruncateHistory() {
        assertThatThrownBy(() -> probe().execute("TRUNCATE TABLE issuance_histories"))
                .as("DROP 을 스키마 단위로 주면 이 문장이 성공해 append-only 가 무너진다")
                .rootCause()
                .isInstanceOfSatisfying(SQLException.class, cause -> assertThat(cause.getErrorCode())
                        .isEqualTo(1142));
    }

    /** 마이그레이션이 계속 돌아야 한다 — DDL 을 통째로 걷으면 다음 배포가 죽는다. */
    @Test
    @DisplayName("⑦ 스키마 변경은 여전히 된다 — Flyway 가 이 계정으로 돈다")
    void ddlStillWorksForFlyway() {
        assertThatCode(() -> {
            probe().execute("CREATE TABLE probe_ddl_check (id BIGINT PRIMARY KEY)");
            probe().execute("ALTER TABLE probe_ddl_check ADD COLUMN note VARCHAR(10)");
        }).doesNotThrowAnyException();
        // DROP 권한이 없으므로 정리는 root 가 한다 — 그 사실 자체가 이 PR 의 요점이다.
        rootTemplate.get().execute("DROP TABLE IF EXISTS probe_ddl_check");
    }

    /**
     * <b>"락이 없다" 를 "끝났다" 로 읽으면 안 된다.</b> 락이 없다는 것은 아직 시작을 안 한
     * 것이기도 하다 — 기존 스키마가 있는 재배포에서 스크립트가 Flyway 보다 먼저 통과해
     * 버리면, 새로 생길 테이블이 DML 권한을 못 받고 앱이 런타임에 1142 로 죽는다.
     * 리뷰가 짚은 이 구멍 때문에 판정을 <b>버전 대조</b>로 바꿨다.
     *
     * <p>여기서는 이 빌드에만 있고 아직 적용되지 않은 마이그레이션을 하나 끼워 넣는다.
     * 컨테이너의 {@code flyway_schema_history} 는 이미 완전하고 락도 없으므로 <b>옛 검사는
     * 그대로 통과했을</b> 상황이다. 버전 대조는 통과하면 안 된다.
     */
    @Test
    @DisplayName("⑧ 안 돌아간 마이그레이션이 남아 있으면 통과하지 않는다")
    void refusesWhenAShippedMigrationHasNotRunYet() throws Exception {
        String unapplied = "V9999999999__probe_never_applied.sql";
        mySqlContainer.copyFileToContainer(
                Transferable.of("SELECT 1;"), "/migrations/" + unapplied);
        try {
            var result = mySqlContainer.execInContainer(
                    "env",
                    "MYSQL_ROOT_PASSWORD=" + mySqlContainer.getPassword(),
                    "MYSQL_DATABASE=" + mySqlContainer.getDatabaseName(),
                    "DB_USERNAME=" + PROBE_USER,
                    "MYSQL_HOST=127.0.0.1",
                    // 기다릴 이유가 없다 — 이 버전은 영원히 안 온다.
                    "APP_GRANTS_WAIT_SECONDS=0",
                    "sh", "/app-grants/apply.sh");
            assertThat(result.getExitCode())
                    .as("적용 안 된 마이그레이션이 남았는데 통과했습니다.%nstdout=%s%nstderr=%s",
                            result.getStdout(), result.getStderr())
                    .isNotZero();
            assertThat(result.getStderr())
                    .as("몇 건 중 몇 건인지 말해 주지 않으면 사람이 원인을 못 찾는다")
                    .contains("마이그레이션이 끝나지 않았다");
        } finally {
            mySqlContainer.execInContainer("rm", "-f", "/migrations/" + unapplied);
        }
    }
}
