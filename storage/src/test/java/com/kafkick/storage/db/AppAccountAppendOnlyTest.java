package com.kafkick.storage.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

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
import org.testcontainers.mysql.MySQLContainer;

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

    @Autowired
    MySQLContainer mySqlContainer;

    @Autowired
    JdbcTemplate jdbcTemplate;

    private HikariDataSource probeDataSource;

    /** {@code apply.sh} 가 만드는 것과 <b>같은 형태</b>의 권한을 프로브 계정에 준다. */
    @BeforeEach
    void grantLikeTheScriptDoes() throws Exception {
        String database = mySqlContainer.getDatabaseName();
        JdbcTemplate root = rootTemplate();

        root.execute("CREATE USER IF NOT EXISTS '" + PROBE_USER + "'@'%' IDENTIFIED BY '"
                + PROBE_PASSWORD + "'");
        root.execute("ALTER USER '" + PROBE_USER + "'@'%' IDENTIFIED BY '" + PROBE_PASSWORD + "'");
        // ① 먼저 전권을 준다 — 도커 이미지가 만드는 계정과 같은 출발점이다.
        root.execute("GRANT ALL PRIVILEGES ON `" + database + "`.* TO '" + PROBE_USER + "'@'%'");
        // ② 스크립트와 같은 순서로 걷고 다시 준다.
        root.execute("REVOKE ALL PRIVILEGES, GRANT OPTION FROM '" + PROBE_USER + "'@'%'");
        root.execute("GRANT CREATE, ALTER, DROP, INDEX, REFERENCES ON `" + database
                + "`.* TO '" + PROBE_USER + "'@'%'");
        for (String table : appendOnlyTables()) {
            root.execute("GRANT SELECT, INSERT ON `" + database + "`.`" + table + "` TO '"
                    + PROBE_USER + "'@'%'");
        }
        root.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON `" + database
                + "`.`issuances` TO '" + PROBE_USER + "'@'%'");
        root.execute("FLUSH PRIVILEGES");

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
        rootTemplate().execute("DROP USER IF EXISTS '" + PROBE_USER + "'@'%'");
    }

    @Test
    @DisplayName("① 넣는 것은 된다 — append-only 는 쓰기를 막는 것이 아니다")
    void canStillInsertHistory() {
        assertThatCode(() -> probe().queryForObject(
                "SELECT COUNT(*) FROM issuance_histories", Integer.class))
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

    @Test
    @DisplayName("④ 목록 밖 테이블은 그대로 고칠 수 있다 — 좁힌 것이 이력뿐이어야 한다")
    void otherTablesKeepFullAccess() {
        assertThatCode(() -> probe().update(
                "UPDATE issuances SET reason_placeholder = reason_placeholder WHERE 1 = 0"))
                .satisfiesAnyOf(
                        // 컬럼이 없으면 1054 다 — 권한 문제가 아니라는 것이 요점이다.
                        thrown -> assertThat(thrown).isNull(),
                        thrown -> assertThat(rootErrorCode(thrown)).isNotEqualTo(1142));
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

    private static int rootErrorCode(Throwable thrown) {
        Throwable cause = thrown;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause instanceof SQLException sql ? sql.getErrorCode() : -1;
    }

    private JdbcTemplate probe() {
        return new JdbcTemplate(probeDataSource);
    }

    private JdbcTemplate rootTemplate() {
        HikariDataSource root = new HikariDataSource();
        root.setJdbcUrl(mySqlContainer.getJdbcUrl());
        root.setUsername("root");
        root.setPassword(mySqlContainer.getPassword());
        root.setMaximumPoolSize(1);
        return new JdbcTemplate((DataSource) root);
    }

    /** 스크립트가 읽는 것과 <b>같은 파일</b>을 읽는다. 목록을 여기 옮겨 적으면 갈린다. */
    private static List<String> appendOnlyTables() {
        Path file = Path.of("..", "infra", "mysql", "app-grants", "append-only.txt");
        try {
            return Files.readAllLines(file).stream()
                    .map(line -> line.replaceAll("#.*", "").trim())
                    .filter(line -> !line.isEmpty())
                    .toList();
        } catch (Exception e) {
            throw new IllegalStateException("append-only 목록을 못 읽었습니다: " + file.toAbsolutePath(), e);
        }
    }
}
