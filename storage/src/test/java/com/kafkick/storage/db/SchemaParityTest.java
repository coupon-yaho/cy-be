// cy-be Flyway 가 정의한 스키마와 시드 로더의 DDL 이 같은 최종 상태를 만드는지 확인합니다.
package com.kafkick.storage.db;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.mysql.MySQLContainer;

/**
 * <b>스키마를 정의하는 것은 cy-be 의 Flyway 다.</b> 시드 저장소는 300만 건을 빠르게 넣으려고
 * DDL 을 <i>테이블만 → 적재 → 제약</i> 순서로 쪼개 두는데, 그 <b>파일 구조는 로더의 사정</b>이고
 * 만들어지는 <b>최종 상태는 같아야</b> 한다. 이 테스트가 그 등식을 지킨다.
 *
 * <p><b>왜 문서 결정이 아니라 테스트인가.</b> 이전 결정은 "시드 저장소가 원본이니 cy-be 는
 * 따라간다" 였고 근거는 <i>"cy-be 가 두 번째 주인처럼 보이면 둘이 어긋나도 아무도 모른다"</i>
 * 였다. 모르는 것이 문제였으므로 <b>알게 만드는 것</b>이 답이다 — 주인을 정하는 것으로는
 * 어긋남을 못 잡는다. 실제로 어긋난 적이 있다: {@code datetime} ↔ {@code datetime(6)} 세 컬럼과
 * 제약 이름 두 개가 아무 경고 없이 갈라져 있었다.
 *
 * <p><b>한 컨테이너에 데이터베이스를 둘 만든다.</b> Flyway 가 이미 만든 쪽과, 시드 DDL 을 부은
 * 쪽을 {@code information_schema} 로 대조한다. 컨테이너를 두 번 띄우면 테스트가 배로 느려지고
 * 서버 설정이 갈릴 여지도 생긴다.
 *
 * <p><b>사본은 손으로 고치지 않는다.</b> {@code src/test/resources/seed-ddl/} 은 시드 저장소
 * {@code ddl/} 의 읽기 전용 사본이다({@code docs/contract.json} 과 같은 규율).
 * 갱신 절차는 {@code seed-ddl/README.md} 에 있다.
 */
@RepositoryTest
class SchemaParityTest {

    /**
     * 시드가 CLEAN 셋을 만들 때 붓는 순서. 파일명 순서가 곧 적재 순서다.
     *
     * <p><b>{@code 90_perf_indexes_optional.sql} 은 넣지 않는다.</b> 그 파일은
     * {@code --with-perf-indexes} 를 줬을 때만 적용되는 <b>처방전</b>이고, 보조 인덱스가 기본
     * 스키마에 없는 것은 누락이 아니라 <b>의도</b>다 — 300만 건에서 느린 쿼리를 겪고 실행계획을
     * 보고 인덱스를 처방해 개선폭을 재는 것이 과제의 일부라, 미리 깔면 그 구간이 사라진다.
     * 스키마 정의가 아니므로 이 대조의 대상도 아니다.
     *
     * <p>덤으로 알게 된 것 — 실측해 보니 InnoDB 는 FK 자동 인덱스를 <b>스스로 지운다.</b>
     * {@code (issuance_id, created_at)} 처럼 선두가 일치하는 복합 인덱스를 만들면 자동 생성된
     * {@code issuance_id} 가 사라진다. 처방전을 넣은 쪽과 안 넣은 쪽의 인덱스 목록이 두 군데서
     * 달라 보이는 이유가 이것이고, 처방전을 대조에서 빼면 양쪽 다 자동 인덱스를 갖는다.
     */
    private static final List<String> CLEAN_DDL = List.of(
            "00_schema.sql",
            "10_constraints_common.sql",
            "11_constraints_clean.sql");

    /**
     * 비교에서 빼는 테이블.
     *
     * <p>Spring Batch 메타({@code BATCH_*})는 {@code V2__batch_metadata.sql} 이 만들고 시드는
     * 알 필요가 없다 — 잡 실행 이력이지 도메인이 아니다. {@code flyway_schema_history} 도 같다.
     */
    private static final List<String> IGNORED_PREFIXES = List.of("BATCH_", "flyway_schema_history");

    private static final String SEED_SCHEMA = "seed_parity";

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private MySQLContainer container;

    private String appSchema;

    /**
     * <b>같은 컨테이너에 만든다.</b> 두 번째 컨테이너를 띄우면 {@code --sql-mode}·
     * {@code --collation-server} 같은 서버 플래그를 손으로 복제해야 하고, 그것이 갈리는 순간
     * 대조가 <b>서버 설정 차이를 스키마 차이로 잘못 보고</b>한다.
     *
     * <p>앱 계정은 자기 데이터베이스 밖에 권한이 없어 {@code CREATE DATABASE} 가 거부된다.
     * 그래서 <b>root 로 두 문장만</b> 실행하고 — 만들고, 앱 계정에 권한을 주고 — 나머지 DDL 은
     * 평소 DataSource 로 돌린다. Testcontainers 는 root 비밀번호를 앱 계정과 같은 값으로 넣는다
     * ({@code MySQLContainer} 가 {@code MYSQL_ROOT_PASSWORD} 를 그렇게 설정한다).
     */
    @BeforeEach
    void buildSeedSchema() throws SQLException {
        appSchema = jdbcClient.sql("SELECT DATABASE()").query(String.class).single();

        try (Connection root = DriverManager.getConnection(
                container.getJdbcUrl(), "root", container.getPassword())) {
            exec(root, "DROP DATABASE IF EXISTS " + SEED_SCHEMA);
            exec(root, "CREATE DATABASE " + SEED_SCHEMA
                    + " DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci");
            exec(root, "GRANT ALL PRIVILEGES ON " + SEED_SCHEMA + ".* TO '"
                    + container.getUsername() + "'@'%'");
            exec(root, "FLUSH PRIVILEGES");
        }

        for (String file : CLEAN_DDL) {
            for (String statement : statementsOf(file)) {
                runIn(SEED_SCHEMA, statement);
            }
        }
    }

    /**
     * <b>컬럼이 이 대조의 본체다.</b> 타입·NULL 허용·기본값·자동증가가 하나라도 갈리면 같은
     * 데이터가 두 DB 에서 다르게 저장된다 — {@code datetime} ↔ {@code datetime(6)} 이 실제로
     * 그랬고, 그때 지문이 머신마다 달라졌다.
     */
    @Test
    @DisplayName("컬럼 정의가 시드 DDL 과 같다")
    void matchColumns() {
        assertThat(columnsOf(appSchema))
                .as("왼쪽이 cy-be Flyway, 오른쪽이 시드 DDL 이다. "
                        + "cy-be 가 스키마 주인이므로 시드를 여기에 맞춘다")
                .isEqualTo(columnsOf(SEED_SCHEMA));
    }

    /**
     * <b>인덱스는 이름까지 본다.</b> 컬럼 조합이 같아도 이름이 다르면 같은 스크립트를 두 스키마에
     * 못 돌린다 — 인라인 {@code UNIQUE} 가 만든 {@code code} 와 시드의 {@code uk_coupon_code} 가
     * 그래서 갈려 있었고, CORRUPT 오버레이가 {@code DROP INDEX} 이름을 달리 써야 했다.
     */
    @Test
    @DisplayName("인덱스 이름과 컬럼이 시드 DDL 과 같다")
    void matchIndexes() {
        assertThat(indexesOf(appSchema)).isEqualTo(indexesOf(SEED_SCHEMA));
    }

    /** FK 가 빠지면 고아 행이 생기고, 더 있으면 시드 적재가 순서 때문에 실패한다. */
    @Test
    @DisplayName("외래키가 시드 DDL 과 같다")
    void matchForeignKeys() {
        assertThat(foreignKeysOf(appSchema)).isEqualTo(foreignKeysOf(SEED_SCHEMA));
    }

    /**
     * CHECK 는 불변식을 DB 로 표현한 것이라 한쪽에만 있으면 <b>같은 오염을 한쪽에서만 심을 수
     * 있다.</b> {@code ck_stock_range} 가 cy-be 에 문서로만 있고 DDL 이 빠져 있던 적이 있다.
     */
    @Test
    @DisplayName("CHECK 제약이 시드 DDL 과 같다")
    void matchCheckConstraints() {
        assertThat(checksOf(appSchema)).isEqualTo(checksOf(SEED_SCHEMA));
    }

    // ─────────────────────────── information_schema 조회 ───────────────────────────

    private Map<String, String> columnsOf(String schema) {
        return rowsToMap(schema, """
                SELECT CONCAT(table_name, '.', column_name)                        AS k,
                       CONCAT_WS(' | ', column_type, is_nullable,
                                 COALESCE(column_default, '-'), extra)             AS v
                  FROM information_schema.columns
                 WHERE table_schema = ?
                 ORDER BY table_name, column_name
                """);
    }

    private Map<String, String> indexesOf(String schema) {
        return rowsToMap(schema, """
                SELECT CONCAT(table_name, '.', index_name)                         AS k,
                       CONCAT_WS(' | ', GROUP_CONCAT(column_name
                                        ORDER BY seq_in_index SEPARATOR ','),
                                 IF(MAX(non_unique) = 0, 'UNIQUE', 'INDEX'))       AS v
                  FROM information_schema.statistics
                 WHERE table_schema = ?
                 GROUP BY table_name, index_name
                 ORDER BY table_name, index_name
                """);
    }

    /**
     * <b>이름은 안 본다.</b> 시드는 {@code ADD FOREIGN KEY} 로 이름을 생략해 MySQL 이
     * {@code 테이블_ibfk_N} 을 붙이고, 번호는 <b>붓는 순서에 따라 달라진다.</b>
     * 지키려는 것은 "어느 컬럼이 어디를 가리키는가" 다.
     */
    private Map<String, String> foreignKeysOf(String schema) {
        return rowsToMap(schema, """
                SELECT CONCAT(table_name, '.', column_name)                        AS k,
                       CONCAT(referenced_table_name, '.', referenced_column_name)  AS v
                  FROM information_schema.key_column_usage
                 WHERE table_schema = ? AND referenced_table_name IS NOT NULL
                 ORDER BY table_name, column_name
                """);
    }

    private Map<String, String> checksOf(String schema) {
        return rowsToMap(schema, """
                SELECT CONCAT(tc.table_name, '.', cc.constraint_name)              AS k,
                       cc.check_clause                                             AS v
                  FROM information_schema.check_constraints cc
                  JOIN information_schema.table_constraints tc
                    ON tc.constraint_schema = cc.constraint_schema
                   AND tc.constraint_name   = cc.constraint_name
                 WHERE cc.constraint_schema = ?
                 ORDER BY tc.table_name, cc.constraint_name
                """);
    }

    /** 순서를 지키는 맵으로 받는다. AssertJ 가 어느 키가 다른지 그대로 보여 준다. */
    private Map<String, String> rowsToMap(String schema, String sql) {
        Map<String, String> out = new LinkedHashMap<>();
        jdbcClient.sql(sql).param(schema).query((rs, rowNum) -> {
            String key = rs.getString("k");
            if (IGNORED_PREFIXES.stream().noneMatch(p -> key.startsWith(p))) {
                out.put(key, rs.getString("v"));
            }
            return null;
        }).list();

        return out;
    }

    // ─────────────────────────────── 시드 DDL 실행 ───────────────────────────────

    /**
     * <b>세미콜론으로 쪼갠다.</b> 시드 DDL 에는 트리거·프로시저가 없어 구분자 처리가 필요 없다 —
     * 생기면 이 분할이 조용히 틀리므로, 그때 이 자리를 함께 고쳐야 한다.
     */
    private List<String> statementsOf(String file) {
        String sql;
        try {
            sql = new String(new ClassPathResource("seed-ddl/" + file).getContentAsByteArray(),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("시드 DDL 사본을 읽지 못했습니다: " + file, e);
        }

        List<String> statements = new ArrayList<>();
        for (String raw : sql.split(";")) {
            String stripped = Arrays.stream(raw.split("\n"))
                    .filter(line -> !line.trim().startsWith("--"))
                    .reduce("", (a, b) -> a + "\n" + b)
                    .trim();
            if (!stripped.isEmpty()) {
                statements.add(stripped);
            }
        }

        return statements;
    }

    private void exec(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    /** 시드 DDL 은 테이블명을 스키마 없이 쓴다. 그래서 카탈로그를 바꿔 실행한다. */
    private void runIn(String catalog, String sql) {
        try (Connection connection = dataSource.getConnection()) {
            if (catalog != null) {
                connection.setCatalog(catalog);
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("시드 DDL 실행 실패: " + sql, e);
        }
    }
}
