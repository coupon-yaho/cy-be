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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.mysql.MySQLContainer;

/**
 * <b>스키마를 정의하는 것은 cy-be 의 Flyway 다.</b> 시드 저장소는 300만 건을 빠르게 넣으려고
 * DDL 을 <i>테이블만 → 적재 → 제약</i> 순서로 쪼개 두는데, 그 <b>파일 구조는 로더의 사정</b>이고
 * 만들어지는 <b>최종 상태는 같아야</b> 한다. 이 대조가 그 등식을 지킨다.
 *
 * <p><b>왜 문서 결정이 아니라 테스트인가.</b> 이전 결정은 "시드 저장소가 원본이니 cy-be 는
 * 따라간다" 였고 근거는 <i>"cy-be 가 두 번째 주인처럼 보이면 둘이 어긋나도 아무도 모른다"</i>
 * 였다. 모르는 것이 문제였으므로 <b>알게 만드는 것</b>이 답이다 — 주인을 정하는 것으로는
 * 어긋남을 못 잡는다. 실제로 어긋난 적이 있다: {@code datetime} ↔ {@code datetime(6)} 세 컬럼과
 * 제약 이름 두 개가 아무 경고 없이 갈라져 있었다.
 *
 * <p><b>한 컨테이너에 데이터베이스를 둘 만든다.</b> 두 번째 컨테이너를 띄우면
 * {@code --sql-mode}·{@code --collation-server} 를 손으로 복제해야 하고, 그것이 갈리는 순간
 * 대조가 <b>서버 설정 차이를 스키마 차이로 잘못 보고</b>한다.
 *
 * <p><b>대조하는 축은 여섯이다.</b> 컬럼(순서·콜레이션 포함) · 인덱스 · 외래키(참조 동작 포함) ·
 * CHECK · 뷰 본문 · 테이블 옵션. 하나라도 빠지면 그 축의 어긋남은 초록으로 지나간다 —
 * 실제로 뷰 본문이 빠져 있었고, {@code v_latest_stats_run} 은 <b>컬럼이 하나뿐이고 의미가
 * 전부 본문</b>인 객체다.
 *
 * <p><b>사본은 손으로 고치지 않는다.</b> {@code src/test/resources/seed-ddl/} 은 시드 저장소
 * {@code ddl/} 의 읽기 전용 사본이다({@code docs/contract.json} 과 같은 규율).
 * 갱신 절차는 {@code seed-ddl/README.md} 에 있다.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class SchemaParityTestBase {

    /** 시드가 CLEAN 셋을 만들 때 붓는 순서. {@code cy-seed/bin/seed.py} 의 create/apply 와 같다. */
    static final List<String> CLEAN_DDL = List.of(
            "00_schema.sql", "10_constraints_common.sql", "11_constraints_clean.sql");

    /** CORRUPT 는 CLEAN 전용 제약 대신 오염 전용 인덱스를 붓는다. */
    static final List<String> CORRUPT_DDL = List.of(
            "00_schema.sql", "10_constraints_common.sql", "12_constraints_corrupt.sql");

    /**
     * 일부러 대조하지 않는 파일.
     *
     * <p>{@code 90_perf_indexes_optional.sql} 은 {@code --with-perf-indexes} 를 줬을 때만
     * 적용되는 <b>처방전</b>이다. 보조 인덱스가 기본 스키마에 없는 것은 누락이 아니라 <b>의도</b>다 —
     * 300만 건에서 느린 쿼리를 겪고 실행계획을 보고 인덱스를 처방해 개선폭을 재는 것이 과제의
     * 일부라, 미리 깔면 그 구간이 사라진다. 스키마 정의가 아니므로 대조 대상도 아니다.
     *
     * <p>덤으로 알게 된 것 — 실측해 보니 InnoDB 는 FK 자동 인덱스를 <b>스스로 지운다.</b>
     * {@code (issuance_id, created_at)} 처럼 선두가 일치하는 복합 인덱스를 만들면 자동 생성된
     * {@code issuance_id} 가 사라진다. 처방전을 대조에서 빼면 양쪽 다 자동 인덱스를 갖는다.
     */
    static final List<String> EXCLUDED_DDL = List.of("90_perf_indexes_optional.sql");

    /**
     * 대조에서 빼는 테이블.
     *
     * <p><b>접두사가 아니라 이름 집합이다.</b> {@code startsWith("BATCH_")} 로 거르면 나중에
     * {@code BATCH_RUN_METRICS} 같은 <b>도메인</b> 테이블이 생겼을 때 네 축에서 통째로 사라져,
     * cy-be 에만 있는 테이블이 있는데도 초록이 된다. 이 저장소는 같은 함정을
     * {@code CorruptSchemaShapeTest} 에서 이미 경계했다 — <i>"이름으로 고정하면 검사가
     * 공허해진다"</i>. 제외의 근거는 "이름이 어떻게 시작하나" 가 아니라
     * <b>"{@code V11__batch_metadata.sql} 이 만들었나"</b> 다.
     */
    static final Set<String> BATCH_METADATA = Set.of(
            "BATCH_JOB_INSTANCE", "BATCH_JOB_INSTANCE_SEQ",
            "BATCH_JOB_EXECUTION", "BATCH_JOB_EXECUTION_PARAMS",
            "BATCH_JOB_EXECUTION_CONTEXT", "BATCH_JOB_EXECUTION_SEQ",
            "BATCH_STEP_EXECUTION", "BATCH_STEP_EXECUTION_CONTEXT",
            "BATCH_STEP_EXECUTION_SEQ",
            "flyway_schema_history");

    /**
     * <b>시드가 만들 이유가 없는 표들</b>(CY-744 합류로 들어왔다).
     *
     * <p>이 검사가 지키는 것은 <i>"검증이 읽는 데이터셋의 모양이 두 저장소에서 같은가"</i> 다.
     * 아래 아홉은 <b>관측·부하측정·관리 화면</b>의 표라 시드 데이터셋에 속하지 않는다 —
     * 시드는 발급·이력·재고·판정만 만든다({@code cy-seed/ddl}). 그래서 대조에서 뺀다.
     *
     * <p><b>접두사가 아니라 이름 집합인 이유는 위 {@link #BATCH_METADATA} 와 같다.</b>
     * {@code analytics_*} 로 거르면 나중에 {@code analytics} 로 시작하는 검증 표가 생겼을 때
     * 네 축에서 통째로 사라져, cy-be 에만 있는 표가 있는데도 초록이 된다.
     *
     * <p>⚠️ <b>여기 이름을 더하는 것은 "시드가 안 만든다" 를 선언하는 것이다.</b> 검증이
     * 읽는 표를 실수로 넣으면 그 표의 스키마 드리프트를 아무도 못 본다 — 더하기 전에
     * 그 표를 {@code cy-seed/ddl} 이 만들어야 하는지부터 판단해라.
     *
     * <p><b>그 판단을 주석에 안 맡긴다.</b> "검증이 안 읽는다" 는 이름을 더한 그 시점에
     * 한 번 잰 사실일 뿐이라, 나중에 규칙이 그 표를 참조해도 아무도 못 본다.
     * {@link OutsideSeedDatasetUnreferencedTest} 가 <b>매 빌드마다 다시 잰다.</b>
     */
    static final Set<String> OUTSIDE_SEED_DATASET = Set.of(
            // 브랜드 분석 집계(CY-674) — 관리 화면이 읽는다
            "analytics_runs", "analytics_daily_issues", "analytics_hourly_issues",
            "analytics_issuance_statuses",
            // 부하 측정 회차와 그 시계열·정합성 판정(CY-560)
            "benchmark_runs", "run_timeseries", "consistency_finals",
            // 발급 시도 이력 — Kafka consumer 가 적재한다(OBS-15)
            "issue_attempts",
            // 회차 생성 스케줄러의 싱글턴 가드
            "coupon_round_schedule_guard",
            // 알림 계약과 저장(CY-642) — 관리자 알림 화면과 발송 경로가 읽는다.
            // 시드가 만드는 것은 발급·이력·재고·판정이고 알림은 런타임이 쌓는
            // 산출물이라 데이터셋에 안 속한다.
            "notifications", "notification_attempts",
            "notification_resend_audits", "notification_outbox");

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private MySQLContainer container;

    private String appSchema;

    /** 이 대조가 붓는 시드 DDL. CLEAN 과 CORRUPT 가 다른 것은 이것 하나뿐이다. */
    abstract List<String> seedDdl();

    /** 사본을 부을 데이터베이스 이름. CLEAN·CORRUPT 가 같은 컨테이너를 쓰면 이름이 겹친다. */
    abstract String seedSchema();

    /**
     * <b>클래스당 한 번만 만든다.</b> 붓는 DDL 이 47문장인데 테스트는 전부 읽기만 한다 —
     * 메서드마다 다시 만들면 InnoDB 가 FK 20개를 그때마다 검증한다.
     *
     * <p>앱 계정은 자기 데이터베이스 밖에 권한이 없어 {@code CREATE DATABASE} 가 거부된다.
     * root 로 <b>만들고 권한만 주고</b> 빠진다. Testcontainers 는 root 비밀번호를 앱 계정과
     * 같은 값으로 넣는다({@code MySQLContainer} 가 {@code MYSQL_ROOT_PASSWORD} 를 그렇게 설정).
     *
     * <p><b>DDL 은 전용 커넥션 하나로 붓는다.</b> 애플리케이션 Hikari 풀에서 꺼내 쓰면
     * 시드 DDL 의 세션 문장이 커넥션에 남은 채 반납된다 — Hikari 가 되돌리는 것은
     * {@code autoCommit}·{@code catalog} 등이고 <b>세션 변수는 아니다.</b> 시드가
     * {@code SET FOREIGN_KEY_CHECKS = 0} 같은 줄을 하나 넣는 날, 같은 컨텍스트를 쓰는 다른
     * 테스트가 FK 검사가 꺼진 커넥션을 뽑는다.
     */
    @BeforeAll
    void buildSeedSchema() throws SQLException {
        appSchema = jdbcClient.sql("SELECT DATABASE()").query(String.class).single();

        try (Connection root = DriverManager.getConnection(
                container.getJdbcUrl(), "root", container.getPassword())) {
            exec(root, "DROP DATABASE IF EXISTS " + seedSchema());
            // COLLATE 를 주지 않는다. 시드도 DEFAULT CHARACTER SET utf8mb4 만 쓴다
            // (cy-seed/seedgen/loader.py). 테스트가 시드에 없는 절을 쓰면 그 자리가
            // 대조 대상에서 빠져, 두 DB 의 콜레이션이 실제로 갈려도 초록이 된다.
            exec(root, "CREATE DATABASE " + seedSchema() + " DEFAULT CHARACTER SET utf8mb4");
            exec(root, "GRANT ALL PRIVILEGES ON " + seedSchema() + ".* TO '"
                    + container.getUsername() + "'@'%'");
            exec(root, "FLUSH PRIVILEGES");
        }

        try (Connection seed = DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
            seed.setCatalog(seedSchema());
            for (String file : seedDdl()) {
                for (String statement : statementsOf(file)) {
                    exec(seed, statement);
                }
            }
        }
    }

    /**
     * <b>컬럼이 이 대조의 본체다.</b> 타입·NULL 허용·기본값·자동증가가 하나라도 갈리면 같은
     * 데이터가 두 DB 에서 다르게 저장된다 — {@code datetime} ↔ {@code datetime(6)} 이 실제로
     * 그랬고, 그때 지문이 머신마다 달라졌다.
     *
     * <p><b>{@code ordinal_position} 을 값에 싣는다.</b> 시드의 {@code LOAD DATA} 에는 컬럼
     * 목록이 없어 <b>위치로</b> 넣는다({@code cy-seed/seedgen/loader.py}). 컬럼 하나가 밀리면
     * DDL 도 적재도 통과하고 <b>값만 옆 컬럼으로 간다</b> — {@code char(64)} 끼리면
     * {@code STRICT_TRANS_TABLES} 도 안 튕긴다. 키가 아니라 값에 싣는 이유는, 키에 넣으면
     * 하나가 밀릴 때 뒤 컬럼 전부가 "다른 키" 로 보여 실패 출력을 못 읽기 때문이다.
     */
    @Test
    @DisplayName("컬럼 정의와 순서가 시드 DDL 과 같다")
    void matchColumns() {
        assertThat(columnsOf(appSchema))
                .as("왼쪽이 cy-be Flyway, 오른쪽이 시드 DDL 이다. "
                        + "cy-be 가 스키마 주인이므로 시드를 여기에 맞춘다")
                .isEqualTo(columnsOf(seedSchema()));
    }

    /**
     * <b>인덱스는 이름까지 본다.</b> 컬럼 조합이 같아도 이름이 다르면 같은 스크립트를 두 스키마에
     * 못 돌린다 — 인라인 {@code UNIQUE} 가 만든 {@code code} 와 시드의 {@code uk_coupon_code} 가
     * 그래서 갈려 있었고, CORRUPT 오버레이가 {@code DROP INDEX} 이름을 달리 써야 했다.
     */
    @Test
    @DisplayName("인덱스 이름과 컬럼이 시드 DDL 과 같다")
    void matchIndexes() {
        assertThat(indexesOf(appSchema)).isEqualTo(indexesOf(seedSchema()));
    }

    /**
     * FK 가 빠지면 고아 행이 생기고, 더 있으면 시드 적재가 순서 때문에 실패한다.
     *
     * <p><b>참조 동작도 본다.</b> {@code ON DELETE CASCADE} 가 한쪽에만 붙으면
     * {@code DELETE FROM verification_runs} 한 문장이 한쪽에서만 검출 800행과 통계 스냅샷을
     * 함께 지운다 — 증적을 남기려고 만든 컬럼이 증적과 함께 사라진다.
     */
    @Test
    @DisplayName("외래키와 참조 동작이 시드 DDL 과 같다")
    void matchForeignKeys() {
        assertThat(foreignKeysOf(appSchema)).isEqualTo(foreignKeysOf(seedSchema()));
    }

    /**
     * CHECK 는 불변식을 DB 로 표현한 것이라 한쪽에만 있으면 <b>같은 오염을 한쪽에서만 심을 수
     * 있다.</b> {@code ck_stock_range} 가 cy-be 에 문서로만 있고 DDL 이 빠져 있던 적이 있다.
     */
    @Test
    @DisplayName("CHECK 제약이 시드 DDL 과 같다")
    void matchCheckConstraints() {
        assertThat(checksOf(appSchema)).isEqualTo(checksOf(seedSchema()));
    }

    /**
     * <b>뷰는 본문이 곧 전부다.</b> {@code v_latest_stats_run} 은 컬럼이 {@code id} 하나뿐이라
     * 컬럼 대조로는 실제 테이블과도 구분되지 않는다. 의미는 전부 {@code WHERE}·{@code ORDER BY}·
     * {@code LIMIT} 에 있다.
     *
     * <p>그리고 그 {@code ORDER BY} 절은 <b>PRD 정의에 없는 것을 일부러 더한 것</b>이라
     * (PRD 만 읽은 사람은 지운다) 두 저장소가 갈릴 확률이 가장 높은 객체다.
     */
    @Test
    @DisplayName("뷰 본문이 시드 DDL 과 같다")
    void matchViews() {
        assertThat(viewsOf(appSchema)).isEqualTo(viewsOf(seedSchema()));
    }

    /**
     * <b>{@code ROW_FORMAT} 은 인덱스 키 길이 상한을 바꾼다.</b> {@code COMPACT} 는 접두사가
     * 767바이트라, 한쪽만 그 값이면 컬럼을 넓히는 날 <b>한쪽에서만</b> 유니크 생성이
     * {@code Specified key was too long} 으로 실패한다.
     *
     * <p>{@code table_type} 도 함께 본다 — 뷰와 테이블이 같은 이름을 가지면 컬럼 대조가
     * 둘을 구분하지 못한다.
     */
    @Test
    @DisplayName("테이블 종류와 엔진·행 포맷·콜레이션이 시드 DDL 과 같다")
    void matchTableOptions() {
        assertThat(tableOptionsOf(appSchema)).isEqualTo(tableOptionsOf(seedSchema()));
    }

    /**
     * <b>사본이 원본과 바이트 동일한지 본다.</b>
     *
     * <p>{@code seed-ddl/README.md} 가 <i>"손으로 고치지 않는다"</i> 로 못 박은 규율인데,
     * <b>지키는 그물이 없었다.</b> 형제 {@link #accountForEverySeedDdl} 은 <b>파일 이름만</b>
     * 대조해서, 내용을 고쳐 파리티를 초록으로 만들어도 아무것도 안 걸렸다 —
     * CY-744 합류에서 실제로 그렇게 했고 리뷰가 잡았다.
     *
     * <p><b>왜 이것이 치명적인가.</b> 이 클래스가 증명하기로 한 명제는
     * <i>"cy-be Flyway 가 만든 최종 상태 == 시드가 만든 최종 상태"</i> 다. 사본을 cy-be 에
     * 맞춰 고치면 이 검사는 <b>cy-be 를 cy-be 와 비교하는 항등식</b>이 된다. 게이트의
     * "흔들리지 않는 축" 이 그 등식 위에 서 있다.
     *
     * <p><b>고치는 순서는 하나뿐이다</b> — 원본({@code cy-seed/ddl})을 고치고, 그쪽에서
     * 시드를 다시 돌려 게이트가 성립하는 것을 확인하고, 그 다음에 사본을 덮고 아래 표와
     * README 의 SHA 를 함께 갱신한다. 표만 고치면 이 검사는 다시 항등식이 된다.
     */
    @Test
    @DisplayName("시드 DDL 사본이 README 가 못박은 리비전과 바이트 동일하다")
    void seedDdlCopyIsPristine() throws IOException {
        Map<String, String> actual = new java.util.TreeMap<>();
        for (Resource resource : new PathMatchingResourcePatternResolver()
                .getResources("classpath:seed-ddl/*.sql")) {
            actual.put(resource.getFilename(), sha256(resource));
        }

        assertThat(actual)
                .as("사본을 손으로 고치면 이 검사가 cy-be 를 cy-be 와 비교하는 항등식이 된다. "
                        + "원본(cy-seed/ddl)을 먼저 고치고 거기서 게이트가 성립하는 것을 "
                        + "확인한 뒤 사본을 덮어라 — README 의 SHA 도 함께 고친다")
                .containsExactlyInAnyOrderEntriesOf(SEED_DDL_DIGESTS);
    }

    /**
     * <b>사본이 온 곳을 한 번만 적게 만든다.</b> 리비전은 README 표와
     * {@link #SEED_DDL_REVISION} 두 군데에 적히는데, 둘의 쓰임이 달라 한쪽으로 합칠 수가
     * 없다 — README 쪽은 <b>사람이 갱신 스크립트에 붙여 넣는 값</b>이고 상수 쪽은
     * 아래 해시가 <b>어느 시점 것인지</b>를 말한다. 그러면 남는 수는 <b>어긋남을 잡는 것</b>이다.
     *
     * <p><b>왜 그물이 필요한가.</b> 사본 갱신은 세 손질이 한 묶음이다 — 파일을 덮고,
     * 해시를 고치고, README 의 SHA 를 고친다. 앞의 둘만 하면 {@link #seedDdlCopyIsPristine}
     * 은 <b>초록으로 통과</b>한다(사본과 해시가 서로 맞으니까). 그런데 README 는 옛
     * 리비전을 가리키고 있어서, 다음 사람이 거기 적힌 검증 스크립트를 돌리면 <b>옛
     * 원본을 받아 와</b> diff 가 갈린다. README 가 그 상황을 "사본을 손댄 것이다" 로
     * 읽으라고 시키므로 <b>멀쩡한 사본을 되돌리는</b> 데까지 간다.
     */
    @Test
    @DisplayName("사본 해시가 못박은 리비전과 README 가 적은 리비전이 같다")
    void seedDdlCopyPinsOneRevision() throws IOException {
        String readme = new String(new PathMatchingResourcePatternResolver()
                .getResource("classpath:seed-ddl/README.md")
                .getContentAsByteArray(), StandardCharsets.UTF_8);

        Matcher matcher = Pattern.compile("@ ([0-9a-f]{7,40})").matcher(readme);
        assertThat(matcher.find())
                .as("README 가 '@ <sha>' 꼴로 리비전을 적어야 갱신 스크립트가 그것을 읽는다")
                .isTrue();

        assertThat(matcher.group(1))
                .as("사본을 갱신할 때 세 곳을 함께 고친다 — 파일 · SEED_DDL_DIGESTS · "
                        + "README 의 SHA. 지금은 README 와 SEED_DDL_REVISION 이 갈렸다")
                .isEqualTo(SEED_DDL_REVISION);
    }

    /**
     * <b>비우는 목록에 빠진 표를 사람이 아니라 기계가 찾는다.</b>
     * {@link AppTableCleaner#TABLES_IN_DELETE_ORDER} 안의 표를 FK 로 무는 표는 그 자신도
     * 목록에 있어야 한다 — 없으면 {@code FOREIGN_KEY_CHECKS = 0} 상태의 TRUNCATE 가
     * <b>고아를 남기고 AUTO_INCREMENT 를 되돌린다.</b>
     *
     * <p><b>형제 {@code VerificationSeed#clear} 는 이 실수를 스스로 알려 준다</b> —
     * {@code DELETE} 는 FK 위반을 {@code ERROR 1451} 로 거부하기 때문이다. 그런데 컨텍스트
     * 기동마다 도는 것은 TRUNCATE 쪽이라 <b>조용히 지나가고</b>, 다음 컨텍스트의
     * {@code coupons} id=1 이 앞 테스트가 남긴 자식 행을 입양한 채 나온다. 그 실패는
     * 클래스 실행 순서를 타므로 원인까지 가는 길이 멀다(CY-744 3차 리뷰가 잡았다 —
     * {@code analytics_*} 셋이 실제로 빠져 있었다).
     */
    @Test
    @DisplayName("비우는 표를 FK 로 무는 표가 전부 비우는 목록 안에 있다")
    void appTableCleanerCoversEveryDependent() {
        List<String> cleaned = AppTableCleaner.TABLES_IN_DELETE_ORDER;

        List<String> dependents = jdbcClient.sql("""
                        SELECT DISTINCT table_name
                          FROM information_schema.key_column_usage
                         WHERE table_schema = DATABASE()
                           AND referenced_table_name IN (:tables)
                        """)
                .param("tables", cleaned)
                .query(String.class)
                .list();

        assertThat(dependents)
                .as("이 표들이 비우는 대상을 FK 로 문다. 목록에 없으면 TRUNCATE 가 고아를 "
                        + "남기고 AUTO_INCREMENT 를 되돌린다 — AppTableCleaner 의 목록에 "
                        + "자식이 먼저 오도록 넣어라")
                .allSatisfy(table -> assertThat(cleaned).contains(table));
    }

    private static String sha256(Resource resource) throws IOException {
        try (var in = resource.getInputStream()) {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(in.readAllBytes());
            StringBuilder hex = new StringBuilder(64);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16))
                        .append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 이 없는 JVM 은 없다", e);
        }
    }

    /**
     * <b>사본이 온 리비전.</b> README 표가 같은 값을 적고 {@link #seedDdlCopyPinsOneRevision}
     * 이 둘을 맞춰 본다 — 한쪽만 고치면 빨개진다.
     *
     * <p>드리프트가 실재한다. README 의 SHA 는 <b>갱신 스크립트가 읽는 값</b>이고 아래
     * 해시는 <b>사본이 실제로 무엇인지</b>다. 사본과 해시만 갱신하고 README 를 두면
     * 다음 사람이 검증 스크립트를 돌렸을 때 옛 리비전을 받아 와 diff 가 갈리고,
     * <b>"사본을 손댔다" 로 오진</b>한다. 반대로 README 만 고치면 그 오진이 반대로 난다.
     */
    private static final String SEED_DDL_REVISION = "d220722";

    /** {@link #SEED_DDL_REVISION} 시점 사본의 해시. 사본을 갱신하면 함께 고친다. */
    private static final Map<String, String> SEED_DDL_DIGESTS = Map.ofEntries(
            Map.entry("00_schema.sql",
                    "d6dfcb9fee92179ff66887cfd9156b2e7037121973e579c5f440d9a7b154795a"),
            Map.entry("10_constraints_common.sql",
                    "6d5e4e54ff38e38d5686d84669c7433e98b5acac1fea977533566bfe459de4e6"),
            Map.entry("11_constraints_clean.sql",
                    "c675179a82473621049480068e09d4b22d90be24d51c512e6a502d48142608f2"),
            Map.entry("12_constraints_corrupt.sql",
                    "5d62c1052d4e7f48e4786515e5a7f0efd5ab1efbc615516c802793ede20e1b69"),
            Map.entry("90_perf_indexes_optional.sql",
                    "d3d12c54889cd2fed33973ae61a7d12f7640aeb1f1698fe72429ebea31d6dc35"));

    /**
     * <b>대조에서 뺀 표가 정말 시드 밖인지 기계로 본다.</b>
     *
     * <p>형제 {@link #BATCH_METADATA} 는 <i>"사람의 규율에 맡기지 않는다"</i> 며
     * {@code V11__batch_metadata.sql} 을 파싱해 대조하는데, {@link #OUTSIDE_SEED_DATASET}
     * 만 사람에게 맡기고 있었다.
     *
     * <p><b>위험한 방향만 조용하다.</b> 오타를 내면 그 이름이 아무것도 안 걸러 파리티가
     * 빨개진다 — 바로 드러난다. 반대로 <b>검증이 읽는 표를 실수로 넣으면</b> 여섯 축
     * 전부에서 그 표가 사라져 cy-be 에만 있는 컬럼·인덱스가 있어도 초록이 된다.
     */
    @Test
    @DisplayName("대조에서 뺀 표는 시드 DDL 이 만들지 않는 표다")
    void excludedTablesAreAbsentFromSeedDdl() throws IOException {
        StringBuilder seedSql = new StringBuilder();
        for (Resource resource : new PathMatchingResourcePatternResolver()
                .getResources("classpath:seed-ddl/*.sql")) {
            seedSql.append(new String(resource.getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8)).append('\n');
        }
        String sql = seedSql.toString();

        assertThat(OUTSIDE_SEED_DATASET)
                .as("시드 DDL 이 만드는 표를 여기 넣으면 여섯 축에서 통째로 사라져 "
                        + "그 표의 스키마 드리프트를 아무도 못 본다")
                .noneMatch(table -> sql.matches("(?is).*\\b" + table + "\\b.*"));
    }

    /**
     * <b>목록에 없는 파일은 조용히 무시된다.</b> 시드가 DDL 파일을 하나 더 붓기 시작하면
     * 이 대조가 증명하는 것이 <i>"cy-be = 시드의 전부"</i> 에서 <i>"cy-be = 시드의 일부"</i> 로
     * 바뀌는데, 그것은 부등식이라 어긋남을 못 잡는다. 사람의 규율에 맡기지 않는다.
     */
    @Test
    @DisplayName("사본에 있는 DDL 은 대조하거나 제외 이유가 적혀 있다")
    void accountForEverySeedDdl() throws IOException {
        List<String> onDisk = Arrays.stream(new PathMatchingResourcePatternResolver()
                        .getResources("classpath:seed-ddl/*.sql"))
                .map(Resource::getFilename)
                .sorted()
                .toList();

        assertThat(onDisk)
                .as("시드에 새 DDL 이 생겼다면 CLEAN_DDL·CORRUPT_DDL·EXCLUDED_DDL 중 하나에 "
                        + "넣어야 한다. 제외한다면 왜인지 EXCLUDED_DDL javadoc 에 적는다")
                .containsExactlyInAnyOrderElementsOf(
                        Stream.of(CLEAN_DDL, CORRUPT_DDL, EXCLUDED_DDL)
                                .flatMap(List::stream)
                                .distinct()
                                .toList());
    }

    /**
     * <b>제외 목록을 손으로 관리하면 낡는다.</b> 실제로 낡아 있었다 — Spring Batch 5 의 시퀀스
     * 테이블 이름을 {@code BATCH_JOB_SEQ} 로 적었는데 {@code V11__batch_metadata.sql} 이 만드는 것은
     * {@code BATCH_JOB_INSTANCE_SEQ} 였다. 메타 테이블이 늘거나 이름이 바뀌면 여기서 잡힌다.
     */
    @Test
    @DisplayName("제외 목록이 V11__batch_metadata.sql 이 만드는 배치 메타와 정확히 같다")
    void ignoreExactlyBatchMetadata() throws IOException {
        String batchMetadataSql = new String(new org.springframework.core.io.ClassPathResource(
                "db/migration/V11__batch_metadata.sql").getContentAsByteArray(),
                StandardCharsets.UTF_8);
        List<String> declared = java.util.regex.Pattern
                .compile("CREATE TABLE\\s+`?(\\w+)`?", java.util.regex.Pattern.CASE_INSENSITIVE)
                .matcher(batchMetadataSql).results()
                .map(m -> m.group(1))
                .toList();

        assertThat(declared)
                .as("V11__batch_metadata.sql 이 만드는 테이블이 늘었다면 BATCH_METADATA 도 같이 늘려야 한다")
                .isNotEmpty()
                .allMatch(BATCH_METADATA::contains);
        assertThat(BATCH_METADATA)
                .containsExactlyInAnyOrderElementsOf(
                        Stream.concat(declared.stream(), Stream.of("flyway_schema_history"))
                                .toList());
    }

    // ─────────────────────────── information_schema 조회 ───────────────────────────

    private Map<String, String> columnsOf(String schema) {
        return rowsToMap(schema, """
                SELECT table_name                                                  AS t,
                       CONCAT(table_name, '.', column_name)                        AS k,
                       CONCAT_WS(' | ', ordinal_position, column_type, is_nullable,
                                 COALESCE(column_default, '-'), extra,
                                 COALESCE(collation_name, '-'))                    AS v
                  FROM information_schema.columns
                 WHERE table_schema = ?
                 ORDER BY table_name, column_name
                """);
    }

    private Map<String, String> indexesOf(String schema) {
        return rowsToMap(schema, """
                SELECT table_name                                                  AS t,
                       CONCAT(table_name, '.', index_name)                         AS k,
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
     *
     * <p>그래서 {@code Map} 이 아니라 정렬된 {@code List} 로 비교한다 — 키를
     * {@code 테이블.컬럼} 으로 두면 복합 FK 나 같은 컬럼의 두 번째 FK 가 덮여 사라진다.
     */
    private List<String> foreignKeysOf(String schema) {
        return jdbcClient.sql("""
                        SELECT kcu.table_name                                      AS t,
                               CONCAT_WS(' | ', kcu.table_name, kcu.column_name,
                                         kcu.ordinal_position,
                                         kcu.referenced_table_name,
                                         kcu.referenced_column_name,
                                         rc.delete_rule, rc.update_rule)           AS v
                          FROM information_schema.key_column_usage kcu
                          JOIN information_schema.referential_constraints rc
                            ON rc.constraint_schema = kcu.constraint_schema
                           AND rc.constraint_name   = kcu.constraint_name
                         WHERE kcu.table_schema = ?
                           AND kcu.referenced_table_name IS NOT NULL
                         ORDER BY kcu.table_name, kcu.column_name, kcu.ordinal_position
                        """)
                .param(schema)
                // rowsToMap 을 안 지나는 유일한 축이라 제외 둘을 여기서도 적용한다.
                // 하나만 빼면 관측·부하측정 표의 FK 가 이 축에서만 살아남는다(실측: 8건).
                .query((rs, rowNum) -> BATCH_METADATA.contains(rs.getString("t"))
                        || OUTSIDE_SEED_DATASET.contains(rs.getString("t"))
                        ? null : rs.getString("v"))
                .list().stream().filter(java.util.Objects::nonNull).toList();
    }

    private Map<String, String> checksOf(String schema) {
        return rowsToMap(schema, """
                SELECT tc.table_name                                               AS t,
                       CONCAT(tc.table_name, '.', cc.constraint_name)              AS k,
                       cc.check_clause                                             AS v
                  FROM information_schema.check_constraints cc
                  JOIN information_schema.table_constraints tc
                    ON tc.constraint_schema = cc.constraint_schema
                   AND tc.constraint_name   = cc.constraint_name
                 WHERE cc.constraint_schema = ?
                 ORDER BY tc.table_name, cc.constraint_name
                """);
    }

    /**
     * MySQL 은 뷰 본문을 정규화해 보관하는데 <b>스키마 이름을 박아 넣는다.</b>
     * 두 뷰가 다른 데이터베이스에 있으므로 그 접두사를 지우고 비교한다 — 안 지우면
     * 언제나 다르다고 나와 이 축이 사실상 꺼진다.
     */
    private Map<String, String> viewsOf(String schema) {
        Map<String, String> out = new LinkedHashMap<>();
        rowsToMap(schema, """
                SELECT table_name                                                  AS t,
                       table_name                                                  AS k,
                       view_definition                                             AS v
                  FROM information_schema.views
                 WHERE table_schema = ?
                 ORDER BY table_name
                """).forEach((k, v) -> out.put(k, v.replace("`" + schema + "`.", "")));

        return out;
    }

    private Map<String, String> tableOptionsOf(String schema) {
        return rowsToMap(schema, """
                SELECT table_name                                                  AS t,
                       table_name                                                  AS k,
                       CONCAT_WS(' | ', table_type, COALESCE(engine, '-'),
                                 COALESCE(row_format, '-'),
                                 COALESCE(table_collation, '-'))                   AS v
                  FROM information_schema.tables
                 WHERE table_schema = ?
                 ORDER BY table_name
                """);
    }

    /** 순서를 지키는 맵으로 받는다. AssertJ 가 어느 키가 다른지 그대로 보여 준다. */
    private Map<String, String> rowsToMap(String schema, String sql) {
        Map<String, String> out = new LinkedHashMap<>();
        jdbcClient.sql(sql).param(schema).query((rs, rowNum) -> {
            String table = rs.getString("t");
            if (!BATCH_METADATA.contains(table) && !OUTSIDE_SEED_DATASET.contains(table)) {
                out.put(rs.getString("k"), rs.getString("v"));
            }
            return null;
        }).list();

        return out;
    }

    // ─────────────────────────────── 시드 DDL 실행 ───────────────────────────────

    /**
     * <b>세미콜론으로 쪼갠다.</b> 시드 DDL 에는 트리거·프로시저가 없어 구분자 처리가 필요 없다 —
     * 생기면 이 분할이 조용히 틀리므로, 그때 이 자리를 함께 고쳐야 한다.
     *
     * <p><b>{@code SET} 문은 버린다.</b> 스키마 모양을 만들지 않고, 세션 상태만 남긴다.
     */
    private List<String> statementsOf(String file) {
        String sql;
        try {
            sql = new String(new ClassPathResourceBytes(file).read(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("시드 DDL 사본을 읽지 못했습니다: " + file, e);
        }

        List<String> statements = new ArrayList<>();
        for (String raw : sql.split(";")) {
            String stripped = Arrays.stream(raw.split("\n"))
                    .filter(line -> !line.trim().startsWith("--"))
                    .reduce("", (a, b) -> a + "\n" + b)
                    .trim();
            if (!stripped.isEmpty() && !stripped.regionMatches(true, 0, "SET ", 0, 4)) {
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

    /** {@code ClassPathResource} 를 람다 밖에서 쓰기 위한 얇은 감싸개. */
    private record ClassPathResourceBytes(String file) {

        byte[] read() throws IOException {
            return new org.springframework.core.io.ClassPathResource("seed-ddl/" + file)
                    .getContentAsByteArray();
        }
    }
}
