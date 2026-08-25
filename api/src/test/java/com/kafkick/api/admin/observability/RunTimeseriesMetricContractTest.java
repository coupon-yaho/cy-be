package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesKey;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.Metric;

/**
 * 지연 축 계약이 <b>세 곳</b>에 걸쳐 있음을 한 테스트로 잇습니다.
 *
 * <p>같은 값 집합이 프론트가 읽는 {@link SeriesKey} · archive 가 쓰는 {@link Metric} ·
 * DB 의 {@code ck_timeseries_metric} 세 곳에 적혀 있습니다. <b>각각을 따로 보는 테스트 셋으로는
 * 이 계약을 못 지킵니다</b> — 한 곳만 늘리면 세 테스트가 모두 통과하고, 실패는 실제 회차를
 * archive 하는 순간 CHECK 위반으로 처음 나타납니다. 그때는 회차가 이미 끝나 있어 재수집이
 * 안 됩니다.</p>
 *
 * <p><b>왜 DB 를 띄우지 않는가.</b> 여기서 보는 것은 "마이그레이션 파일이 무엇을 허용한다고
 * 적었는가" 이고, "그 파일이 실제 MySQL 에서 그 값을 받는가" 는 컨테이너를 띄우는
 * {@code RunTimeseriesMigrationTest} 가 {@link Metric#values()} 를 전부 적재해 봅니다. 둘이
 * 이어져야 사슬이 닫힙니다.</p>
 */
class RunTimeseriesMetricContractTest {

    /**
     * {@code ck_timeseries_metric} 의 <b>떨구기와 정의를 한 흐름으로</b> 읽습니다.
     *
     * <p>첫 갈래가 DROP, 둘째 갈래가 정의입니다 — 그룹 1 이 비면 그 지점에서 제약이 사라졌다는
     * 뜻이고, 차면 그 값 집합으로 되살아났다는 뜻입니다.</p>
     */
    private static final Pattern CHECK_DROP_OR_DEFINITION = Pattern.compile(
            "DROP\\s+CHECK\\s+`?ck_timeseries_metric`?"
                    + "|ck_timeseries_metric`?[^;]*?IN\\s*\\(([^)]*)\\)",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

    /** {@code metric} 컬럼의 유효 폭. 마지막 정의가 이깁니다. */
    private static final Pattern METRIC_COLUMN_WIDTH =
            Pattern.compile("`metric`\\s+varchar\\((\\d+)\\)", Pattern.CASE_INSENSITIVE);

    private static final Pattern QUOTED = Pattern.compile("'([^']*)'");

    /**
     * SQL 주석입니다. 지우고 나서 매칭합니다.
     *
     * <p>이 저장소의 마이그레이션은 근거를 주석으로 길게 답니다. "예전에는 {@code IN ('A','B')}
     * 였다" 같은 설명이 실제 문장 옆에 붙으면 파서가 주석을 정의로 읽습니다.</p>
     *
     * <p><b>반대 방향 실패</b> — 문자열 리터럴 안에 {@code --} 나 {@code /*} 가 있으면 진짜
     * SQL 을 지웁니다. 이 테이블이 담는 값은 enum 이름뿐이라 지금은 그런 리터럴이 없고
     * (전 마이그레이션 확인), 생기면 값 집합이 어긋나 이 테스트가 빨개집니다.</p>
     */
    private static final Pattern SQL_COMMENT = Pattern.compile("--[^\\n]*|/\\*.*?\\*/", Pattern.DOTALL);

    /** Flyway 버전을 숫자로 읽습니다. {@code V3} 는 {@code V2026082003} 보다 앞입니다. */
    private static final Pattern VERSION = Pattern.compile("^V(\\d+)__");

    @Test
    @DisplayName("SeriesKey · Metric · CHECK 제약이 같은 값 집합을 본다")
    void seriesKeyMetricAndCheckConstraintAgree() throws IOException {
        Set<String> allowedByDatabase = allowedMetricValuesInMigrations();
        Set<String> archivedMetrics = Arrays.stream(Metric.values())
                .map(Enum::name)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        assertThat(allowedByDatabase)
                .as("enum 만 늘리고 마이그레이션을 빼면 실제 회차 archive 에서 처음 터집니다 —"
                        + " 그때는 회차가 끝나 있어 재수집이 안 됩니다")
                .containsExactlyInAnyOrderElementsOf(archivedMetrics);

        // 화면 계약과 archive 계약이 겹치는 부분은 지연 축이다. 나머지 계열은 원천이 달라
        // 한쪽에만 있는 것이 정상이라(THROUGHPUT 은 적재하지 않고 DB_POOL_USAGE 는 계열이
        // 아니다) 전체를 같다고 보면 이 테스트는 아무 때나 빨개진다.
        assertThat(latencyAxes(archivedMetrics))
                .as("지연 축은 화면과 archive 가 같은 이름을 써야 회차 간 비교가 이어집니다")
                .containsExactlyInAnyOrderElementsOf(latencyAxes(
                        Arrays.stream(SeriesKey.values()).map(Enum::name).toList()));
    }

    /**
     * enum 이름이 컬럼에 <b>들어가는지</b> 봅니다.
     *
     * <p>값 집합이 맞아도 이름이 길면 STRICT 모드가 INSERT 를 거절합니다 — CHECK 위반과 똑같이
     * 실제 회차 archive 에서 처음 터지고, 그때는 회차가 끝나 있습니다. 폭을 여기 옮겨 적지 않고
     * 마이그레이션에서 읽습니다.</p>
     */
    @Test
    @DisplayName("모든 Metric 이름이 metric 컬럼 폭에 들어간다")
    void everyMetricNameFitsTheColumn() throws IOException {
        int width = metricColumnWidthInMigrations();
        for (Metric metric : Metric.values()) {
            assertThat(metric.name().length())
                    .as("%s 가 varchar(%d) 를 넘습니다 — 적재 시점에 거절됩니다", metric, width)
                    .isLessThanOrEqualTo(width);
        }
    }

    /**
     * archive 질의가 축마다 <b>서로 다른지</b> 봅니다.
     *
     * <p>switch 망라성은 case 가 있는지만 봅니다 — 다른 축의 질의를 복사해 붙이면 컴파일도 되고
     * 기존 테스트도 통과합니다. 그러면 새 이름으로 <b>옛 축의 값</b>이 적재되는데,
     * {@code run_timeseries} 는 완료 회차 불변이라 소급 정정이 안 됩니다.</p>
     */
    @Test
    @DisplayName("archive 질의는 축마다 다르고 비어 있지 않다")
    void archiveQueriesAreDistinctAndNonBlank() {
        Set<String> queries = new LinkedHashSet<>();
        for (Metric metric : Metric.values()) {
            String promQl = PromQueryClient.rangeQueryFor(metric);
            assertThat(promQl).as("%s 의 archive 질의가 비었습니다", metric).isNotBlank();
            assertThat(queries.add(promQl))
                    .as("%s 가 다른 축의 질의를 그대로 씁니다 — 새 이름으로 옛 값이 적재됩니다", metric)
                    .isTrue();
        }
    }

    @Test
    @DisplayName("성공 축은 이름이 바뀌지 않는다")
    void successAxisKeepsItsName() {
        // 개명하면 과거 archive 행을 UPDATE 해야 하고, 그 순간 회차 비교 축이 갈린다.
        // 이 데이터의 존재 이유가 회차 비교라 그것이 깨지면 남는 값이 없다.
        assertThat(Arrays.stream(Metric.values()).map(Enum::name))
                .contains("LATENCY_P99");
        assertThat(Arrays.stream(SeriesKey.values()).map(Enum::name))
                .contains("LATENCY_P99");
    }

    private static Set<String> latencyAxes(Iterable<String> names) {
        Set<String> axes = new LinkedHashSet<>();
        for (String name : names) {
            if (name.startsWith("LATENCY_")) {
                axes.add(name);
            }
        }
        return axes;
    }

    /**
     * 마이그레이션을 Flyway 순서로 훑어 <b>마지막</b> 제약 정의를 읽습니다.
     *
     * <p>MySQL 은 CHECK 를 자리에서 못 고쳐 DROP 후 재생성합니다 — 첫 정의만 보면 축을 더한
     * 마이그레이션을 통째로 못 봅니다.</p>
     */
    private static Set<String> allowedMetricValuesInMigrations() throws IOException {
        Set<String> allowed = null;
        for (Resource migration : orderedMigrations()) {
            String sql = sqlWithoutComments(migration);
            // DROP 과 ADD 를 <나온 순서대로> 본다. 파일 단위로 나눠 보면 같은 파일 안의
            // 순서를 잃고, 떨구기만 한 마이그레이션 뒤에도 앞 파일의 정의로 폴백한다 —
            // 그러면 DB 에는 제약이 없는데 이 테스트만 초록이 된다.
            Matcher matcher = CHECK_DROP_OR_DEFINITION.matcher(sql);
            while (matcher.find()) {
                allowed = matcher.group(1) == null ? null : quotedValues(matcher.group(1));
            }
        }
        assertThat(allowed)
                .as("ck_timeseries_metric 이 떨궈진 뒤 되살아나지 않았거나 정의를 찾지 못했습니다")
                .isNotNull();
        return allowed;
    }

    /** 마이그레이션을 Flyway 순서로 훑어 {@code metric} 컬럼의 <b>마지막</b> 폭을 읽습니다. */
    private static int metricColumnWidthInMigrations() throws IOException {
        Integer width = null;
        for (Resource migration : orderedMigrations()) {
            Matcher matcher = METRIC_COLUMN_WIDTH.matcher(sqlWithoutComments(migration));
            while (matcher.find()) {
                width = Integer.parseInt(matcher.group(1));
            }
        }
        assertThat(width).as("metric 컬럼 정의를 찾지 못했습니다").isNotNull();
        return width;
    }

    /** 주석을 지운 마이그레이션 본문을 돌려줍니다. */
    private static String sqlWithoutComments(Resource migration) throws IOException {
        String sql = new String(migration.getContentAsByteArray(), StandardCharsets.UTF_8);
        return SQL_COMMENT.matcher(sql).replaceAll("");
    }

    /** classpath 의 마이그레이션을 Flyway 버전 순으로 돌려줍니다. */
    private static List<Resource> orderedMigrations() throws IOException {
        Resource[] migrations = new PathMatchingResourcePatternResolver()
                .getResources("classpath*:db/migration/*.sql");
        assertThat(migrations).as("마이그레이션이 테스트 classpath 에 없습니다").isNotEmpty();
        List<Resource> ordered = new ArrayList<>(Arrays.asList(migrations));
        ordered.sort(Comparator.comparing(RunTimeseriesMetricContractTest::versionOf));
        return ordered;
    }

    private static Set<String> quotedValues(String inClause) {
        Set<String> values = new LinkedHashSet<>();
        Matcher matcher = QUOTED.matcher(inClause);
        while (matcher.find()) {
            values.add(matcher.group(1));
        }
        return values;
    }

    private static BigInteger versionOf(Resource migration) {
        String name = migration.getFilename() == null ? "" : migration.getFilename();
        Matcher matcher = VERSION.matcher(name);
        assertThat(matcher.find()).as("Flyway 버전을 읽을 수 없는 파일입니다: %s", name).isTrue();
        return new BigInteger(matcher.group(1));
    }
}
