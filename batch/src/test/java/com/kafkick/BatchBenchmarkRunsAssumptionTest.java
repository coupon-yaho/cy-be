package com.kafkick;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.StreamUtils;

/**
 * 관측 대상 회차를 환경변수로 박는 것은 {@code benchmark_runs} 가 없어서 쓰던 임시 방편이었다.
 * <b>OBS-14b(CY-377)가 그 테이블을 만들었다 — 이제 이 감시는 방편이 아직 안 걷혔다는 것을 지킨다.</b>
 *
 * <p>측정 시작(benchmark start)은 batch 가 이미 떠 있는 뒤에 일어난다. 그래서 환경변수로 박으면
 * 회차마다 batch 를 재시작해야 한다. 테이블이 생겼으므로 <b>진행 중인 행</b>
 * ({@code run_status = 'RUNNING'})에서 회차를 읽는 쪽이 맞다 — 재시작도, 사람이 값을 넣는 절차도 없어진다.
 *
 * <p>{@code benchmark_runs} 행에는 회차뿐 아니라 <b>엔진 버전</b>도 들어간다. 지금은 batch 가 둘 다
 * 기동 시점 환경변수로 고정하고 있어 측정 회차와 어긋나도 알 방법이 없다 — 그래서 지금 믿고 있는
 * 값을 {@code app.observation.engine.version} 으로 내보내 사람이 대조하게 해 뒀다.
 *
 * <p><b>남은 일(별도 티켓 · batch 소유).</b> OBS-14b 는 저장소·서비스까지만 세운다 — 아래는 batch
 * 관측 경로를 고치는 작업이라 이 티켓의 경계 밖이고, {@code core/observation} 과
 * {@code batch/observation} 은 지금 다른 워크트리(CY-370)가 잡고 있다.
 * <ul>
 *   <li>{@code ConsistencyRawValueReader} 의 회차 선택을 진행 중인 run 기준으로 바꾼다.
 *   <li><b>엔진 버전도 같은 행에서 읽는다.</b> 회차 도중 런타임 토글이 바뀌어도 그 회차의 기준은
 *       시작할 때 박제된 값이라, 런타임 설정(OBS-19)보다 이쪽이 측정의 권위다.
 *   <li>{@code observation.domain-gauge.coupon-id} 는 그 조회를 무시하고 강제 지정하는
 *       수동 탈출구로만 남긴다(회차 없이 관측만 돌려 볼 때).
 *   <li>run 이 없는 동안의 동작을 정한다 — 지금처럼 "가장 최근 열린 회차" 인지, 값을 안 내는지.
 * </ul>
 *
 * <p>그 작업이 끝나면 이 클래스는 통째로 지운다. 여기 남은 파서는 "테이블이 생겼는지" 를 판정하는
 * 물건이라, 방편이 걷힌 뒤에는 지킬 것이 없다.
 */
class BatchBenchmarkRunsAssumptionTest {

    private static final Pattern CREATE_BENCHMARK_RUNS =
            Pattern.compile("(?i)create\\s+table\\s+(?:if\\s+not\\s+exists\\s+)?"
                    + "(?:[`\"]?\\w+[`\"]?\\s*\\.\\s*)?"      // 스키마 접두사
                    + "[`\"]?benchmark_runs[`\"]?(?![\\w$])");      // 식별자 경계 — _archive 를 안 잡는다

    @Test
    @DisplayName("테이블 생성만 판정한다 — 이름이 스쳐 지나가는 것과 구분한다")
    void onlyTableCreationCounts() {
        assertThat(createsBenchmarkRuns("CREATE TABLE `benchmark_runs` (id bigint)")).isTrue();
        assertThat(createsBenchmarkRuns("create table if not exists benchmark_runs (id bigint)")).isTrue();
        assertThat(createsBenchmarkRuns("CREATE TABLE app.benchmark_runs (id bigint)")).isTrue();

        // OBS-22 가 회차 archive 테이블을 만든다. 그건 이 가드의 대상이 아니다.
        assertThat(createsBenchmarkRuns("CREATE TABLE `benchmark_runs_archive` (id bigint)")).isFalse();
        // CY-253 이 실제로 이렇게 적었고, 그때 이 가드가 오탐했다.
        assertThat(createsBenchmarkRuns(
                "CREATE TABLE `issue_attempts` (`benchmark_run_id` bigint COMMENT 'benchmark_runs.id')"))
                .isFalse();
        assertThat(createsBenchmarkRuns("-- benchmark_runs(OBS-14b)가 가져간다")).isFalse();
        // DDL 이 문자열 안에 들어 있는 것은 생성이 아니다(감사 로그 시딩 등).
        assertThat(createsBenchmarkRuns(
                "INSERT INTO audit(sql_text) VALUES ('CREATE TABLE benchmark_runs (id bigint)')"))
                .isFalse();
        assertThat(createsBenchmarkRuns(
                "CREATE TABLE `x` (`c` text COMMENT 'CREATE TABLE benchmark_runs (id)')")).isFalse();

        // 문자열 안의 `--` 가 같은 줄 뒤의 진짜 DDL 을 삼키면 안 된다.
        assertThat(createsBenchmarkRuns(
                "INSERT INTO t VALUES ('a -- b'); CREATE TABLE benchmark_runs (id bigint);")).isTrue();
        // 반대로 주석 안의 어퍼스트로피가 뒤쪽 문자열까지 묶어 DDL 을 삼켜도 안 된다.
        assertThat(createsBenchmarkRuns(
                "-- don't do this\nCREATE TABLE `benchmark_runs` (id bigint);\nINSERT INTO t VALUES ('x');"))
                .isTrue();
        // MySQL 은 `/*!` 로 시작하는 블록을 주석이 아니라 코드로 실행한다(실측 확인).
        assertThat(createsBenchmarkRuns(
                "/*!80000 CREATE TABLE `benchmark_runs` (id bigint) */;")).isTrue();
        // 블록 주석 안의 DDL 도 생성이 아니다.
        assertThat(createsBenchmarkRuns("/* CREATE TABLE benchmark_runs (id) */")).isFalse();
        assertThat(createsBenchmarkRuns(
                "/* 나중에 */ CREATE TABLE `benchmark_runs` (id bigint);")).isTrue();
        // MySQL 은 `#` 도 줄 주석으로 인정한다.
        assertThat(createsBenchmarkRuns("# CREATE TABLE benchmark_runs (id bigint)")).isFalse();
        assertThat(createsBenchmarkRuns(
                "# 나중에 만든다\nCREATE TABLE `benchmark_runs` (id bigint);")).isTrue();
    }

    @Test
    @DisplayName("benchmark_runs 가 생겼다 — 회차 출처를 환경변수에서 진행 중인 run 으로 바꿀 차례다")
    void migrationCreatesBenchmarkRunsTable() throws Exception {
        Resource[] migrations = new PathMatchingResourcePatternResolver()
                .getResources("classpath*:db/migration/*.sql");

        assertThat(migrations).as("마이그레이션을 하나도 못 읽었다면 이 감시는 무효다").isNotEmpty();
        assertThat(Arrays.stream(migrations).filter(BatchBenchmarkRunsAssumptionTest::createsBenchmarkRuns))
                .as("benchmark_runs 를 만드는 마이그레이션이 사라졌다. 회차를 진행 중인 run 에서 읽기로 한"
                        + " 전제가 무너졌으므로, 환경변수 방편을 되살릴지부터 다시 정한다")
                .isNotEmpty();
    }

    /**
     * 테이블이 <b>생겼는지</b>를 본다. 이름이 등장하는 것만으로는 아니다 — CY-253 이
     * {@code benchmark_run_id} 컬럼을 만들며 {@code COMMENT} 안에 이 이름을 적었고, 그때 이
     * 가드가 오탐했다. 그래서 {@code CREATE TABLE} 바로 뒤의 <b>이름 자리</b>만 본다.
     */
    private static boolean createsBenchmarkRuns(Resource migration) {
        try {
            try (InputStream in = migration.getInputStream()) {
                return createsBenchmarkRuns(StreamUtils.copyToString(in, StandardCharsets.UTF_8));
            }
        } catch (Exception exception) {
            throw new IllegalStateException("마이그레이션을 읽지 못했다: " + migration, exception);
        }
    }

    /**
     * 실행되는 SQL 만 남기고 테이블 이름 자리를 본다.
     *
     * <p>이름이 <b>등장</b>하는 것과 테이블이 <b>생기는</b> 것은 다르다. 실제로 걸린 것들 —
     * CY-253 의 {@code COMMENT} 언급, OBS-22 가 만들 {@code benchmark_runs_archive},
     * 문자열에 든 DDL 텍스트, 문자열 안 {@code --} 뒤의 진짜 DDL, MySQL 의 {@code #} 주석.
     *
     * <p><b>정규식을 차례로 적용하지 않는다.</b> 주석과 문자열은 서로를 품을 수 있어 어느 쪽을 먼저
     * 지우든 반대 구멍이 남는다 — 주석을 먼저 지우면 문자열 속 {@code --} 뒤의 DDL 을 놓치고,
     * 문자열을 먼저 지우면 주석 속 어퍼스트로피({@code don't})가 뒤쪽 문자열까지 한 리터럴로 묶어
     * 그 사이의 DDL 을 삼킨다. 둘 다 실측으로 확인했다. 그래서 상태를 들고 <b>한 번만</b> 훑는다.
     */
    static boolean createsBenchmarkRuns(String sql) {
        return CREATE_BENCHMARK_RUNS.matcher(executableSql(sql)).find();
    }

    /** 문자열 리터럴과 주석을 공백으로 바꾼 SQL. */
    private static String executableSql(String sql) {
        StringBuilder executable = new StringBuilder(sql.length());
        int index = 0;
        while (index < sql.length()) {
            char current = sql.charAt(index);
            if (current == '\'') {
                index = skipStringLiteral(sql, index);
                executable.append(' ');
            } else if (sql.startsWith("--", index) || current == '#') {
                while (index < sql.length() && sql.charAt(index) != '\n') {
                    index++;
                }
                executable.append(' ');
            } else if (sql.startsWith("/*!", index)) {
                // MySQL 실행 주석. 서버가 이 안을 코드로 실행하므로 남긴다 — 버전 조건
                // (`/*!80000 ...*/`)이 맞지 않아 건너뛰는 경우까지 구분하려면 서버 버전을 알아야
                // 하는데, 한 번 더 울리는 것보다 놓치는 쪽이 나쁘다. 실측으로 확인했다.
                index += 3;
            } else if (sql.startsWith("/*", index)) {
                index += 2;
                while (index < sql.length() && !sql.startsWith("*/", index)) {
                    index++;
                }
                index = Math.min(index + 2, sql.length());
                executable.append(' ');
            } else {
                executable.append(current);
                index++;
            }
        }
        return executable.toString();
    }

    /**
     * 여는 따옴표부터 닫는 따옴표까지 건너뛴다.
     *
     * <p>{@code ''}(이스케이프된 따옴표)를 따로 처리하지 않는다. 그렇게 하면 리터럴 하나가 인접한
     * 리터럴 둘로 쪼개질 뿐 <b>건너뛰는 구간은 같아서</b> 판정이 달라지지 않는다 — 시뮬레이션으로
     * 확인했다. 어떤 테스트로도 구분되지 않는 분기를 두면 "지키는 척하는 코드" 가 된다.
     */
    private static int skipStringLiteral(String sql, int start) {
        int index = start + 1;
        while (index < sql.length()) {
            if (sql.charAt(index) == '\'') {
                return index + 1;
            }
            index++;
        }
        return index;
    }
}
