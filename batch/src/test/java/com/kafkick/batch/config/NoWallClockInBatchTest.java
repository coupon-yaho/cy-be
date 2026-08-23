// 배치 본문에 벽시계 호출이 새로 생기지 않는지 소스를 직접 훑습니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>{@code docs/04-review-checklist.md} 가 배치 코드의 {@code now()} 를 금지한다.</b>
 * 판정이 현재 시각에 기대는 순간 같은 {@code asOf} 두 실행이 다른 답을 낼 수 있고,
 * 그것이 이 저장소가 증명해야 하는 결정론을 정면으로 깬다.
 *
 * <p><b>예외가 하나 생겼다.</b> {@link RunningJobProbe} 는 배치 메타에 찍힌 시각과 나이를
 * 비교하는데, 그 시각을 찍는 것이 인자 없는 {@code LocalDateTime.now()}
 * ({@code AbstractJob} 과 {@code SimpleJobRepository}, 6.0.4 바이트코드로 확인)라
 * {@code TimeProvider}(UTC)로 비교하면 KST 기기에서 아홉 시간 어긋나 <b>가드가 통째로 꺼진다.</b>
 *
 * <p><b>그래서 예외를 목록으로 못 박는다.</b> 안 그러면 다음 사람이 규칙 Step 안에서
 * {@code now()} 를 부르며 <i>"프로브도 쓴다"</i> 를 근거로 삼는다 — 예외가 선례가 되는
 * 자리를 여기서 끊는다. 늘리려면 이 목록을 고쳐야 하고, 그때 리뷰가 걸린다.
 *
 * <p><b>SQL 안의 {@code NOW()} 는 안 본다.</b> 패턴이 자바 호출만 잡는다.
 * {@code BatchRunMetricsRefresher} 의 조회 창이 {@code DATE_SUB(NOW(), INTERVAL 7 DAY)} 를
 * 쓰는데, 그것은 <b>판정이 아니라 조회 범위</b>라 규칙 밖이다 — 판정에 쓰이면 같은 금지가
 * 적용되어야 하고, 그때는 이 테스트가 못 잡는다는 것을 알고 있어야 한다.
 *
 * <p><b>이 모듈만 훑는다.</b> 검증 규칙 SQL 이 사는 {@code storage} 와 도메인이 사는
 * {@code core} 는 범위 밖이다 — 지금 그쪽에 위반이 없는 것은 확인했지만, 이 테스트가
 * 그것을 지켜 주지는 않는다. {@code core} 는 {@code TimeProvider} 정의부가 정당하게
 * {@code Clock.systemUTC()} 를 쓰므로 같은 규칙을 그대로 옮길 수도 없다.
 */
class NoWallClockInBatchTest {

    private static final Path SOURCE_ROOT = Path.of("src/main/java");

    /**
     * <b>좁게 잡으면 우회로가 규칙보다 넓어진다.</b> 처음에는 {@code LocalDateTime.now()} 류
     * 넷만 봤는데, 그러면 {@code ZonedDateTime.now()} 한 줄로 통과한다 — 리포트 타임존을
     * 맞추려는 자연스러운 동기에서 나오는 바로 그 호출이다.
     */
    private static final Pattern WALL_CLOCK = Pattern.compile(
            "\\b(LocalDateTime|LocalDate|LocalTime|Instant|ZonedDateTime|OffsetDateTime"
                    + "|OffsetTime|Year|YearMonth)\\s*\\.\\s*now\\s*\\("
                    + "|System\\s*\\.\\s*(currentTimeMillis|nanoTime)\\s*\\("
                    + "|Clock\\s*\\.\\s*(systemUTC|systemDefaultZone)\\s*\\("
                    + "|new\\s+Date\\s*\\(\\s*\\)"
                    + "|Calendar\\s*\\.\\s*getInstance\\s*\\(");

    /**
     * <b>파일명이 아니라 상대 경로로 못 박는다.</b> 이름만 비교하면 다른 패키지에 같은 이름의
     * 파일이 생기는 순간 조용히 면제된다. 늘릴 때는 왜인지 클래스 주석에 함께 적는다.
     */
    private static final Set<String> ALLOWED =
            Set.of("com/kafkick/batch/config/RunningJobProbe.java");

    /**
     * <b>주입된 시계도 예산을 갖는다.</b> {@code timeProvider.now()} 는 위 정규식에 안 걸리고,
     * 걸려서도 안 된다 — 그것이 이 규칙이 안내하는 대체 수단이다. 그런데 CY-397 이
     * {@code verifyJob} 의 Step 둘에 {@code TimeProvider} 를 주입하면서, 규칙 결과·checksum·
     * verdict 를 다루는 코드와 <b>같은 스코프</b>에 시계가 놓였다.
     *
     * <p><b>배치 전체를 훑고 파일마다 예산을 못 박는다.</b> 한 파일만 세면 두 가지를 놓친다 —
     * 판정 경로의 <b>다른</b> 클래스({@code rule/**} · {@code replay/**})에 시계를 주입하는 것과,
     * 예산이 있는 파일에서 <b>자리만 옮기는</b> 것이다. 앞엣것은 목록에 없는 파일을 실패로
     * 만들어 잡고, 뒤엣것은 파일별 수를 정확히 단언해 좁힌다.
     *
     * <p>세는 방식이 정규식이라 <b>주석 안의 같은 형태도 함께 센다.</b> 그래서 본문 주석은
     * 그 자리를 {@code TimeProvider#now} 로 적는다 — 값싼 검사의 대가다.
     *
     * <p>{@code verifyJob} 의 둘은 {@code started_at}·{@code finished_at} 을 찍는 자리이고
     * 판정에 안 들어간다. 그 컬럼들은 {@code as_of}(UTC)와 좌표계를 맞춰야 해서 도메인
     * 시계를 쓴다 — 배치 메타의 {@code START_TIME} 은 JVM 기본 존이다.
     */
    @Test
    @DisplayName("주입된 시계를 부르는 파일과 횟수가 예산과 정확히 같다")
    void injectedClockStaysInsideItsBudget() throws IOException {
        try (Stream<Path> sources = Files.walk(SOURCE_ROOT)) {
            List<String> offenders = new ArrayList<>();
            for (Path path : sources.filter(f -> f.toString().endsWith(".java")).toList()) {
                String relative = SOURCE_ROOT.relativize(path).toString();
                long calls = INJECTED_CLOCK
                        .matcher(Files.readString(path, StandardCharsets.UTF_8))
                        .results().count();
                int budget = INJECTED_CLOCK_BUDGET.getOrDefault(relative, 0);
                if (calls != budget) {
                    offenders.add(relative + " 호출=" + calls + " 예산=" + budget);
                }
            }

            assertThat(offenders)
                    .as("검증 규칙·판정에 현재 시각을 쓰면 같은 asOf 재실행이 다른 답을 낸다. "
                            + "주입 시계는 위 정규식이 못 잡으므로 여기서 파일별로 센다. "
                            + "늘려야 하면 예산을 고치고 **왜인지 함께 적어라** — "
                            + "주석에 같은 형태를 쓰면 그것도 세므로 TimeProvider#now 로 적는다")
                    .isEmpty();
        }
    }

    /** 주입된 시계 호출. JDK 직접 호출과 달리 금지가 아니라 <b>파일별 예산</b>이다. */
    private static final Pattern INJECTED_CLOCK =
            Pattern.compile("\\btimeProvider\\s*\\.\\s*now\\s*\\(");

    /**
     * 예산에 없는 파일에서 한 건이라도 나오면 실패다. 늘릴 때는 <b>그 시각이 판정에 안
     * 들어간다는 근거</b>를 그 자리에 함께 적는다.
     *
     * <pre>
     * VerifyJobConfig      2  started_at · finished_at — as_of 와 좌표계를 맞춘다
     * ExpireJobConfig      2  asOf 가드와 커밋 시각 — 판정이 아니라 만료 대상 선정이다
     * CleanupJobConfig     1  버려진 실행 컷오프 — 정리는 판정을 안 낸다
     * ExpireScheduler      2  크론 슬롯 계산과 기동 가드 — 잡 밖이다
     * CleanupScheduler     2  같은 축
     * VerifyTriggerController 1  asOf 미래 검사 — 접수 단계다
     * BatchApiExceptionHandler 1  응답 timestamp
     * </pre>
     */
    private static final Map<String, Integer> INJECTED_CLOCK_BUDGET = Map.of(
            "com/kafkick/batch/job/VerifyJobConfig.java", 2,
            "com/kafkick/batch/job/ExpireJobConfig.java", 2,
            "com/kafkick/batch/job/CleanupJobConfig.java", 1,
            "com/kafkick/batch/schedule/ExpireScheduler.java", 2,
            "com/kafkick/batch/schedule/CleanupScheduler.java", 2,
            "com/kafkick/batch/api/VerifyTriggerController.java", 1,
            "com/kafkick/batch/api/BatchApiExceptionHandler.java", 1);

    @Test
    @DisplayName("배치 본문의 벽시계 호출은 허용 목록 안에만 있다")
    void wallClockCallsStayInTheAllowList() throws IOException {
        try (Stream<Path> sources = Files.walk(SOURCE_ROOT)) {
            List<String> offenders = sources
                    .filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !ALLOWED.contains(
                            SOURCE_ROOT.relativize(path).toString().replace('\\', '/')))
                    .filter(NoWallClockInBatchTest::callsWallClock)
                    .map(SOURCE_ROOT::relativize)
                    .map(Path::toString)
                    .sorted()
                    .toList();

            assertThat(offenders)
                    .as("판정이 현재 시각에 기대면 같은 asOf 두 실행이 다른 답을 낼 수 있다. "
                            + "시각이 필요하면 TimeProvider 를 주입받아라 — 정말 배치 메타의 "
                            + "좌표계가 필요한 경우에만 ALLOWED 에 더하고, 왜인지 함께 적어라")
                    .isEmpty();
        }
    }

    private static boolean callsWallClock(Path source) {
        try {
            // 주석 안의 언급까지 세면 설명을 못 쓴다. 줄 앞의 // 와 * 를 걷어내고 본다 —
            // 블록 주석 중간 줄이 * 로 시작하는 것을 이용한다.
            return Files.readAllLines(source).stream()
                    .map(String::strip)
                    .filter(line -> !line.startsWith("//") && !line.startsWith("*")
                            && !line.startsWith("/*"))
                    .anyMatch(line -> WALL_CLOCK.matcher(line).find());
        } catch (IOException e) {
            throw new IllegalStateException("소스를 못 읽었다: " + source, e);
        }
    }
}
