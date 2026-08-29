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
 * <p><b>예외가 하나 있다.</b> {@link RunningJobProbe} 는 배치 메타에 찍힌 시각과 나이를
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

    /** 주입된 시계 호출. JDK 직접 호출과 달리 금지가 아니라 <b>파일별 예산</b>이다. */
    private static final Pattern INJECTED_CLOCK =
            Pattern.compile("\\btimeProvider\\s*\\.\\s*now\\s*\\(");

    /**
     * 예산에 없는 파일에서 한 건이라도 나오면 실패다. 늘릴 때는 <b>그 시각이 판정에 안
     * 들어간다는 근거</b>를 그 자리에 함께 적는다.
     *
     * <pre>
     * VerifyJobConfig      0  판정 잡이다 — .coderabbit.yaml 이 주입 시계까지 금지한다
     * ExpireJobConfig      1  청크의 커밋 시각 — 판정이 아니라 만료 대상 선정이다
     *                            (CY-421 이 관측 리스너를 걷어내며 둘에서 하나가 됐다)
     * CleanupJobConfig     2  버려진 실행 컷오프 · 배치 메타 보존 컷오프
     * ExpireScheduler      2  크론 슬롯 계산과 기동 가드 — 잡 밖이다
     * CleanupScheduler     2  같은 축
     * VerifyScheduler      2  같은 축 — 크론 슬롯이 asOf 가 되지만 그 값은 <b>잡 파라미터</b>고,
     *                            판정은 여전히 잡 안에서 그 asOf 로만 한다
     * VerifyTriggerController 2  asOf 미래 검사 · 곧 뜰 만료와의 간격 — 둘 다 접수 단계다
     * BatchApiExceptionHandler 1  응답 timestamp
     * AdminTokenFilter       1  같은 축 — 401 봉투의 timestamp. 이 필터는 DispatcherServlet
     *                            앞에서 응답해 위 핸들러를 못 지나므로 봉투를 직접 만든다
     *
     * ExpirePendingRefresher 는 <b>주입 시계</b>를 안 쓴다(예산 0) — 시각을 배치 메타에서
     * 읽어 오기 때문이고, 그것이 그 클래스의 요지다. 다만 7일 창은 SQL 의 {@code NOW()} 라
     * <b>이 정규식이 못 보는 축</b>이고, MySQL 세션 존({@code --default-time-zone=+00:00})
     * 에 기대는 것은 {@code BatchRunMetricsRefresher} 와 같다.
     * </pre>
     */
    private static final Map<String, Integer> INJECTED_CLOCK_BUDGET = Map.of(
            "com/kafkick/batch/job/ExpireJobConfig.java", 1,
            // 둘이다 — 버려진 검증 컷오프(abandoned-after-hours)와
            // 배치 메타 보존 컷오프(batch.cleanup.metadata-keep-days). Step 이 갈려 있어 각자 잡는다.
            "com/kafkick/batch/job/CleanupJobConfig.java", 2,
            "com/kafkick/batch/schedule/ExpireScheduler.java", 2,
            "com/kafkick/batch/schedule/CleanupScheduler.java", 2,
            // 둘이다 — 기동 가드의 크론 최대간격 계산과, 발화 때 슬롯을 구하는 자리.
            // 뒤엣것이 asOf 가 되지만 판정은 잡 안에서 그 값으로만 하므로 축이 안 섞인다.
            "com/kafkick/batch/schedule/VerifyScheduler.java", 2,
            // 하나다 — 한 tick 이 여는 대상과 닫는 대상을 **같은 시각**으로 판정한다.
            // 두 번 읽으면 그 사이에 경계를 넘은 회차가 열리고 바로 닫힌다.
            "com/kafkick/batch/schedule/CouponRoundScheduler.java", 1,
            // 하나다 — 대기 수 넷을 한 시각으로 센다. 갈리면 서로 다른 시각의 값이 나란히 나간다.
            "com/kafkick/batch/config/CouponRoundPendingRefresher.java", 1,
            // 둘이다 — asOf 미래 검사와, 곧 뜰 만료와 겹치는지 보는 검사(CY-470).
            // 뒤엣것은 max-expire-skips=0 이 손 트리거의 방어를 없앤 자리를 접수 단계에서
            // 닫는다. 둘 다 판정에 안 들어간다 — 잡에 실리는 asOf 는 요청값 그대로다.
            "com/kafkick/batch/api/VerifyTriggerController.java", 2,
            "com/kafkick/batch/api/BatchApiExceptionHandler.java", 1,
            // 하나다 — 거절 응답 봉투의 timestamp. BatchApiExceptionHandler 와 같은 축이고
            // 같은 이유로 예산에 든다: 판정이 아니라 **응답 메타**다. 이 필터는
            // DispatcherServlet 앞에서 응답해 그 핸들러를 못 지나므로 봉투를 직접 만든다 —
            // 안 만들면 같은 API 표면에서 error 필드 집합이 갈린다(CY-742 리뷰).
            "com/kafkick/batch/config/AdminTokenFilter.java", 1);

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
     * <p><b>{@code verifyJob} 의 예산은 0 이다.</b> {@code .coderabbit.yaml} 이 판정 배치에서는
     * 주입된 시계도 같은 위반으로 못 박았고, {@code started_at}·{@code finished_at} 조차
     * Spring Batch 가 이미 기록한 시각을 쓰라고 출처를 지정한다.
     */
    @Test
    @DisplayName("주입된 시계를 부르는 파일과 횟수가 예산과 정확히 같다")
    void injectedClockStaysInsideItsBudget() throws IOException {
        try (Stream<Path> sources = Files.walk(SOURCE_ROOT)) {
            List<String> offenders = new ArrayList<>();
            for (Path path : sources.filter(f -> f.toString().endsWith(".java")).toList()) {
                String relative = SOURCE_ROOT.relativize(path).toString().replace('\\', '/');
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

    /**
     * <b>{@code ZoneId.systemDefault()} 도 환경 상태다.</b> 위 정규식은 <b>시각</b>을 읽는
     * 호출만 보는데, 존을 읽는 것도 같은 부류다 — {@code LocalDate.ofInstant(x,
     * ZoneId.systemDefault())} 로 날짜를 자르면 같은 {@code asOf} 재실행이 <b>서버 존에 따라
     * 다른 버킷</b>을 내고 {@code findings_checksum} 이 갈리는데, diff 에는 {@code now()} 가
     * 안 보인다.
     *
     * <p><b>허용 목록이 아니라 예산으로 든다.</b> 자리를 옮기는 변경도 걸려야 한다.
     */
    private static final Pattern DEFAULT_ZONE = Pattern.compile(
            "\\bZoneId\\s*\\.\\s*systemDefault\\s*\\("
                    + "|\\bTimeZone\\s*\\.\\s*getDefault\\s*\\(");

    /**
     * <pre>
     * BatchTimeAxis    1  배치 메타 시각(JVM 기본 존)을 도메인 축(UTC)으로 옮기는 자리.
     *                     판정에 안 들어간다 — started_at·finished_at 은 checksum·지문
     *                     재료가 아니고, .coderabbit.yaml 이 그 둘만 예외로 뒀다.
     * DefaultZoneGuard 1  **검사 대상 자체가 그 값이다.** 기동 때 JVM 기본 존이 고정
     *                     오프셋 0 인지 보는 가드라, 이것을 안 읽으면 할 일이 없다.
     * </pre>
     */
    private static final Map<String, Integer> DEFAULT_ZONE_BUDGET = Map.of(
            "com/kafkick/batch/config/BatchTimeAxis.java", 1,
            "com/kafkick/batch/config/DefaultZoneGuard.java", 1);

    @Test
    @DisplayName("JVM 기본 존을 읽는 파일과 횟수가 예산과 정확히 같다")
    void defaultZoneReadsStayInsideTheirBudget() throws IOException {
        try (Stream<Path> sources = Files.walk(SOURCE_ROOT)) {
            List<String> offenders = new ArrayList<>();
            for (Path path : sources.filter(f -> f.toString().endsWith(".java")).toList()) {
                String relative = SOURCE_ROOT.relativize(path).toString().replace('\\', '/');
                long reads = DEFAULT_ZONE
                        .matcher(Files.readString(path, StandardCharsets.UTF_8))
                        .results().count();
                int budget = DEFAULT_ZONE_BUDGET.getOrDefault(relative, 0);
                if (reads != budget) {
                    offenders.add(relative + " 읽기=" + reads + " 예산=" + budget);
                }
            }

            assertThat(offenders)
                    .as("JVM 기본 존으로 날짜·시(hour)를 자르면 같은 asOf 재실행이 서버 존에 "
                            + "따라 다른 답을 낸다 — 정규식이 now() 를 안 보므로 diff 에서도 "
                            + "안 보인다. 늘려야 하면 예산을 고치고 왜인지 함께 적어라")
                    .isEmpty();
        }
    }

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
