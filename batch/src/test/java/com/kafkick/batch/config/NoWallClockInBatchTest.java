// 배치 본문에 벽시계 호출이 새로 생기지 않는지 소스를 직접 훑습니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
