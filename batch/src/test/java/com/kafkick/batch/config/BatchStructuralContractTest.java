// 소스를 읽어야만 지킬 수 있는 구조 계약들을 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>{@code batch.config → batch.job} 화살표를 막는다.</b>
 *
 * <p>잡 설정 셋({@code Expire}·{@code Verify}·{@code Cleanup})은 전부 {@code config} 를
 * 가져다 쓴다. 반대 방향이 하나라도 생기면 두 패키지가 <b>순환</b>이 되고, 자바는 그것을
 * 안 막으므로 컴파일도 테스트도 통과한다 — <b>다음 티켓이 "이미 그렇게 하고 있다" 를
 * 근거로 삼을 때까지 아무도 모른다.</b>
 *
 * <p><b>{@code schedule} 도 함께 막는다.</b> {@code batch.job} 과 {@code batch.schedule} 은
 * <b>이미 서로를 가져간다</b>({@code ExpireScheduler → VerifyJobConfig},
 * {@code ExpireJobConfig → CronSlot}) — 그 순환은 이 티켓보다 앞선 것이라 여기서 안
 * 건드린다. 다만 {@code config → schedule} 을 열어 두면 그 기존 순환을 타고
 * {@code config → schedule → job} 으로 <b>이 자물쇠를 통째로 우회</b>할 수 있다.
 *
 * <p>CY-421 이 실제로 그 화살표를 하나 만들었다 — 지표 되읽기가 잡 설정의 {@code JOB_NAME}
 * 과 {@code blockedFrom} 을 직접 가져왔다. 리뷰가 잡았고, 계약을 {@link ExpireStepContext}
 * 로 내려 방향을 되돌렸다. <b>이 테스트는 그것이 다시 돌아오지 않게 하는 자물쇠다.</b>
 *
 * <p><b>왜 소스를 읽나.</b> 바이트코드에는 상수 인라이닝 때문에 {@code JOB_NAME} 참조가
 * 안 남는다 — {@code String} 상수는 컴파일 타임에 값으로 박힌다. 그래서 {@code import}
 * 문을 본다. 정규화된 이름을 본문에 직접 쓰는 우회도 함께 잡는다.
 *
 * <p>{@code docs/13} 이 이 티켓의 리뷰에서 <i>"같은 종류의 결함이 반복해서 나온다"</i> 며
 * <b>기계적 검사</b>를 후속으로 적어 뒀다. 이 클래스가 그 첫 조각이다.
 */
class BatchStructuralContractTest {

    private static final Path CONFIG = Path.of("src/main/java/com/kafkick/batch/config");

    /** {@code job} 은 직접, {@code schedule} 은 우회로라서 막는다. */
    private static final List<String> FORBIDDEN =
            List.of("com.kafkick.batch.job.", "com.kafkick.batch.schedule.");

    @Test
    @DisplayName("batch.config 는 batch.job 도 batch.schedule 도 참조하지 않는다")
    void configDoesNotDependOnJob() throws IOException {
        try (Stream<Path> files = Files.walk(CONFIG)) {
            List<String> offenders = files
                    .filter(path -> path.toString().endsWith(".java"))
                    .flatMap(path -> stripComments(read(path)).lines()
                            .map(String::strip)
                            .filter(line -> FORBIDDEN.stream().anyMatch(line::contains))
                            .map(line -> path.getFileName() + ": " + line))
                    .toList();

            assertThat(offenders)
                    .as("계약이 필요하면 ExpireStepContext 처럼 config 쪽에 둔다 — "
                            + "잡 설정이나 스케줄러에서 끌어오면 순환이 된다")
                    .isEmpty();
        }
    }

    /**
     * <b>만료 대기 게이지의 기록자는 하나다.</b> {@code ExpirePendingRefresher} 뿐이고,
     * 잡도 스케줄러도 안 건드린다.
     *
     * <p><b>기록자가 둘이 되면 순서 규칙이 다시 필요해진다.</b> CY-421 이 그 규칙을 지운
     * 근거가 <i>"쓰는 곳이 하나라 마지막 기록이 곧 진실"</i> 이다. 잡이나 스케줄러가
     * {@code markUnknown()} 을 하나 되돌려 넣는 순간 그 전제가 깨지는데, <b>깨져도 전부
     * 초록이다</b> — 실제로 그 상태를 한 번 지나왔다(스케줄러가 슬롯을 건너뛸 때
     * {@code markUnknown(asOf)} 를 불렀고, 그 계약이 문서와 어긋나 있었다).
     */
    @Test
    @DisplayName("ExpireMetrics 를 쓰는 프로덕션 코드는 되읽기 하나뿐이다")
    void expireMetricsHasOneWriter() throws IOException {
        Path main = Path.of("src/main/java/com/kafkick/batch");
        try (Stream<Path> files = Files.walk(main)) {
            List<String> writers = files
                    .filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !path.getFileName().toString().equals("ExpireMetrics.java"))
                    // **타입 참조와 호출을 함께 본다.** 타입 이름만 세면 javadoc 문장을
                    // 기록자로 오탐하고, 호출만 세면 record(...) 라는 이름을 공유하는 형제
                    // (BatchRunMetricsRefresher)까지 걸린다 — 실제로 걸려서 이렇게 바꿨다.
                    .filter(path -> {
                        String body = read(path);
                        return TYPE_REF.matcher(body).find() && WRITE_CALL.matcher(body).find();
                    })
                    .map(path -> path.getFileName().toString())
                    .sorted()
                    .toList();

            assertThat(writers)
                    .as("기록자가 늘면 CY-421 이 지운 순서 규칙이 다시 필요해진다 — "
                            + "그 상태는 게이지를 조용히 얼린다")
                    .containsExactly("ExpirePendingRefresher.java");
        }
    }

    private static final Pattern TYPE_REF = Pattern.compile("\\bExpireMetrics\\b");

    private static final Pattern WRITE_CALL =
            Pattern.compile("\\.(record|markUnknown|recordSchema)\\s*\\(");

    /**
     * <b>주석 제거가 문자열을 안 건드리는지 잰다.</b> 앞선 정규식 판이 정확히 여기서 깨졌고,
     * 그 실패 모드가 <b>오탐이 아니라 누락</b>이라 자물쇠가 조용히 열린다.
     */
    @Test
    @DisplayName("주석만 지우고 문자열 안의 마커는 주석으로 안 읽는다")
    void stripsCommentsWithoutEatingStringLiterals() {
        String pkg = "com.kafkick.batch.job.Real";

        assertThat(stripComments("String s = \"//\"; " + pkg + " x;"))
                .as("문자열 안의 // 가 뒤를 삼키면 진짜 의존을 놓친다 — "
                        + "SchemaPresenceGuard 의 jdbc:mysql:// 가 이미 그 모양이다")
                .contains(pkg);

        assertThat(stripComments("String a = \"/*\";\n" + pkg + " x;\nString b = \"*/\";"))
                .as("문자열 둘 사이가 블록 주석으로 읽히면 그 사이가 통째로 사라진다")
                .contains(pkg);

        assertThat(stripComments("char c = '/'; " + pkg + " x;"))
                .as("문자 리터럴도 같다")
                .contains(pkg);

        assertThat(stripComments("String t = \"\"\"\n/*\n\"\"\";\n" + pkg + " x;"))
                .as("텍스트 블록 안의 마커도 주석이 아니다")
                .contains(pkg);

        assertThat(stripComments("// " + pkg + "\nString s = \"ok\";"))
                .as("진짜 줄 주석은 지워야 한다")
                .doesNotContain(pkg);

        assertThat(stripComments("int x = 1; // " + pkg + " 참고"))
                .as("꼬리 주석도 지워야 한다")
                .doesNotContain(pkg);

        assertThat(stripComments("/* " + pkg + " */\nint x = 1;"))
                .as("진짜 블록 주석도 지워야 한다")
                .doesNotContain(pkg);
    }

    /**
     * <b>주석만 걷어내고 문자열은 그대로 둔다.</b>
     *
     * <p>정규식으로 {@code //...} 와 {@code /*...*}{@code /} 를 지우는 것으로는 안 된다 —
     * <b>문자열 안의 마커까지 주석 시작으로 읽는다.</b> 이 저장소에 이미 그런 리터럴이 있다:
     * {@code SchemaPresenceGuard} 의 {@code "jdbc:mysql://host:3306/<db>"}. 그 뒤에 참조가
     * 오면 통째로 삼켜져 <b>순환이 조용히 통과한다.</b> {@code "/*"} 와 {@code "*}{@code /"}
     * 가 서로 다른 줄에 있으면 그 사이가 전부 사라진다.
     *
     * <p>그래서 한 글자씩 훑으며 <b>문자열·문자·텍스트 블록 안에서는 마커를 안 본다.</b>
     * 지워진 주석 자리에 공백을 남겨 줄 번호와 토큰 경계를 보존한다.
     *
     * <p><b>문자열 내용은 남긴다.</b> 정규화된 이름이 문자열 안에 있으면 그것은 대개
     * {@code Class.forName} 같은 진짜 의존이라, 지우면 이 자물쇠를 문자열로 우회할 수 있다.
     * 오탐(테스트가 빨개진다)보다 누락(순환을 놓친다)이 나쁘다.
     *
     * <p>형제 {@code NoWallClockInBatchTest} 는 줄 앞의 {@code //}·{@code *} 만 걷어내는
     * 더 성긴 방식이다. 그쪽이 세는 것은 {@code TimeProvider#now} 라 마커와 얽힐 일이
     * 없어서 아직 안 옮겼다 — 옮기려면 이 메서드를 공용으로 빼면 된다.
     */
    static String stripComments(String body) {
        StringBuilder out = new StringBuilder(body.length());
        int i = 0;
        while (i < body.length()) {
            char c = body.charAt(i);
            if (body.startsWith("\"\"\"", i)) {
                int end = body.indexOf("\"\"\"", i + 3);
                end = end < 0 ? body.length() : end + 3;
                out.append(body, i, end);
                i = end;
            } else if (c == '"' || c == '\'') {
                int j = i + 1;
                while (j < body.length() && body.charAt(j) != c) {
                    j += body.charAt(j) == '\\' ? 2 : 1;
                }
                j = Math.min(j + 1, body.length());
                out.append(body, i, j);
                i = j;
            } else if (body.startsWith("//", i)) {
                while (i < body.length() && body.charAt(i) != '\n') {
                    i++;
                }
            } else if (body.startsWith("/*", i)) {
                int end = body.indexOf("*/", i + 2);
                end = end < 0 ? body.length() : end + 2;
                // 줄 번호가 밀리지 않게 개행은 남긴다.
                body.substring(i, end).chars()
                        .filter(ch -> ch == '\n')
                        .forEach(ch -> out.append('\n'));
                i = end;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    private static String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new IllegalStateException(path.toString(), e);
        }
    }
}
