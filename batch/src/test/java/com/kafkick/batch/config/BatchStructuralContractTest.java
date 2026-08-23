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
     * <b>주석을 걷어낸다.</b> 블록 주석을 통째로 지우고 줄 주석은 끝까지 자른다 —
     * 줄 앞의 {@code //} 만 면제하면 {@code code; // com.kafkick.batch.job.X 참고} 같은
     * 꼬리 주석이 위반으로 잡혀 <b>설명을 못 쓰게 된다.</b>
     *
     * <p><b>문자열 리터럴은 안 지운다.</b> 지우자는 지적이 있었지만, 정규화된 이름이
     * 문자열 안에 있으면 그것은 대개 {@code Class.forName} 같은 <b>진짜 의존</b>이다 —
     * 지우면 이 자물쇠를 문자열로 우회할 수 있다. 오탐(테스트가 빨개진다)보다
     * 누락(순환이 조용히 들어온다)이 나쁘다.
     */
    private static String stripComments(String body) {
        return body.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("//[^\\n]*", "");
    }

    private static String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new IllegalStateException(path.toString(), e);
        }
    }
}
