// stuckBefore 바인딩이 StuckRunClaim 밖으로 새지 않는지 소스에서 확인합니다.
package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * <b>"콜사이트에서 축을 빠뜨리는 것이 불가능하다" 를 주석이 아니라 검사로 만든다.</b>
 * {@link StuckRunClaim#claim} 이 바인딩을 감싸도 <b>그것을 안 쓰면 그만</b>이다 — SQL 상수가
 * 패키지 공개라 같은 패키지의 서비스가 {@code jdbcClient.sql(CLAIM)} 으로 직접 실행하고
 * 원시 {@code LocalDateTime} 을 넣을 수 있다. 실제로 CY-718 이전이 그 모양이었고, 그때
 * 세 콜사이트 중 하나만 고쳐도 축 테스트는 초록으로 남았다.
 *
 * <p>그래서 <b>소스에서 직접 센다.</b> {@code stuckBefore} 를 바인딩하는 자리는 이 저장소에
 * 정확히 <b>한 곳</b>이어야 하고, 그 한 번은 {@code Timestamp.valueOf} 를 지나야 한다.
 *
 * <p>⚠️ <b>정확한 리터럴 하나로 찾지 않는다.</b> 처음엔 {@code .param("stuckBefore"} 를 그대로
 * 찾았는데, 그러면 {@code .param( "stuckBefore"} 나 줄바꿈이 낀 형태처럼 <b>문법적으로 같은</b>
 * 표현을 놓친다 — 우회를 막겠다는 검사가 우회당한다(CY-718 리뷰가 잡았다). 공백을 허용하는
 * 패턴으로 든다. 아래 {@link Detection} 이 그 변형들을 직접 단언한다.
 *
 * <p><b>왜 AST 나 아키텍처 규칙이 아닌가.</b> 이 저장소에 ArchUnit 의존성이 없고, 찾는 것이
 * <b>파라미터 이름 하나</b>라 토큰 패턴으로 충분하다. 오탐도 실측했다 — 두 모듈 main 소스에서
 * 이 패턴에 걸리는 자리는 {@code StuckRunClaim} 의 진짜 바인딩 <b>한 줄뿐</b>이고, 주석은
 * 전부 {@code {@code stuckBefore}} 꼴이라 안 걸린다.
 *
 * <p>형제 선례는 {@code NoWallClockInBatchTest} · {@code NoOrphanJavadocTest} 다 — 둘 다
 * 같은 방식으로 "코드가 지켜야 할 모양" 을 소스 스캔으로 든다.
 */
class StuckBeforeBindingIsCentralizedTest {

    /** 바인딩이 허용된 유일한 파일. */
    private static final String OWNER = "StuckRunClaim.java";

    /** {@code .param("stuckBefore"} — 괄호·따옴표 사이 공백과 줄바꿈을 허용한다. */
    static final Pattern BINDING =
            Pattern.compile("\\.param\\s*\\(\\s*\"stuckBefore\"");

    /** 그 바인딩이 축 변환을 지나는 형태. */
    static final Pattern ON_DB_AXIS = Pattern.compile(
            "\\.param\\s*\\(\\s*\"stuckBefore\"\\s*,\\s*Timestamp\\s*\\.\\s*valueOf\\s*\\(");

    @Test
    @DisplayName("stuckBefore 를 바인딩하는 곳은 StuckRunClaim 하나뿐이다")
    void onlyStuckRunClaimBindsIt() throws IOException {
        List<Path> offenders = sources()
                .filter(path -> !path.getFileName().toString().equals(OWNER))
                .filter(path -> BINDING.matcher(read(path)).find())
                .toList();

        assertThat(offenders)
                .as("""
                        stuckBefore 를 직접 바인딩하면 원시 LocalDateTime 이 들어갈 수 있고, \
                        그 값은 드라이버의 존 정규화를 안 타서 배치 메타 컬럼과 다른 축에 선다. \
                        StuckRunClaim.claim(...) 을 쓰십시오 — 거기 한 곳만 Timestamp.valueOf 를 \
                        건다. 근거는 TimestampBindingAxisTest 와 DefaultZoneGuard 의 javadoc.""")
                .isEmpty();
    }

    /**
     * <b>주인 파일 자체도 센다.</b> 이것이 없으면 {@code StuckRunClaim} 안에 원시 바인딩을
     * 하나 더 넣어도 위 검사가 통과한다 — 파일 이름만 보기 때문이다.
     */
    @Test
    @DisplayName("StuckRunClaim 안에서도 바인딩은 한 번뿐이고 축 변환을 지난다")
    void theOwnerBindsExactlyOnceThroughTheConversion() throws IOException {
        String owner = read(sources()
                .filter(path -> path.getFileName().toString().equals(OWNER))
                .findFirst()
                .orElseThrow(() -> new AssertionError("StuckRunClaim.java 를 못 찾았습니다")));

        assertThat(count(BINDING, owner))
                .as("바인딩이 늘면 그중 하나가 원시로 남을 수 있다")
                .isEqualTo(1);
        assertThat(count(ON_DB_AXIS, owner))
                .as("그 한 번은 반드시 Timestamp.valueOf 를 지나야 컬럼과 같은 축이다")
                .isEqualTo(1);
    }

    /**
     * <b>탐지기 자신을 잰다.</b> 소스 스캔 검사는 <b>아무것도 못 찾아도 초록</b>이라, 패턴이
     * 망가진 것과 위반이 없는 것이 구분되지 않는다. 그래서 <b>잡아야 할 변형</b>과
     * <b>잡으면 안 되는 것</b>을 표본으로 박아 둔다.
     */
    @Nested
    @DisplayName("탐지 패턴")
    class Detection {

        @Test
        @DisplayName("공백·줄바꿈이 낀 우회를 잡는다")
        void catchesWhitespaceVariants() {
            assertThat(BINDING.matcher(".param(\"stuckBefore\", stuckBefore)").find()).isTrue();
            assertThat(BINDING.matcher(".param( \"stuckBefore\", stuckBefore)").find()).isTrue();
            assertThat(BINDING.matcher(".param (\"stuckBefore\", stuckBefore)").find()).isTrue();
            assertThat(BINDING.matcher(".param(\n        \"stuckBefore\", x)").find()).isTrue();
            assertThat(BINDING.matcher("\n    .param(\"stuckBefore\",\n        stuckBefore)").find())
                    .isTrue();
        }

        @Test
        @DisplayName("축 변환을 지나는 형태만 안전으로 센다")
        void tellsTheSafeFormApart() {
            assertThat(ON_DB_AXIS.matcher(".param(\"stuckBefore\", Timestamp.valueOf(x))").find())
                    .isTrue();
            assertThat(ON_DB_AXIS.matcher(".param(\"stuckBefore\",\n    Timestamp.valueOf(x))")
                    .find()).isTrue();
            assertThat(ON_DB_AXIS.matcher(".param(\"stuckBefore\", stuckBefore)").find())
                    .as("원시 바인딩을 안전으로 세면 검사가 무의미해진다")
                    .isFalse();
        }

        @Test
        @DisplayName("다른 파라미터나 이름만 언급한 문장은 안 잡는다")
        void doesNotFireOnMentions() {
            assertThat(BINDING.matcher(".param(\"stuckBeforeX\", v)").find()).isFalse();
            assertThat(BINDING.matcher("// stuckBefore 는 UTC 축이어야 한다").find()).isFalse();
            assertThat(BINDING.matcher(" * {@code stuckBefore} 를 바인딩한다").find()).isFalse();
            assertThat(BINDING.matcher(".param(\"id\", executionId)").find()).isFalse();
        }
    }

    private static int count(Pattern pattern, String source) {
        return (int) pattern.matcher(source).results().count();
    }

    private static String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new IllegalStateException("소스를 못 읽었습니다: " + path, e);
        }
    }

    /** 배치·저장소 두 모듈의 main 소스를 본다 — SQL 은 양쪽에 흩어져 있다. */
    private static Stream<Path> sources() throws IOException {
        Path module = Path.of("").toAbsolutePath();
        Path root = module.getFileName().toString().equals("batch")
                ? module.getParent() : module;
        return Stream.of(root.resolve("batch/src/main/java"), root.resolve("storage/src/main/java"))
                .flatMap(dir -> {
                    try {
                        return Files.walk(dir);
                    } catch (IOException e) {
                        throw new IllegalStateException("소스를 못 걸었습니다: " + dir, e);
                    }
                })
                .filter(path -> path.toString().endsWith(".java"));
    }
}
