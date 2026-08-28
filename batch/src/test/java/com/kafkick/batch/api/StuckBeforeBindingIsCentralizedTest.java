// stuckBefore 바인딩이 StuckRunClaim 밖으로 새지 않는지 소스에서 확인합니다.
package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>"콜사이트에서 축을 빠뜨리는 것이 불가능하다" 를 주석이 아니라 검사로 만든다.</b>
 * {@link StuckRunClaim#claim} 이 바인딩을 감싸도 <b>그것을 안 쓰면 그만</b>이다 — SQL 상수가
 * 패키지 공개라 같은 패키지의 서비스가 {@code jdbcClient.sql(CLAIM)} 으로 직접 실행하고
 * 원시 {@code LocalDateTime} 을 넣을 수 있다. 실제로 CY-718 이전이 그 모양이었고, 그때
 * 세 콜사이트 중 하나만 고쳐도 축 테스트는 초록으로 남았다.
 *
 * <p>그래서 <b>소스에서 직접 센다.</b> {@code stuckBefore} 를 바인딩하는 자리는 이 저장소에
 * 정확히 <b>한 곳</b>이어야 한다. 리플렉션이나 아키텍처 규칙 라이브러리가 없어도 되고,
 * 우회가 <b>추가되는 순간</b> 빨개진다.
 *
 * <p>형제 선례는 {@code NoWallClockInBatchTest} · {@code NoOrphanJavadocTest} 다 — 둘 다
 * 같은 방식으로 "코드가 지켜야 할 모양" 을 소스 스캔으로 든다.
 */
class StuckBeforeBindingIsCentralizedTest {

    /** 바인딩이 허용된 유일한 파일. */
    private static final String OWNER = "StuckRunClaim.java";

    private static final String BINDING = ".param(\"stuckBefore\"";

    @Test
    @DisplayName("stuckBefore 를 바인딩하는 곳은 StuckRunClaim 하나뿐이다")
    void onlyStuckRunClaimBindsIt() throws IOException {
        List<Path> offenders = sources()
                .filter(StuckBeforeBindingIsCentralizedTest::bindsStuckBefore)
                .filter(path -> !path.getFileName().toString().equals(OWNER))
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
     * <b>주인 파일 자체는 정확히 한 번만 바인딩한다.</b> 이것이 없으면 {@code StuckRunClaim}
     * 안에 원시 바인딩을 하나 더 넣어도 위 검사가 통과한다 — 파일 이름만 보기 때문이다.
     */
    @Test
    @DisplayName("StuckRunClaim 안에서도 바인딩은 한 번뿐이고 축 변환을 지난다")
    void theOwnerBindsExactlyOnceThroughTheConversion() throws IOException {
        String owner = Files.readString(sources()
                .filter(path -> path.getFileName().toString().equals(OWNER))
                .findFirst()
                .orElseThrow(() -> new AssertionError("StuckRunClaim.java 를 못 찾았습니다")));

        assertThat(occurrences(owner, BINDING))
                .as("바인딩이 늘면 그중 하나가 원시로 남을 수 있다")
                .isEqualTo(1);
        assertThat(owner)
                .as("그 한 번은 반드시 Timestamp.valueOf 를 지나야 컬럼과 같은 축이다")
                .contains(BINDING + ", Timestamp.valueOf(stuckBefore))");
    }

    private static boolean bindsStuckBefore(Path path) {
        try {
            return Files.readString(path).contains(BINDING);
        } catch (IOException e) {
            throw new IllegalStateException("소스를 못 읽었습니다: " + path, e);
        }
    }

    private static int occurrences(String haystack, String needle) {
        int count = 0;
        for (int at = haystack.indexOf(needle); at >= 0;
                at = haystack.indexOf(needle, at + needle.length())) {
            count++;
        }
        return count;
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
