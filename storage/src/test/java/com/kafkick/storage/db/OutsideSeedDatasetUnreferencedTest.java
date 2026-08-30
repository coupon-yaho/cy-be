// 대조에서 뺀 표를 검증 경로가 읽기 시작하면 이 테스트가 빨개집니다.
package com.kafkick.storage.db;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>{@link SchemaParityTestBase#OUTSIDE_SEED_DATASET} 의 근거를 코드로 붙든다.</b>
 *
 * <p>그 목록에 이름을 넣는 것은 <i>"시드가 안 만든다"</i> 를 선언하는 것이고, 그래도 되는
 * 근거는 <b>검증이 그 표를 안 읽는다</b>는 것 하나다. 안 읽으니 모양이 갈려도 판정이 안 흔들린다.
 *
 * <p>그런데 그 근거는 <b>목록에 이름을 더한 그 시점에 한 번 잰 사실</b>일 뿐이라, 나중에
 * 검증 규칙이 그 표를 참조해도 아무도 못 본다. 그러면 대조에서 빠진 채로 스키마가 갈리고,
 * <b>네 축 전부 초록인데 판정이 틀린다</b> — 이 대조가 막으려던 바로 그 상태다.
 * 그래서 그 사실을 <b>매 빌드마다 다시 잰다.</b>
 *
 * <p><b>이 검사가 빨개졌다면 둘 중 하나다.</b> ⑴ 그 표가 정말 검증 대상이 됐다 —
 * {@code OUTSIDE_SEED_DATASET} 에서 빼고 {@code cy-seed/ddl} 이 만들게 한다.
 * ⑵ 이름만 스쳐 지나갔다 — 그래도 한 번은 눈으로 봐야 하므로 자동 면제는 두지 않는다.
 *
 * <p><b>한계를 적어 둔다.</b> 표 이름을 문자열로 조립하면({@code "notif" + "ications"})
 * 이 검사가 못 잡는다. 이 저장소의 검증 SQL 은 전부 상수 문자열이라 지금은 성립하지만,
 * 그 전제가 깨지면 이 가드도 함께 꺼진다.
 */
class OutsideSeedDatasetUnreferencedTest {

    /**
     * <b>검증이 사는 곳 전부.</b> 모듈 루트가 아니라 패키지까지 좁힌 것은, 같은 모듈의
     * 관리 화면·관측 코드가 이 표들을 <b>정당하게</b> 읽기 때문이다 — 그쪽까지 훑으면
     * 이 검사는 출발부터 빨갛고 아무 뜻이 없다.
     *
     * <p>테스트의 작업 디렉터리는 모듈 루트({@code storage/})다 — {@code ..} 가 저장소
     * 루트인 것은 {@code batch} 의 여러 대조 테스트가 이미 쓰는 방식과 같다.
     */
    private static final List<Path> VERIFICATION_SOURCES = List.of(
            Path.of("..", "core", "src", "main", "java", "com", "kafkick", "core", "verification"),
            Path.of("src", "main", "java", "com", "kafkick", "storage", "db", "verification"),
            Path.of("..", "batch", "src", "main", "java", "com", "kafkick", "batch", "job"),
            Path.of("..", "batch", "src", "main", "java", "com", "kafkick", "batch", "replay"));

    /**
     * <b>경로가 하나라도 없으면 검사가 조용히 0건이 된다.</b> 패키지를 옮기거나 이름을
     * 바꾸면 그 순간 이 가드가 통째로 꺼지므로, 없는 경로를 <b>통과가 아니라 실패</b>로 본다.
     */
    @Test
    @DisplayName("훑을 검증 경로가 전부 실재한다 — 하나라도 없으면 가드가 꺼진 것이다")
    void verificationSourceRootsExist() {
        for (Path root : VERIFICATION_SOURCES) {
            assertThat(Files.isDirectory(root))
                    .as("검증 경로가 없다: %s — 패키지가 옮겨졌으면 이 목록을 함께 고쳐라",
                            root.normalize())
                    .isTrue();
        }
    }

    @Test
    @DisplayName("대조에서 뺀 표를 검증 경로가 참조하지 않는다")
    void verificationNeverReadsTablesExcludedFromParity() throws IOException {
        List<String> violations = new ArrayList<>();

        for (String table : SchemaParityTestBase.OUTSIDE_SEED_DATASET) {
            Pattern reference = Pattern.compile("\\b" + Pattern.quote(table) + "\\b");
            for (Path root : VERIFICATION_SOURCES) {
                if (!Files.isDirectory(root)) {
                    continue; // 위 검사가 이미 실패로 알린다. 여기서 또 죽이면 원인이 가린다.
                }
                try (Stream<Path> files = Files.walk(root)) {
                    for (Path file : files.filter(Files::isRegularFile).toList()) {
                        String text = Files.readString(file, StandardCharsets.UTF_8);
                        if (reference.matcher(text).find()) {
                            violations.add(table + " ← " + root.getParent().relativize(file));
                        }
                    }
                }
            }
        }

        assertThat(violations)
                .as("""
                        검증 경로가 스키마 대조에서 뺀 표를 참조한다.

                        그 표를 뺀 근거가 "검증이 안 읽는다" 하나였으므로 근거가 깨졌다.
                        읽어야 하는 표라면 OUTSIDE_SEED_DATASET 에서 빼고 cy-seed/ddl 이
                        만들게 해라 — 안 그러면 그 표의 스키마 드리프트를 아무도 못 본다.""")
                .isEmpty();
    }
}
