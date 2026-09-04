// 팀 규칙에 어긋나는 orElse(null) 을 기계로 잡습니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>{@code .coderabbit.yaml} 이 적어 둔 규칙과 코드를 잇는다.</b>
 *
 * <p>규칙은 <i>"조회 결과가 없을 때 {@code null} 비교 대신 {@code Optional.orElseThrow} 를
 * 쓴다"</i> 인데, 저장소에 {@code orElse(null)} 이 <b>25곳</b> 있었고 규칙을 지키는 곳은
 * 1곳뿐이었다.
 *
 * <p><b>왜 티켓이 됐나.</b> CY-903 에서 리뷰가 그 규칙을 근거로 한 곳을 짚었을 때
 * <i>"25곳이 관례"</i> 라며 반려했다가, 규칙 파일을 확인하고 철회했다. 숫자가 많은 쪽이
 * 관례처럼 보였지만 <b>이 저장소는 적어 둔 것이 법이고 코드가 거기 맞춘다.</b>
 * 25곳은 관례가 아니라 부채였다.
 *
 * <h2>기계적으로 치환하면 안 된다 — 세 갈래다</h2>
 *
 * <table border="1">
 *   <caption>25곳의 분류 (CY-909)</caption>
 *   <tr><th>갈래</th><th>수</th><th>조치</th></tr>
 *   <tr><td>① 없으면 <b>분기</b>한다 — {@code == null} 비교가 뒤따른다</td><td>8</td>
 *       <td>{@code isEmpty()} 분기 · {@code orElseThrow} · {@code map/orElseGet}</td></tr>
 *   <tr><td>② 없을 때의 값이 <b>이미 정해져 있다</b></td><td>2</td>
 *       <td>{@code orElse(0L)} — null 을 거쳐 다시 비교할 이유가 없다</td></tr>
 *   <tr><td>③ <b>없음이 정상이고 null 이 그대로 값</b>이다</td><td>15</td>
 *       <td><b>그대로 둔다</b> — 아래 예산</td></tr>
 * </table>
 *
 * <p><b>예산 합은 15 가 아니라 17 이다.</b> 갈래 ① 을 고치면서
 * {@code ApiTopologyValidator} 의 {@code orElse(null)} 하나가 <b>둘로 늘었다</b> —
 * 조회 결과를 {@code Optional} 로 들고 있다가 응답 필드 <b>두 개</b>를 만들 때 각각
 * {@code null} 이 되기 때문이다. 늘어난 둘도 갈래 ③ 이다(nullable 응답 필드).
 * <b>고쳤는데 숫자가 늘어난 것</b>이라 헷갈리기 쉬워 여기 적어 둔다.
 *
 * <p>규칙의 의도는 <b>"조회 결과가 없을 때 null 비교"</b> 를 없애는 것이지 nullable 값
 * 자체를 금지하는 것이 아니다. 응답 DTO 의 nullable 필드를 {@code orElseThrow} 로 바꾸면
 * <b>없는 것이 정상인 자리에서 예외가 난다.</b>
 *
 * <h2>예산으로 든다</h2>
 *
 * <p>{@link NoOrphanJavadocTest} 와 같은 모양이다 — <b>늘어도 줄어도 실패시킨다.</b>
 * 갈래 ③ 을 고쳤으면 이 표에서 지워야 통과하고, 새 {@code orElse(null)} 은 어디에
 * 생기든 잡힌다. 표가 곧 분류이고, <b>주석이 아니라 검사라서 화석이 안 된다.</b>
 *
 * <p>테스트 코드는 안 본다 — 대역과 픽스처는 규칙의 대상이 아니다.
 */
class OrElseNullBudgetTest {

    /** 저장소 뿌리. 배치 테스트의 작업 디렉터리가 모듈 안이라 한 칸 올라간다. */
    private static final Path REPO_ROOT = Path.of("..");

    /**
     * 공백을 넣거나 줄을 바꿔도 잡는다. {@code orElse\u0028null\u0029} 만 찾으면
     * {@code orElse( null )} 이나 여러 줄로 쓴 같은 호출이 <b>검사를 그냥 지나간다</b> —
     * 동작은 똑같은데 예산에는 안 잡히는 구멍이 된다.
     */
    private static final Pattern OR_ELSE_NULL = Pattern.compile("orElse\\(\\s*null\\s*\\)");

    /**
     * 갈래 ③ — <b>없음이 정상이고 {@code null} 이 그대로 값</b>인 자리.
     *
     * <p>전부 <b>바깥으로 나가는 nullable 값</b>을 만든다. 응답 DTO 필드이거나,
     * nullable 을 받는 자리로 넘기거나, 포트가 "없으면 {@code null}" 을 계약으로 둔
     * 경우다 — 어느 쪽도 {@code == null} 비교로 분기하지 않는다.
     */
    private static final Map<String, Integer> BUDGET = Map.ofEntries(
            // 포트가 "없으면 null" 을 계약으로 둔 셋. 호출부는 그 값을 그대로 나른다
            // (예: `anchor != null ? anchor : runs.latestExecutionId(...)` — 비교 대상은
            // 요청 파라미터이지 조회 결과가 아니다).
            Map.entry("storage/src/main/java/com/kafkick/storage/db/verification/"
                    + "VerificationRunJdbcAdapter.java", 1),
            Map.entry("storage/src/main/java/com/kafkick/storage/db/verification/"
                    + "VerificationRuleJdbcAdapter.java", 1),
            Map.entry("storage/src/main/java/com/kafkick/storage/db/batch/"
                    + "BatchRunJdbcAdapter.java", 1),

            // 응답 DTO 의 nullable 필드를 만드는 자리. 회차 행이 아직 없으면 그 필드들이
            // 통째로 비는 것이 정상이다 — orElseThrow 로 바꾸면 정상 응답이 500 이 된다.
            Map.entry("api/src/main/java/com/kafkick/api/admin/benchmark/"
                    + "ApiTopologyValidator.java", 2),
            Map.entry("api/src/main/java/com/kafkick/api/admin/benchmark/dto/"
                    + "BenchmarkDetailResponse.java", 1),
            Map.entry("batch/src/main/java/com/kafkick/batch/api/VerifyRunView.java", 10),

            // nullable 을 받는 자리로 넘긴다 — 창을 못 읽어도 판정을 포기하지 않는다는
            // 것이 CY-768 이 정한 동작이고, 그 근거가 그 자리 주석에 적혀 있다.
            Map.entry("batch/src/main/java/com/kafkick/batch/config/"
                    + "ExpirePendingRefresher.java", 1));

    @Test
    @DisplayName("본코드의 orElse(null) 이 갈래 ③ 예산과 정확히 같다")
    void orElseNullStaysInsideItsBudget() throws IOException {
        try (Stream<Path> sources = Files.walk(REPO_ROOT)) {
            List<String> offenders = new ArrayList<>();
            List<String> visited = new ArrayList<>();
            for (Path path : sources
                    .filter(file -> file.toString().endsWith(".java"))
                    .filter(file -> file.toString().replace('\\', '/').contains("/src/main/"))
                    .filter(file -> !file.toString().replace('\\', '/').contains("/build/"))
                    .sorted()
                    .toList()) {
                String relative = REPO_ROOT.relativize(path).toString().replace('\\', '/');
                visited.add(relative);
                long found = OR_ELSE_NULL
                        .matcher(Files.readString(path, StandardCharsets.UTF_8))
                        .results().count();
                int budget = BUDGET.getOrDefault(relative, 0);
                if (found != budget) {
                    offenders.add(relative + " orElse(null)=" + found + " 예산=" + budget);
                }
            }

            assertThat(offenders)
                    .as("조회 결과가 없을 때 null 로 분기하지 않습니다(.coderabbit.yaml). "
                            + "없음이 정상이고 null 이 그대로 값인 자리라면 BUDGET 에 "
                            + "이유와 함께 적으십시오. 고쳤으면 BUDGET 에서 지우십시오")
                    .isEmpty();

            // **없는 파일은 위 순회가 아예 안 본다.** 파일이 지워지거나 옮겨지면 그 예산은
            // 검사되지 않은 채 조용히 통과하고, 예산표는 그때부터 사실이 아니게 된다 —
            // 나중에 같은 경로에 새 파일이 생기면 잘못된 면제까지 딸려 온다.
            assertThat(visited)
                    .as("BUDGET 에 있는 경로가 실제로 없습니다. 옮겼으면 경로를 고치고, "
                            + "지웠으면 BUDGET 에서도 지우십시오")
                    .containsAll(BUDGET.keySet());
        }
    }
}
