// 잡 파라미터 이름을 예산으로 붙듭니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>잡 파라미터 값은 관제 화면에 그대로 나간다.</b> 그러니 무엇이 파라미터가 되는지를
 * 규약으로 붙든다.
 *
 * <h2>왜 값을 줄이지 않고 이름을 붙드나</h2>
 *
 * <p>실패 원문({@code EXIT_MESSAGE})은 <b>프레임워크가</b> 스택을 넣는 자리라 우리가 통제
 * 못 한다 — 그래서 {@code FailureSummary} 가 줄인다. 파라미터는 반대로 <b>우리가 넣는
 * 값</b>이라 통제가 가능하다. 값을 줄이면 재현에 필요한 정보(어떤 {@code asOf} 로 돌았나)가
 * 같이 사라지므로, <b>값은 그대로 두고 무엇을 넣을지를 붙든다.</b>
 *
 * <p>그 규약이 <b>주석으로만 있으면 지켜지지 않는다</b> — 리뷰가 짚은 자리다. 새 파라미터를
 * 추가하는 사람이 그 주석을 읽을 이유가 없다. 여기서 막으면 <b>추가하는 순간</b> 걸린다.
 *
 * <h2>무엇을 못 막는가</h2>
 *
 * <p>이름만 본다. 누가 {@code memberId} 라는 이름을 예산에 넣으면서 값에 이메일을 담으면
 * 이 검사는 통과한다. 그때 막는 것은 <b>예산을 늘리는 그 리뷰</b>이지 정규식이 아니다 —
 * 이 검사가 하는 일은 <b>그 결정이 리뷰를 거치게 만드는 것</b>이다.
 *
 * <p>{@link NoOrphanJavadocTest} 의 예산과 같은 모양이라 늘어도 줄어도 실패한다.
 */
class JobParameterNameBudgetTest {

    /** 저장소 뿌리. 배치 테스트의 작업 디렉터리가 모듈 안이라 한 칸 올라간다. */
    private static final Path REPO_ROOT = Path.of("..");

    /** {@code JobParametersBuilder} 의 {@code addXxx("name", …)} 에서 이름만 뽑는다. */
    private static final Pattern PARAMETER_NAME =
            Pattern.compile("\\.add(?:String|Long|Double|Date|LocalDate|LocalDateTime)"
                    + "\\(\\s*\"([A-Za-z_][A-Za-z0-9_]*)\"");

    /**
     * 지금 쓰는 파라미터 전부. <b>전부 시각·회차·이름이고 사람을 가리키는 값이 없다.</b>
     *
     * <ul>
     *   <li>{@code asOf} · {@code firedAt} — 시각</li>
     *   <li>{@code attempt} · {@code seedRunId} — 회차 식별자</li>
     *   <li>{@code dataset} · {@code scope} — 검증 대상 구분</li>
     * </ul>
     *
     * <p><b>여기에 이름을 더할 때 물을 것</b> — 그 값이 관제 화면에 그대로 떠도 되는가.
     * 회원 식별자·연락처·주문 내역처럼 <b>사람을 가리키는 값</b>이면 파라미터가 아니라
     * 잡 안에서 조회할 것이다.
     */
    private static final Set<String> BUDGET =
            Set.of("asOf", "attempt", "dataset", "firedAt", "scope", "seedRunId");

    @Test
    @DisplayName("잡 파라미터 이름이 예산과 정확히 같다 — 값은 관제에 그대로 나간다")
    void jobParameterNamesStayInsideTheirBudget() throws IOException {
        Set<String> found = new TreeSet<>();
        try (Stream<Path> sources = Files.walk(REPO_ROOT)) {
            List<Path> files = sources
                    .filter(file -> file.toString().endsWith(".java"))
                    .filter(file -> file.toString().replace('\\', '/').contains("/src/main/"))
                    .filter(file -> !file.toString().replace('\\', '/').contains("/build/"))
                    .toList();
            for (Path file : files) {
                Matcher name = PARAMETER_NAME.matcher(
                        Files.readString(file, StandardCharsets.UTF_8));
                while (name.find()) {
                    found.add(name.group(1));
                }
            }
        }

        assertThat(found)
                .as("잡 파라미터 값은 관제 화면(/batch-executions/{id}/parameters)에 "
                        + "그대로 나갑니다. 사람을 가리키는 값이면 파라미터가 아니라 잡 안에서 "
                        + "조회하십시오. 안전한 값이면 BUDGET 에 추가하십시오")
                .containsExactlyInAnyOrderElementsOf(BUDGET);
    }
}
