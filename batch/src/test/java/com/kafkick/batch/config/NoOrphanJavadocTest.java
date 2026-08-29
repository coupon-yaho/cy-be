// 선언에 안 붙는 javadoc 을 기계로 잡습니다.
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
 * <b>새 멤버를 남의 javadoc 과 그 선언 <i>사이</i> 에 끼워 넣으면 앞 블록이 주인을 잃는다.</b>
 * javadoc 도구는 선언 <b>직전의 마지막</b> 주석 하나만 붙이므로, 앞 블록은 어디에도 안 붙고
 * 그 선언은 문서가 통째로 사라진다. 컴파일도 테스트도 안 잡는다.
 *
 * <p><b>왜 기계로 잡나.</b> 사람이 기억해서 확인하는 방식이 <b>한 세션에서 여섯 번</b>
 * 실패했다 — CY-678 에서 세 파일, CY-686 에서 세 파일이다. 매번 리뷰가 잡아 라운드를
 * 하나씩 태웠다. 검사 자체는 정규식 하나다.
 *
 * <p><b>예산으로 든다.</b> {@code NoWallClockInBatchTest} 의 시계 예산과 같은 모양이다 —
 * 늘어도 줄어도 실패시킨다. 기존 넷은 이 티켓의 범위 밖이라 값으로 박아 뒀다.
 * <b>고치면 이 표에서 지워야 통과한다</b> — 그래야 예산이 화석이 안 된다. 새로 더하지 마라.
 */
class NoOrphanJavadocTest {

    /** 저장소 뿌리. 배치 테스트의 작업 디렉터리가 모듈 안이라 한 칸 올라간다. */
    private static final Path REPO_ROOT = Path.of("..");

    /** {@code *&#47;} 다음에 곧바로 {@code /**} 가 오는 자리. 그 앞 블록이 주인을 잃는다. */
    private static final Pattern ORPHAN = Pattern.compile("\\*/\\s*\\n\\s*/\\*\\*");

    /**
     * 이 티켓 이전부터 있던 것들. 원주인을 정확히 집으려면 각 javadoc 의 뜻을 읽어야 해서
     * 범위 밖으로 뒀다 — 잘못 옮기면 남의 문서를 엉뚱한 선언에 붙인다.
     */
    private static final Map<String, Integer> BUDGET = Map.ofEntries(
            Map.entry("batch/src/main/java/com/kafkick/batch/api/VerifyTriggerController.java", 1),
            Map.entry("batch/src/test/java/com/kafkick/batch/config/ResolvedBatchConfigTest.java", 1),
            Map.entry("batch/src/test/java/com/kafkick/batch/schedule/"
                    + "ExpireSchedulerReportingTest.java", 1),
            Map.entry("storage/src/test/java/com/kafkick/storage/db/verification/"
                    + "VerificationRunJdbcAdapterTest.java", 1),

            // ── 아래 여덟은 CY-744 합류로 들어온 **다른 영역 파일**이다 ──
            //
            // 이 검사는 배치가 세운 규약(새 멤버를 남의 javadoc 과 선언 사이에 끼우지 않는다)
            // 인데, 저장소 전체를 훑으므로 합류 순간 남의 파일까지 판정 대상이 됐다.
            // **남의 파일을 고쳐서 통과시키지 않는다** — 그 규약을 그쪽이 채택한 적이 없고,
            // 고치면 그 영역의 diff 에 이유 없는 변경이 섞인다. 사실대로 예산에 적는다.
            //
            // 새로 생기는 위반은 여전히 잡힌다 — 예산은 **정확히 같아야** 통과하므로
            // 하나만 늘어도 빨개진다.
            Map.entry("api/src/test/java/com/kafkick/api/admin/observability/"
                    + "LatencySeriesOutcomeContractTest.java", 1),
            Map.entry("api/src/test/java/com/kafkick/api/observation/"
                    + "AutoInstrumentedMetersTest.java", 1),
            Map.entry("api/src/test/java/com/kafkick/api/observation/"
                    + "DeployedConfigContractTest.java", 1),
            Map.entry("batch/src/main/java/com/kafkick/batch/observation/"
                    + "ConsistencyRawValueReader.java", 1),
            Map.entry("batch/src/test/java/com/kafkick/batch/config/"
                    + "ObservationAccountPrivilegeTest.java", 1),
            Map.entry("core/src/main/java/com/kafkick/core/observation/SourceStatusCode.java", 1),
            Map.entry("infra/mq/src/main/java/com/kafkick/infra/mq/config/"
                    + "KafkaTopicProvisioner.java", 1),
            Map.entry("infra/mq/src/test/java/com/kafkick/infra/mq/config/"
                    + "KafkaTopicProvisionerTest.java", 1));

    @Test
    @DisplayName("선언에 안 붙는 javadoc 이 예산과 정확히 같다")
    void orphanJavadocStaysInsideItsBudget() throws IOException {
        try (Stream<Path> sources = Files.walk(REPO_ROOT)) {
            List<String> offenders = new ArrayList<>();
            for (Path path : sources
                    .filter(file -> file.toString().endsWith(".java"))
                    .filter(file -> file.toString().replace('\\', '/').contains("/src/"))
                    .filter(file -> !file.toString().replace('\\', '/').contains("/build/"))
                    .sorted()
                    .toList()) {
                String relative = REPO_ROOT.relativize(path).toString().replace('\\', '/');
                long found = ORPHAN
                        .matcher(Files.readString(path, StandardCharsets.UTF_8))
                        .results().count();
                int budget = BUDGET.getOrDefault(relative, 0);
                if (found != budget) {
                    offenders.add(relative + " 고아=" + found + " 예산=" + budget);
                }
            }

            assertThat(offenders)
                    .as("새 멤버를 남의 javadoc 과 그 선언 사이에 넣었습니다. 새 블록과 그 "
                            + "선언을 함께 아래로 내리십시오 — 앞 블록이 원래 주인 바로 위에 "
                            + "다시 붙어야 합니다. 기존 것을 고쳤으면 BUDGET 에서 지우십시오")
                    .isEmpty();
        }
    }
}
