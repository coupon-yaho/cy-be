// 배치 메타 시각을 읽는 파일과 횟수를 예산으로 못 박습니다.
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
 * <b>배치 메타 시각은 JVM 기본 존 벽시계다.</b> 스프링 배치가 <b>인자 없는
 * {@code LocalDateTime.now()}</b> 로 찍기 때문이다(6.0.4 바이트코드). 이 저장소의 도메인
 * 시각은 전부 {@code TimeProvider}(UTC)라, 그 값을 <b>변환 없이 도메인이나 응답으로 흘리면
 * 축이 갈린다</b> — 예외도 로그도 안 나고, 배포는 {@code DefaultZoneGuard} 가 UTC 를 강제해
 * <b>비UTC 환경에서만 조용히</b> 어긋난다.
 *
 * <p><b>{@link BatchTimeAxis} 를 만들었지만 그것만으로는 부족하다.</b> 부르는 것을 강제하는
 * 수단이 없어서, 새 조회를 여는 사람이 {@code execution.getStartTime()} 을 응답 레코드에 그대로
 * 담으면 아무도 안 잡는다. 실제로 CY-743 이 그 상태를 하나 남겼다가 리뷰에서 잡혔다
 * ({@code StuckRunView}).
 *
 * <p><b>형제 선례는 {@code StuckBeforeBindingIsCentralizedTest} 다</b> — 그쪽도 바인딩을 한
 * 곳에 모으고도 부족해서 소스를 훑는다. 여기서는 <b>읽는 자리를 세어</b> 새 자리가 생기면
 * 사람이 한 번 보게 만든다.
 *
 * <p>⚠️ <b>주석 안의 같은 형태도 센다.</b> 정규식이라 그렇다 — 문서에 적을 때는
 * {@code JobExecution#getStartTime} 처럼 점 호출이 아닌 모양으로 쓴다.
 */
class BatchMetaTimeBudgetTest {

    private static final Path SOURCE_ROOT = Path.of("src/main/java");

    /** 점 호출만 본다. {@code {@code getStartTime()}} 같은 문서 표기는 안 센다. */
    private static final Pattern BATCH_META_TIME =
            Pattern.compile("\\.\\s*get(Start|End|Create)Time\\s*\\(\\s*\\)");

    /**
     * <pre>
     * BatchTimeAxis           1  javadoc 의 점 호출 표기 하나. 변환의 정의 자리다
     * VerifyJobConfig         2  startRunStep · finalizeRunStep — 둘 다 onDomainAxis 를 지난다
     * VerifyRunView           2  조회 응답 — 둘 다 널가드를 거쳐 onDomainAxis 를 지난다
     * StuckRunView            2  시체 목록 응답 — CY-743 이 옮겼다
     * RunningJobProbe         3  **배치 메타 축 안에서만** 비교한다. 도메인으로 안 넘긴다 —
     *                            untilStuck 이 LocalDateTime.now() 로 같은 축에서 뺀다
     * ExpireRecoveryService   3  null 검사와 "이미 끝났나" 판정뿐. 값을 밖으로 안 낸다
     * CleanupRecoveryService  3  같은 축
     * </pre>
     */
    private static final Map<String, Integer> BUDGET = Map.of(
            "com/kafkick/batch/config/BatchTimeAxis.java", 1,
            "com/kafkick/batch/job/VerifyJobConfig.java", 2,
            "com/kafkick/batch/api/VerifyRunView.java", 2,
            "com/kafkick/batch/api/StuckRunView.java", 2,
            "com/kafkick/batch/config/RunningJobProbe.java", 3,
            "com/kafkick/batch/api/ExpireRecoveryService.java", 3,
            "com/kafkick/batch/api/CleanupRecoveryService.java", 3);

    @Test
    @DisplayName("배치 메타 시각을 읽는 파일과 횟수가 예산과 정확히 같다")
    void batchMetaTimeReadsStayInsideTheirBudget() throws IOException {
        try (Stream<Path> sources = Files.walk(SOURCE_ROOT)) {
            List<String> offenders = new ArrayList<>();
            for (Path path : sources.filter(f -> f.toString().endsWith(".java")).toList()) {
                // 예산 키는 / 로 적는다. Path.toString() 은 윈도우에서 \\ 를 주므로
                // 정규화 안 하면 **모든 파일이 예산 0 으로 판정**돼 통째로 빨개진다.
                String relative = SOURCE_ROOT.relativize(path).toString().replace('\\', '/');
                long reads = BATCH_META_TIME
                        .matcher(Files.readString(path, StandardCharsets.UTF_8))
                        .results().count();
                int budget = BUDGET.getOrDefault(relative, 0);
                if (reads != budget) {
                    offenders.add(relative + " 읽기=" + reads + " 예산=" + budget);
                }
            }

            assertThat(offenders)
                    .as("""
                            배치 메타 시각은 JVM 기본 존이다. 도메인이나 응답으로 넘기려면 \
                            BatchTimeAxis.onDomainAxis 를 거쳐라 — 안 거치면 같은 응답 안에서 \
                            asOf 와 좌표계가 갈리고, 배포는 UTC 라 증상이 안 드러난다. \
                            같은 축 안에서만 비교하는 자리라면 예산을 고치고 **왜인지 함께 \
                            적어라**. 주석의 점 호출도 세므로 문서에는 JobExecution#getStartTime \
                            처럼 적는다""")
                    .isEmpty();
        }
    }
}
