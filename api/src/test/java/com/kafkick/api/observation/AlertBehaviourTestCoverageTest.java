package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>모든 알림에 동작 시험이 있거나, 없는 이유가 적혀 있다.</b>
 *
 * <p>{@code promtool check rules} 는 <b>문법만</b> 본다. 규칙이 <i>"문법은 맞는데 영원히
 * 안 뜨는"</i> 상태로 바뀌어도 CI 가 초록이다 — {@code batch-alerts_test.yml} 의 머리말이
 * 그것을 적어 뒀고, <b>그쪽은 실제로 두 번 그 상태를 잡았다.</b>
 *
 * <p>그런데 <b>그 그물에 구멍이 13개 있었다</b>(CY-935 전). 작고 새로 만든 규칙 파일은
 * 100% 인데 크고 오래된 파일이 비어 있었고, 하필 <b>가장 많이 기대는 알림</b>들이 거기
 * 있었다 — 배치 실패의 정본, 시체 실행, 검증 판정.
 *
 * <p>{@link DomainMeterAlertCoverageTest} 가 <i>"지표에 알림이 있나"</i> 를 잠갔다면
 * 이 테스트는 <i>"알림에 시험이 있나"</i> 를 잠근다. <b>새 알림을 만든 사람이 그 자리에서
 * 시험을 요구받는다.</b>
 */
class AlertBehaviourTestCoverageTest {

    private static final Path RULES_DIR = Path.of("../infra/prometheus/rules");
    private static final Path TESTS_DIR = Path.of("../infra/prometheus/tests");

    private static final Pattern ALERT = Pattern.compile("^\\s*- alert:\\s*(\\w+)\\s*$",
            Pattern.MULTILINE);
    private static final Pattern ALERTNAME = Pattern.compile("^\\s*alertname:\\s*(\\w+)\\s*$",
            Pattern.MULTILINE);

    /**
     * <b>일부러 동작 시험을 안 쓴 알림과 그 이유.</b>
     *
     * <p><b>시험을 위해 규칙을 고치지 않는다.</b> 재기 어려운 표현식이 있으면 그것을 여기
     * 적는 것이 맞다 — 규칙을 시험하기 쉽게 바꾸면 <b>운영 동작이 바뀐다.</b>
     *
     * <p>지금은 비어 있다. 13개를 메우고 나니 남는 것이 없었다 — 비어 있는 것이 정상이고,
     * 채우게 되는 날 <b>왜 못 쓰는지</b>가 여기 남는다.
     */
    private static final Map<String, String> DELIBERATELY_UNTESTED = new LinkedHashMap<>();

    @Test
    @DisplayName("모든 알림에 동작 시험이 있거나, 없는 이유가 적혀 있다")
    void everyAlertIsEitherExercisedOrExplainedAway() throws IOException {
        List<String> tested = namesIn(TESTS_DIR, ALERTNAME);
        List<String> untested = new ArrayList<>();
        for (String alert : namesIn(RULES_DIR, ALERT)) {
            if (tested.contains(alert) || DELIBERATELY_UNTESTED.containsKey(alert)) {
                continue;
            }
            untested.add(alert);
        }

        assertThat(untested)
                .as("""
                        promtool 동작 시험을 쓰거나, 왜 못 쓰는지 DELIBERATELY_UNTESTED 에 적으십시오.
                        check rules 는 문법만 봅니다 — '문법은 맞는데 영원히 안 뜨는' 규칙을
                        CI 가 초록으로 통과시킵니다(이 저장소에서 실제로 두 번 있었습니다).""")
                .isEmpty();
    }

    /** 시험을 쓰고도 면제 목록에 남아 있으면, 다음 사람이 <b>없는 줄 알고</b> 또 쓴다. */
    @Test
    @DisplayName("시험이 있는 알림은 '일부러 안 썼다' 목록에 남아 있지 않다")
    void nothingIsBothTestedAndExcused() throws IOException {
        List<String> tested = namesIn(TESTS_DIR, ALERTNAME);

        assertThat(DELIBERATELY_UNTESTED.keySet().stream().filter(tested::contains))
                .as("시험을 썼으면 DELIBERATELY_UNTESTED 에서 빼십시오")
                .isEmpty();
    }

    /** 사라진 알림을 목록이 붙들고 있으면 그 문장이 거짓이 된다. */
    @Test
    @DisplayName("'일부러 안 썼다' 목록이 실재하는 알림만 가리킨다")
    void theExcuseListDoesNotNameGhosts() throws IOException {
        assertThat(namesIn(RULES_DIR, ALERT))
                .as("규칙에 없는 알림 이름이 목록에 있다")
                .containsAll(DELIBERATELY_UNTESTED.keySet());
    }

    /**
     * <b>시험 파일이 {@code rules/} 밖에 있어야 한다.</b> 안에 두면
     * {@code prometheus.yml} 의 글롭({@code rules/*.yml})에 걸려 프로메테우스가 그것을
     * 규칙으로 읽으려다 <b>기동에 실패한다</b> — 시험 파일들이 머리말에 적어 둔 경고다.
     */
    @Test
    @DisplayName("시험 파일이 규칙 디렉터리 안으로 들어와 있지 않다")
    void testFilesStayOutOfTheRulesDirectory() throws IOException {
        try (Stream<Path> files = Files.list(RULES_DIR)) {
            assertThat(files.map(Path::getFileName).map(Path::toString))
                    .as("여기 있는 파일은 프로메테우스가 규칙으로 읽는다 — 시험이 섞이면 기동이 죽는다")
                    .noneMatch(name -> name.endsWith("_test.yml"));
        }
    }

    /**
     * <b>글롭으로 읽는다.</b> 파일 이름을 적으면 새 규칙·시험 파일이 조용히 빠진다 —
     * CI 의 promtool 루프가 같은 이유로 글롭이다.
     */
    private static List<String> namesIn(Path directory, Pattern pattern) throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            List<Path> yml = files.filter(p -> p.toString().endsWith(".yml")).sorted().toList();
            assertThat(yml).as("%s 에 yml 이 하나도 없다 — 경로가 바뀌었는지 확인하라", directory)
                    .isNotEmpty();
            List<String> names = new ArrayList<>();
            for (Path file : yml) {
                Matcher matcher = pattern.matcher(Files.readString(file, StandardCharsets.UTF_8));
                while (matcher.find()) {
                    names.add(matcher.group(1));
                }
            }
            assertThat(names).as("%s 에서 이름을 하나도 못 읽었다", directory).isNotEmpty();
            return names;
        }
    }
}
