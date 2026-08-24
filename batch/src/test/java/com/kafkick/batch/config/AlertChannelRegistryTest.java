// 알림 채널 대장(docs/14)이 규칙 파일과 어긋나지 않는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>{@code channel} 라벨은 라우팅의 유일한 축이다.</b> 빠뜨린 알림은 alertmanager 의
 * {@code sink-unrouted} 로 떨어져 <b>아무도 안 보는 곳</b>에 쌓인다. {@code docs/14} 의 채널
 * 표가 그 점검 기준인데, <b>표와 규칙 파일을 잇는 것이 아무것도 없었다</b> — 이 티켓 직전의
 * 표는 규칙 넷({@code Verification*} 셋 · {@code AlertDeliveryFailing})을 빠뜨린 채였고,
 * 그 위에 새 알림이 붙으면서 <i>완전한 등록부처럼</i> 보였다.
 *
 * <p>이 저장소는 그 실패 모드에 이름을 붙여 뒀다 —
 * <i>"이 저장소에서 같은 형태의 거짓말을 세 번 했다 … 그래서 문장으로 잇지 않고 파일을
 * 읽어서 잇는다"</i>({@code BatchMetricExposureTest}). 이 클래스가 그 방식이다.
 *
 * <p><b>컨테이너도 컨텍스트도 안 띄운다.</b> 재는 것이 두 텍스트 파일의 집합 비교다.
 */
class AlertChannelRegistryTest {

    private static final Path RULES = Path.of("../infra/prometheus/rules/batch-alerts.yml");

    private static final Path CHANNEL_DOC = Path.of("../docs/14-observability-wiring.md");

    private static final Pattern ALERT = Pattern.compile("(?m)^\\s*- alert: (\\w+)");

    private static final Pattern BACKTICKED = Pattern.compile("`(\\w+)`");

    @Test
    @DisplayName("규칙 파일의 알림 전부가 docs/14 채널 표에 있다 — 빠지면 sink-unrouted 로 간다")
    void everyAlertIsRegisteredInTheChannelTable() throws IOException {
        Set<String> declared = matches(ALERT, Files.readString(RULES, StandardCharsets.UTF_8));
        Set<String> listed = matches(BACKTICKED, channelTable());

        // **비어 있으면 allSatisfy 가 무조건 통과한다.** 정규식이 어긋나 0개를 뽑으면
        // 이 클래스가 막겠다고 적은 상태(표가 완전해 보이는 것)가 그대로 재현된다.
        assertThat(declared)
                .as("규칙 파일에서 알림을 하나도 못 뽑았다 — ALERT 정규식이 어긋났다")
                .isNotEmpty();
        assertThat(listed)
                .as("채널 표에서 이름을 하나도 못 뽑았다 — 표 형식이 바뀌었다")
                .isNotEmpty();
        assertThat(declared)
                .as("표에 없는 알림은 채널 배정을 아무도 검토하지 않았다는 뜻이다. "
                        + "docs/14 의 채널 표에 추가해라")
                .allSatisfy(alert -> assertThat(listed).contains(alert));
    }

    @Test
    @DisplayName("채널 표에 없는 알림 이름이 표에 남아 있지 않다 — 지운 알림의 잔재")
    void channelTableHasNoStaleEntries() throws IOException {
        Set<String> declared = matches(ALERT, Files.readString(RULES, StandardCharsets.UTF_8));
        Set<String> listed = matches(BACKTICKED, channelTable());

        assertThat(listed).as("채널 표에서 이름을 하나도 못 뽑았다").isNotEmpty();
        assertThat(declared).as("규칙 파일에서 알림을 하나도 못 뽑았다").isNotEmpty();
        assertThat(listed)
                .as("규칙에서 사라진 알림이 표에 남아 있으면, 다음 사람이 그것을 근거로 "
                        + "존재하지 않는 알림을 기다린다")
                .allSatisfy(alert -> assertThat(declared).contains(alert));
    }

    /**
     * <b>표의 첫 열만 잘라낸다.</b> 문서 전체에서 백틱 낱말을 뽑으면 메트릭 이름·설정 키가
     * 섞이고, 표 전체에서 뽑으면 <b>채널 값({@code server}·{@code data})</b>이 알림 이름으로
     * 섞인다 — 실제로 그렇게 빨개졌다.
     */
    private static String channelTable() throws IOException {
        String doc = Files.readString(CHANNEL_DOC, StandardCharsets.UTF_8);
        int start = doc.indexOf("| 알림 | channel |");
        assertThat(start)
                .as("docs/14 의 채널 표 머리글을 못 찾았다 — 표를 옮겼다면 이 테스트도 함께 "
                        + "고쳐라. 그냥 지우면 표와 규칙을 잇는 것이 다시 사라진다")
                .isNotNegative();
        int end = doc.indexOf("\n\n", start);
        String table = doc.substring(start, end < 0 ? doc.length() : end);
        // 각 행의 첫 칸(| … |)만 남긴다. 둘째 칸이 channel 값이다.
        return table.lines()
                .filter(line -> line.startsWith("|"))
                .map(line -> line.split("\\|", 3))
                .filter(cells -> cells.length > 1)
                .map(cells -> cells[1])
                .collect(Collectors.joining("\n"));
    }

    private static Set<String> matches(Pattern pattern, String text) {
        Matcher matcher = pattern.matcher(text);
        return matcher.results().map(result -> result.group(1)).collect(Collectors.toSet());
    }
}
