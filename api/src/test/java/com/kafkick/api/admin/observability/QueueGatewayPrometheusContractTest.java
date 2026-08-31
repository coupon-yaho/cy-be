package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class QueueGatewayPrometheusContractTest {

    @Test
    void fixturePinsTheSevenDocumentedPrometheusNamesAndTypes() throws Exception {
        String exposition;
        try (var input = getClass().getResourceAsStream(
                "/queue-gateway/v0.4.0-prometheus.txt")) {
            assertThat(input).isNotNull();
            exposition = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }

        Map<String, String> types = exposition.lines()
                .filter(line -> line.startsWith("# TYPE "))
                .map(line -> line.split("\\s+"))
                .collect(Collectors.toMap(parts -> parts[2], parts -> parts[3]));

        assertThat(types).containsExactlyInAnyOrderEntriesOf(Map.of(
                QueueGatewayPrometheusContract.WAITING, "gauge",
                QueueGatewayPrometheusContract.SNAPSHOT_AGE, "gauge",
                QueueGatewayPrometheusContract.CAPACITY_CREDIT, "gauge",
                QueueGatewayPrometheusContract.CAPACITY_NODES, "gauge",
                QueueGatewayPrometheusContract.JUDGEMENT_TOTAL, "counter",
                QueueGatewayPrometheusContract.BACKEND_FALLBACK_TOTAL, "counter",
                QueueGatewayPrometheusContract.ALLOCATION_OVERSHOOT_TOTAL, "counter"));
    }

    @Test
    void fixtureRequiresOnlyTheDocumentedQualityLabel() throws Exception {
        String exposition;
        try (var input = getClass().getResourceAsStream(
                "/queue-gateway/v0.4.0-prometheus.txt")) {
            exposition = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        Set<String> sampleLines = exposition.lines()
                .filter(line -> !line.isBlank() && !line.startsWith("#"))
                .collect(Collectors.toSet());

        assertThat(sampleLines).anyMatch(line -> line.startsWith(
                QueueGatewayPrometheusContract.JUDGEMENT_TOTAL + "{quality=\"degraded\"}"));
        assertThat(sampleLines.stream()
                .filter(line -> !line.startsWith(QueueGatewayPrometheusContract.JUDGEMENT_TOTAL))
                .flatMap(line -> Arrays.stream(line.split("\\s+", 2)).limit(1)))
                .noneMatch(line -> line.contains("{"));
    }
}
