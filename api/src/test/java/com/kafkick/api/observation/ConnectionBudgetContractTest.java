package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class ConnectionBudgetContractTest {
    @Test
    void poolDefaultsAndMysqlRuntimeLimitFitRollingOverlap() throws Exception {
        Map<String, String> env = Files.readAllLines(ConfigContractFixture.repoRoot().resolve(".env.example"))
                .stream()
                .filter(line -> !line.isBlank() && !line.startsWith("#") && line.contains("="))
                .map(line -> line.split("=", 2))
                .collect(java.util.stream.Collectors.toMap(parts -> parts[0], parts -> parts[1]));
        int apiMain = Integer.parseInt(env.get("DB_POOL_SIZE"));
        int batchMain = Integer.parseInt(env.get("BATCH_DB_POOL_SIZE"));
        int mysqlLimit = Integer.parseInt(env.get("BENCHMARK_MYSQL_MAX_CONNECTIONS"));
        int operationalTotal = Integer.parseInt(env.get("BENCHMARK_HIKARI_POOL_TOTAL"));

        assertThat(operationalTotal).isEqualTo(apiMain * 4);
        assertThat((apiMain + 2) * 4 + batchMain + 2).isEqualTo(66);
        assertThat((apiMain + 2) * 5 + batchMain + 2).isLessThan(mysqlLimit);

        Map<String, Object> compose = ConfigContractFixture.loadYaml(
                ConfigContractFixture.repoRoot().resolve("compose.yml"));
        Map<String, Object> services = cast(compose.get("services"));
        Map<String, Object> mysql = cast(services.get("mysql"));
        assertThat((List<?>) mysql.get("command")).singleElement()
                .isEqualTo("--max-connections=151");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> cast(Object value) {
        return (Map<String, Object>) value;
    }
}
