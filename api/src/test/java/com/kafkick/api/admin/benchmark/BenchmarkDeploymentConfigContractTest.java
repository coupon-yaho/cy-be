package com.kafkick.api.admin.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class BenchmarkDeploymentConfigContractTest {

    @Test
    void envTemplateExposesEveryRequiredMeasurementInput() throws Exception {
        Path working = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        Path repository = Files.exists(working.resolve(".env.example"))
            ? working : working.getParent();
        String env = Files.readString(repository.resolve(".env.example"));

        assertThat(env).contains(
            "BENCHMARK_APP_REPLICAS=1",
            "BENCHMARK_BATCH_CONSISTENCY_READ_TIMEOUT=3s",
            "BENCHMARK_CONSISTENCY_CLAIM_LEASE=5m",
            "BENCHMARK_CONSISTENCY_MAX_OBSERVATION_LAG=15m",
            "DOMAIN_GAUGE_COUPON_ID=");
        assertThat(env).containsPattern("(?m)^BENCHMARK_ADMIN_COMMAND_SECRET=\\s*$");

        String application = Files.readString(repository.resolve("application.yml.example"));
        assertThat(application).contains(
            "claim-lease: ${BENCHMARK_ARCHIVE_CLAIM_LEASE:5m}",
            "max-samples: ${BENCHMARK_ARCHIVE_MAX_SAMPLES:10000}",
            "base-url: ${BENCHMARK_BATCH_BASE_URL:http://batch:9091}",
            "connect-timeout: ${BENCHMARK_BATCH_CONNECT_TIMEOUT:100ms}",
            "read-timeout: ${BENCHMARK_BATCH_READ_TIMEOUT:300ms}",
            "consistency-read-timeout: ${BENCHMARK_BATCH_CONSISTENCY_READ_TIMEOUT:3s}",
            "claim-lease: ${BENCHMARK_CONSISTENCY_CLAIM_LEASE:5m}",
            "max-observation-lag: ${BENCHMARK_CONSISTENCY_MAX_OBSERVATION_LAG:15m}");
    }
}
