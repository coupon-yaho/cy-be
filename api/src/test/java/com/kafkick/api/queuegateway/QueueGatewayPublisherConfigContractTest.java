package com.kafkick.api.queuegateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class QueueGatewayPublisherConfigContractTest {

    private static final Path REPO_ROOT = Path.of("..").toAbsolutePath().normalize();

    @Test
    void apiExampleAndEnvironmentExposeTheSamePublisherSettings() throws Exception {
        String application = Files.readString(
                REPO_ROOT.resolve("api/src/main/resources/application.yml.example"));
        String environment = Files.readString(REPO_ROOT.resolve(".env.example"));

        assertThat(application).contains(
                "${QUEUE_GATEWAY_PUBLISHER_ENABLED:false}",
                "${QUEUE_GATEWAY_CAPACITY_INTERVAL:1s}",
                "${QUEUE_GATEWAY_COUPON_ROUND_INTERVAL:5s}",
                "${QUEUE_GATEWAY_CREDITS_PER_SECOND:0}",
                "${QUEUE_GATEWAY_INSTANCE_ID:${HOSTNAME:api-local}}");
        assertThat(environment).contains(
                "QUEUE_GATEWAY_PUBLISHER_ENABLED=false",
                "QUEUE_GATEWAY_CAPACITY_INTERVAL=1s",
                "QUEUE_GATEWAY_COUPON_ROUND_INTERVAL=5s",
                "QUEUE_GATEWAY_CREDITS_PER_SECOND=0",
                "# QUEUE_GATEWAY_INSTANCE_ID=api-local");
    }
}
