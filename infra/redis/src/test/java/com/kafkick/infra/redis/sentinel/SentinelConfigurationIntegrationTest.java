package com.kafkick.infra.redis.sentinel;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/** 테스트가 만든 문자열이 아니라 저장소의 sentinel.conf 자체를 Redis가 파싱한다. */
@Testcontainers(disabledWithoutDocker = true)
class SentinelConfigurationIntegrationTest {

    @Test
    void committedSentinelConfigurationStartsAndRegistersTheMaster() throws Exception {
        try (GenericContainer<?> sentinel = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
                .withCopyFileToContainer(MountableFile.forHostPath("sentinel.conf"), "/config/sentinel.conf")
                .withExposedPorts(26379)
                .withCommand("/bin/sh", "-ec", "cp /config/sentinel.conf /tmp/sentinel.conf "
                        + "&& exec redis-server /tmp/sentinel.conf --sentinel")) {
            sentinel.start();

            assertThat(sentinel.execInContainer("redis-cli", "-p", "26379", "ping").getStdout().trim())
                    .isEqualTo("PONG");
        }
    }
}
