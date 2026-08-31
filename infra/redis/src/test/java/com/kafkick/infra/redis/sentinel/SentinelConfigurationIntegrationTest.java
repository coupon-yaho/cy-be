package com.kafkick.infra.redis.sentinel;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * 테스트가 만든 문자열이 아니라 저장소의 sentinel.conf 자체를 Redis가 파싱한다.
 *
 * <p><b>보는 것은 치환 <i>결과</i>가 유효한가뿐이다.</b> 배포는 compose 가
 * {@code REDIS_MASTER_IP} 로 자리표시자를 바꾸는데, 여기서는 그 셸 명령을 태우지 않고
 * 루프백을 직접 넣는다 — 감시 대상이 뜰 필요가 없기 때문이다. 그 배선이 깨지는 것은
 * {@code SentinelComposeContractTest} 가 잡는다.
 */
@Testcontainers(disabledWithoutDocker = true)
class SentinelConfigurationIntegrationTest {

    @Test
    void committedSentinelConfigurationStartsAndRegistersTheMaster() throws Exception {
        try (GenericContainer<?> sentinel = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
                .withCopyFileToContainer(MountableFile.forHostPath("sentinel.conf"), "/config/sentinel.conf")
                .withExposedPorts(26379)
                .withCommand("/bin/sh", "-ec",
                        "sed 's/__MASTER_ADDR__/127.0.0.1/' /config/sentinel.conf "
                                + "> /tmp/sentinel.conf "
                                + "&& exec redis-server /tmp/sentinel.conf --sentinel")) {
            sentinel.start();

            assertThat(sentinel.execInContainer("redis-cli", "-p", "26379", "ping").getStdout().trim())
                    .isEqualTo("PONG");
        }
    }
}
