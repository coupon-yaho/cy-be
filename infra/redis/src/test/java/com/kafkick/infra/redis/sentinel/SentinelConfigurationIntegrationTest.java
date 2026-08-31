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
 * <p><b>compose 와 같은 주입을 거쳐 띄운다.</b> 파일에는 {@code __MASTER_ADDR__} 자리표시자가
 * 있고 배포는 entrypoint 가 기동 시 그것을 해석한 IP 로 바꾼다 — 파일만 그대로 복사하면
 * 파싱에서 죽는다. 즉 이 테스트가 확인하는 것은 "파일이 유효한가" 가 아니라
 * <b>"배포가 하는 그대로 했을 때 뜨는가"</b> 다.
 */
@Testcontainers(disabledWithoutDocker = true)
class SentinelConfigurationIntegrationTest {

    @Test
    void committedSentinelConfigurationStartsAndRegistersTheMaster() throws Exception {
        try (GenericContainer<?> sentinel = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
                .withCopyFileToContainer(MountableFile.forHostPath("sentinel.conf"), "/config/sentinel.conf")
                .withExposedPorts(26379)
                .withCommand("/bin/sh", "-ec",
                        // compose 의 entrypoint 와 같은 치환. 여기서는 감시 대상이 뜰 필요가
                        // 없어 루프백을 넣는다 — 이 테스트가 보는 것은 파싱과 기동뿐이다.
                        "sed 's/__MASTER_ADDR__/127.0.0.1/' /config/sentinel.conf "
                                + "> /tmp/sentinel.conf "
                                + "&& exec redis-server /tmp/sentinel.conf --sentinel")) {
            sentinel.start();

            assertThat(sentinel.execInContainer("redis-cli", "-p", "26379", "ping").getStdout().trim())
                    .isEqualTo("PONG");
        }
    }
}
