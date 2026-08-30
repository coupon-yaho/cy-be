package com.kafkick.infra.redis;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 한 Docker 머신 안의 Sentinel 검증이다. 머신 장애의 가용성을 증명하지 않는다. master를 멈춘 뒤
 * Sentinel이 새 master를 알리고 그 주소에서 쓰기가 재개되는 코드 경로만 검증한다.
 *
 * <p><b>커밋된 {@code sentinel.conf} 를 그대로 태운다.</b> 테스트가 설정을 따로 만들면
 * 배포가 쓰는 것과 다른 것을 검증하게 된다 — 실제 파일은 master 를 <b>호스트명</b>으로
 * 감시하고 {@code resolve-hostnames}·{@code announce-hostnames} 를 켜는데, IP 로 감시하는
 * 사본은 그 해석 경로를 한 번도 지나지 않는다. 그래서 master 컨테이너의 별칭을
 * {@code redis} 로 두어 파일의 {@code sentinel monitor coupon-master redis 6379 2} 가
 * 그대로 성립하게 하고, 테스트 시간을 줄이는 {@code down-after} 한 줄만 sed 로 바꾼다.
 */
@Testcontainers(disabledWithoutDocker = true)
class SentinelFailoverIntegrationTest {

    private static final DockerImageName REDIS = DockerImageName.parse("redis:7.4-alpine");
    private static final Network NETWORK = Network.newNetwork();
    private static GenericContainer<?> master;
    private static GenericContainer<?> replica1;
    private static GenericContainer<?> replica2;
    private static GenericContainer<?> sentinel1;
    private static GenericContainer<?> sentinel2;
    private static GenericContainer<?> sentinel3;
    private static GenericContainer<?> client;
    private static String masterAddress;

    @BeforeAll
    static void startTopology() {
        // 별칭이 compose 의 서비스명과 같아야 커밋된 sentinel.conf 의 호스트명 감시가 성립한다.
        master = redisContainer("redis", "redis-server", "--appendonly", "yes",
                "--maxmemory-policy", "noeviction");
        master.start();
        masterAddress = master.getContainerInfo().getNetworkSettings().getNetworks().values().iterator()
                .next().getIpAddress();
        replica1 = redisContainer("redis-replica-1", "redis-server", "--replicaof", "redis", "6379",
                "--appendonly", "yes", "--maxmemory-policy", "noeviction");
        replica2 = redisContainer("redis-replica-2", "redis-server", "--replicaof", "redis", "6379",
                "--appendonly", "yes", "--maxmemory-policy", "noeviction");
        replica1.start();
        replica2.start();
        sentinel1 = sentinel("sentinel-1");
        sentinel2 = sentinel("sentinel-2");
        sentinel3 = sentinel("sentinel-3");
        sentinel1.start();
        sentinel2.start();
        sentinel3.start();
        client = new GenericContainer<>(REDIS)
                .withNetwork(NETWORK)
                .withNetworkAliases("issuance-client")
                .withCommand("/bin/sh", "-c", "while :; do sleep 3600; done");
        client.start();
    }

    @AfterAll
    static void stopTopology() {
        for (GenericContainer<?> container : new GenericContainer<?>[] {
                client, sentinel3, sentinel2, sentinel1, replica2, replica1, master
        }) {
            if (container != null) container.stop();
        }
        NETWORK.close();
    }

    @Test
    void promotesAReplicaAcceptsWritesAndBootstrapsMissingRuntimeConfig() throws Exception {
        String initialMaster = awaitMasterAddress(null);
        redisCommand(initialMaster, "SET", "before-failover", "1");
        // 승격 대상이 없는 상태로 죽이면 failover 가 아니라 그냥 정지다. compose 의
        // healthcheck 가 막는 바로 그 상태를 여기서도 막는다 — 안 막으면 이 테스트는
        // "승격 실패"를 타임아웃으로만 보고 플레이키가 된다.
        awaitReplicaCount(2);

        master.stop(); // docker stop과 같은 SIGTERM 경로

        String promoted = awaitDifferentMaster(initialMaster);
        assertThat(redisCommand(promoted, "SET", "after-failover", "1")).isEqualTo("OK");
        assertThat(redisCommand(promoted, "GET", "after-failover")).isEqualTo("1");

        // RuntimeConfigBootstrap의 원자 계약(SET NX)을 새 master에서 같은 방식으로 실행한다.
        redisCommand(promoted, "DEL", "config:runtime");
        assertThat(redisCommand(promoted, "SET", "config:runtime", "bootstrap", "NX")).isEqualTo("OK");
        assertThat(redisCommand(promoted, "GET", "config:runtime")).isEqualTo("bootstrap");
    }

    private static GenericContainer<?> redisContainer(String alias, String... command) {
        return new GenericContainer<>(REDIS).withNetwork(NETWORK).withNetworkAliases(alias).withCommand(command);
    }

    private static GenericContainer<?> sentinel(String alias) {
        // 배포가 쓰는 파일을 복사한다. 바꾸는 것은 down-after 하나뿐이다 — 5초를 그대로 두면
        // 테스트가 매번 그만큼 더 걸린다. 나머지(호스트명 감시·resolve/announce-hostnames·
        // failover-timeout·parallel-syncs)는 손대지 않아야 검증하는 의미가 있다.
        String config = "sed 's/^sentinel down-after-milliseconds coupon-master .*/"
                + "sentinel down-after-milliseconds coupon-master 1000/' /config/sentinel.conf"
                + " > /tmp/sentinel.conf && exec redis-server /tmp/sentinel.conf --sentinel";
        return new GenericContainer<>(REDIS).withNetwork(NETWORK).withNetworkAliases(alias)
                .withCopyFileToContainer(
                        MountableFile.forHostPath("sentinel.conf"), "/config/sentinel.conf")
                .withCommand("/bin/sh", "-ec", config);
    }

    private static String awaitMasterAddress(String expected) throws Exception {
        String address = "";
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            String[] lines = redisCommandAtPort("sentinel-1", "26379", "SENTINEL", "get-master-addr-by-name", "coupon-master")
                    .split("\\R");
            if (lines.length > 0 && !lines[0].isBlank()) {
                address = lines[0].trim();
                if (expected == null || expected.equals(address)) return address;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Sentinel master address did not become " + expected + ": " + address);
    }

    /** Sentinel 이 replica 를 몇 대 인지했는지. compose healthcheck 와 같은 술어를 쓴다. */
    private static void awaitReplicaCount(int expected) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        long seen = -1;
        while (System.nanoTime() < deadline) {
            seen = redisCommandAtPort("sentinel-1", "26379", "SENTINEL", "replicas", "coupon-master")
                    .lines().map(String::trim).filter("ip"::equals).count();
            if (seen >= expected) return;
            Thread.sleep(100);
        }
        throw new AssertionError("Sentinel 이 replica " + expected + "대를 인지하지 못했다: " + seen);
    }

    private static String awaitDifferentMaster(String oldMaster) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            String candidate = awaitMasterAddress(null);
            if (!oldMaster.equals(candidate)) return candidate;
            Thread.sleep(200);
        }
        throw new AssertionError("Sentinel did not promote a replica");
    }

    private static String redisCommand(String host, String... args) throws Exception {
        return redisCommandAtPort(host, "6379", args);
    }

    private static String redisCommandAtPort(String host, String port, String... args) throws Exception {
        String[] command = new String[args.length + 6];
        command[0] = "redis-cli";
        command[1] = "-h";
        command[2] = host;
        command[3] = "-p";
        command[4] = port;
        command[5] = "--raw";
        System.arraycopy(args, 0, command, 6, args.length);
        GenericContainer.ExecResult result = client.execInContainer(command);
        assertThat(result.getExitCode()).isZero();
        return result.getStdout().trim();
    }
}
