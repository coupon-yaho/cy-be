package com.kafkick.infra.redis.sentinel;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class SentinelComposeContractTest {

    @Test
    void sentinelCanResolveTheComposeMasterNameAndAppsWaitForHealthyQuorum() throws Exception {
        String config = Files.readString(Path.of("sentinel.conf"));
        String compose = Files.readString(Path.of("../../compose.yml"));
        String environment = Files.readString(Path.of("../../.env.example"));

        assertThat(config).contains(
                "sentinel monitor coupon-master redis 6379 2",
                "sentinel resolve-hostnames yes",
                "sentinel announce-hostnames yes",
                "sentinel down-after-milliseconds coupon-master 5000");
        assertThat(compose).contains(
                "coupon-redis-replica-1-data:/data",
                "coupon-redis-replica-2-data:/data",
                "redis-sentinel-1:\n        condition: service_healthy",
                "redis-sentinel-2:\n        condition: service_healthy",
                "redis-sentinel-3:\n        condition: service_healthy")
                .doesNotContain("COUPON_V2_SENTINEL_STARTUP_CLOSE_ENABLED")
                // Sentinel·replica 는 인증이 없다. 호스트로 열면 그대로 무인증 엔드포인트가 된다.
                .doesNotContain("26379:26379")
                // 복제 역할을 명령줄에 박으면 재기동이 Sentinel 이 기록한 역할을 덮어써,
                // failover 뒤 승격본이 옛 master 에 full sync 되어 승격 이후의 쓰기가 사라진다.
                // config:runtime 은 DB 재구성 대상이 아니라 그대로 유실된다.
                .doesNotContain("--replicaof");
        // 세 데이터 노드는 자기 역할을 볼륨의 conf 에 남기고, Sentinel 도 승격을 기억한다.
        // 셋이 같은 토폴로지를 읽어야 재기동이 어긋나지 않는다.
        assertThat(compose).contains(
                "echo \"replicaof redis 6379\" > /data/redis.conf",
                "coupon-redis-sentinel-1-data:/data",
                "coupon-redis-sentinel-2-data:/data",
                "coupon-redis-sentinel-3-data:/data");
        // healthcheck 는 PING 이 아니라 감시 상태를 묻는다. PING 은 master·replica 를 하나도
        // 인지하지 못한 Sentinel 도 통과시켜, 승격 대상이 없는 상태로 api 를 기동시킨다.
        // replica 는 띄운 수(2)만큼 요구한다 — 1대만 인지하면 승격 뒤 남는 replica 가 0 이다.
        assertThat(compose).contains(
                "sentinel master coupon-master | grep -q \"^coupon-master$$\"",
                "sentinel replicas coupon-master | grep -c \"^ip$$\")\" -ge 2",
                // 서로를 모르면 정족수를 못 채워 승격 자체가 없다 — master·replica 만 보면
                // 그 창이 healthy 로 통과한다.
                "sentinel sentinels coupon-master | grep -c \"^name$$\")\" -ge 2");
        // Redis 인증은 배선돼 있지 않다. 한쪽만 넣으면 "인증이 걸렸다"는 오해가 생기고,
        // 그 상태로 경계를 낮추면 같은 네트워크 누구나 재고 키를 고칠 수 있다.
        // 셋(master requirepass · replica masterauth · sentinel auth-pass)을 함께 넣는 날
        // 이 단언을 그 전부를 확인하는 것으로 바꾼다.
        assertThat(compose).doesNotContain("--requirepass");
        // 미배선 사실의 정본은 설계 문서 한 곳이다. 세 파일이 각자 설명을 들고 있으면
        // 배선하는 날 한 곳만 고쳐지고 나머지가 남아 다음 사람이 반대로 읽는다.
        String design = Files.readString(Path.of("../../docs/12-v2-redis-design.md"));
        assertThat(design).contains(
                "### 6.1.2 인증 — 배선하지 않았다 (정본)",
                // 재기동이 승격을 유실하지 않는다는 근거도 같은 문서가 정본이다.
                "### 6.1.1 재기동 — 역할은 파일이 기억한다");
        assertThat(compose).contains("docs/12-v2-redis-design.md §6.1.2");
        assertThat(config).contains("docs/12-v2-redis-design.md §6.1.2");
        assertThat(environment).contains("docs/12-v2-redis-design.md §6.1.2");
        // 주석은 세는 대상이 아니다 — 지시어로 살아 있는 줄만 본다.
        assertThat(config.lines().map(String::strip).filter(line -> !line.startsWith("#")))
                .noneMatch(line -> line.startsWith("sentinel auth-pass"));
        // 데이터 노드는 자동 복귀하지 않는다. 승격 뒤 옛 master 가 되살아나면 command 에
        // replicaof 가 없어 master 로 뜨고, replica 는 명령줄의 replicaof 로 강등된 노드를
        // 다시 따라간다. 어느 쪽이든 Sentinel 이 정한 토폴로지를 덮어써 선점이 갈린다.
        assertThat(compose).contains(
                "  redis:\n    image: redis:7.4-alpine\n    restart: \"no\"",
                "  redis-replica-1:\n    image: redis:7.4-alpine\n    restart: \"no\"",
                "  redis-replica-2:\n    image: redis:7.4-alpine\n    restart: \"no\"");
        assertThat(environment).contains(
                "REDIS_CB_SLIDING_WINDOW_SIZE=",
                "REDIS_CB_MINIMUM_NUMBER_OF_CALLS=",
                "REDIS_CB_FAILURE_RATE_THRESHOLD=",
                "REDIS_CB_WAIT_DURATION_IN_OPEN_STATE=");
    }
}
