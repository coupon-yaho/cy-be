package com.kafkick.infra.redis.coupon.v2;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.v2.query.CouponDefinition;
import com.kafkick.core.coupon.v2.query.CouponDefinitionSnapshot;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L2 어댑터 통합 테스트. <b>목이 증명하지 못하는 셋</b>을 실제 Redis 위에서 고정한다.
 *
 * <p>① 반납 Lua 가 실제로 돌고 토큰이 다르면 남의 권한을 지우지 않는가 — 목 앞에서는 문법이
 * 틀려도 아무 일이 안 일어나고, 실전에서는 반납이 조용히 실패해 lease 가 끝날 때까지 아무도
 * 로드하지 못한다. ② 동시에 부른 여럿 중 <b>정확히 하나</b>만 권한을 얻는가 — 이게 깨지면
 * L2 를 넣은 목적(회차당 DB 왕복 1회)이 사라진다. ③ {@code StringRedisTemplate} 의
 * serializer 를 지난 JSON 이 같은 값으로 돌아오는가 — {@code ObjectMapper} 단독 왕복은
 * 그 구간을 지나지 않는다.
 */
@Testcontainers(disabledWithoutDocker = true)
class RedisCouponDefinitionL2CacheIntegrationTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private static final Instant NOW = Instant.parse("2026-08-28T00:00:00Z");

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redisTemplate;

    private final ObjectMapper objectMapper = JsonMapper.builder().build();

    private RedisCouponDefinitionL2Cache cache;

    @BeforeAll
    static void connect() {
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getFirstMappedPort()));
        factory.afterPropertiesSet();
        factory.start();
        redisTemplate = new StringRedisTemplate(factory);
    }

    /**
     * 컨테이너보다 먼저 연결을 닫는다. 안 닫으면 Lettuce 가 <b>이미 죽은 컨테이너로 재접속</b>을
     * 시도하고, 그 실패 알림을 종료 중인 Netty 이벤트 루프에 넣다가
     * {@code RejectedExecutionException: event executor terminated} 를 ERROR 로 뱉는다.
     * 단언은 이미 끝난 뒤라 빌드는 초록인데 로그만 시뻘게진다 — 진짜 실패와 구별이 안 된다.
     *
     * <p>null 을 보는 이유는 <b>컨테이너가 안 뜬 경우</b> 때문이다. 그때는 {@code connect()} 가
     * {@code factory} 에 대입하기 전에 죽고, JUnit 은 그래도 이 메서드를 부른다. 무조건
     * {@code destroy()} 하면 NPE 가 원래 실패에 suppressed 로 얹혀 리포트에서 원인을 찾기
     * 어려워진다 — 컨테이너 기동 실패는 이 저장소 CI 에서 실제로 나는 일이다.
     */
    @AfterAll
    static void disconnect() {
        if (factory != null) {
            factory.destroy();
        }
    }

    @BeforeEach
    void reset() {
        redisTemplate.delete(List.of(
                RedisCouponDefinitionL2Cache.VALUE_KEY, RedisCouponDefinitionL2Cache.LOAD_KEY));
        cache = new RedisCouponDefinitionL2Cache(redisTemplate, objectMapper);
    }

    @Test
    @DisplayName("템플릿 serializer 를 지나도 정의가 같은 값으로 돌아온다")
    void roundTripsThroughTheRealTemplate() {
        cache.put(snapshot(), Duration.ofSeconds(30));

        assertThat(cache.find()).contains(snapshot());
    }

    @Test
    @DisplayName("빈 목록도 miss 와 구별된다 — 회차가 없는 상태를 캐시하지 못하면 매번 DB 로 간다")
    void distinguishesAnEmptyListFromAMiss() {
        assertThat(cache.find()).isEmpty();

        cache.put(new CouponDefinitionSnapshot(List.of(), NOW.plusSeconds(60)), Duration.ofSeconds(30));

        assertThat(cache.find()).isPresent()
                .get().satisfies(shared -> assertThat(shared.definitions()).isEmpty());
    }

    @Test
    @DisplayName("TTL 이 실제로 걸린다 — 안 걸리면 회차가 끝나도 옛 목록이 영원히 남는다")
    void appliesTheTimeToLive() {
        cache.put(snapshot(), Duration.ofSeconds(30));

        assertThat(redisTemplate.getExpire(
                RedisCouponDefinitionL2Cache.VALUE_KEY, TimeUnit.SECONDS))
                .isBetween(1L, 30L);
    }

    @Test
    @DisplayName("동시에 부른 32개 중 정확히 하나만 로드 권한을 얻는다")
    void grantsTheLoadPermitToExactlyOneCaller() throws Exception {
        int callers = 32;
        CountDownLatch ready = new CountDownLatch(callers);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(callers);
        try {
            List<Future<Optional<String>>> results = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                results.add(executor.submit((Callable<Optional<String>>) () -> {
                    ready.countDown();
                    assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                    return cache.tryAcquireLoad(Duration.ofSeconds(3));
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            long granted = 0;
            for (Future<Optional<String>> result : results) {
                if (result.get(5, TimeUnit.SECONDS).isPresent()) {
                    granted++;
                }
            }
            assertThat(granted).as("여럿이 얻으면 그 수만큼 DB 로 간다 — L2 를 넣은 이유가 사라진다")
                    .isEqualTo(1L);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("반납 Lua 가 실제로 돌아 권한을 지운다 — 안 돌면 lease 3초 동안 아무도 로드 못 한다")
    void releasesThePermitSoTheNextLoadCanStart() {
        String token = cache.tryAcquireLoad(Duration.ofSeconds(30)).orElseThrow();

        cache.releaseLoad(token);

        assertThat(redisTemplate.hasKey(RedisCouponDefinitionL2Cache.LOAD_KEY)).isFalse();
        assertThat(cache.tryAcquireLoad(Duration.ofSeconds(30))).isPresent();
    }

    @Test
    @DisplayName("남의 권한은 지우지 않는다 — lease 가 먼저 끝난 앞 인스턴스가 지우면 락이 락이 아니다")
    void doesNotReleaseAPermitHeldByAnotherInstance() {
        String mine = cache.tryAcquireLoad(Duration.ofSeconds(30)).orElseThrow();
        cache.releaseLoad(mine);
        String theirs = cache.tryAcquireLoad(Duration.ofSeconds(30)).orElseThrow();

        // lease 만료로 권한을 놓친 인스턴스가 뒤늦게 반납을 시도하는 상황이다.
        cache.releaseLoad(mine);

        assertThat(redisTemplate.opsForValue().get(RedisCouponDefinitionL2Cache.LOAD_KEY))
                .isEqualTo(theirs);
    }

    @Test
    @DisplayName("깨진 값은 miss 로 다룬다 — 여기서 던지면 Redis 오염이 곧 목록 503 이다")
    void treatsACorruptStoredValueAsAMiss() {
        redisTemplate.opsForValue().set(RedisCouponDefinitionL2Cache.VALUE_KEY, "{not json");

        assertThat(cache.find()).isEmpty();
    }

    private static CouponDefinitionSnapshot snapshot() {
        return new CouponDefinitionSnapshot(List.of(new CouponDefinition(
                7L, 2L, "영화 할인", CouponPolicyType.FIXED_AMOUNT, null, null, 3_000, 30,
                NOW.minusSeconds(1), NOW.plusSeconds(60), CouponRoundStatus.OPEN)),
                NOW.plusSeconds(60));
    }
}
