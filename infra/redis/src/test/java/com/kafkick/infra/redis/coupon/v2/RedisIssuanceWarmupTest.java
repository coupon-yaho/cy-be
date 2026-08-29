package com.kafkick.infra.redis.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

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

import com.kafkick.core.coupon.v2.IssuedValue;
import com.kafkick.core.coupon.v2.IssuedValueCodec;
import com.kafkick.core.coupon.v2.port.RebuiltIssued;
import com.kafkick.core.coupon.v2.port.RestorationHaltStore;

/**
 * 시딩 어댑터 통합 테스트. <b>키 리터럴을 베끼지 않는다</b> — 출처는 {@link IssuanceKeys} 하나다.
 *
 * <p>여기서 보는 것은 셋이다. 값이 4필드 codec 으로 다시 읽히는가, {@code HLEN} 과
 * {@code issued_ever} 가 <b>같은 호출로</b> 맞춰지는가({@code LUA_GAP} 의 정의), 그리고
 * <b>{@code meta} 를 건드리지 않는가</b> — 여기서 게이트가 열리면 순서 계약이 무너진다.
 */
@Testcontainers(disabledWithoutDocker = true)
class RedisIssuanceWarmupTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private static final long ROUND_ID = 11;

    private static StringRedisTemplate redisTemplate;

    private final IssuedValueCodec codec = new IssuedValueCodec();
    private final IssuanceKeys keys = IssuanceKeys.of(ROUND_ID);

    private RedisIssuanceWarmup warmup;

    @BeforeAll
    static void connect() {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getFirstMappedPort()));
        factory.afterPropertiesSet();
        factory.start();
        redisTemplate = new StringRedisTemplate(factory);
    }

    @BeforeEach
    void reset() {
        redisTemplate.delete(List.of(keys.issued(), keys.issuedEver(), keys.stock(), keys.meta(),
                keys.restorationHalt()));
        warmup = new RedisIssuanceWarmup(
                redisTemplate, new RedisRestorationHaltStore(redisTemplate));
    }

    @Test
    @DisplayName("세 키를 함께 쓰고 meta 는 건드리지 않는다")
    void seedsThreeKeysAndLeavesGateClosed() {
        warmup.seedCounters(ROUND_ID, List.of(
                new RebuiltIssued(1, 1_700_000_000_000L),
                new RebuiltIssued(2, 1_700_000_000_001L)), 98);

        assertThat(redisTemplate.opsForHash().size(keys.issued())).isEqualTo(2);
        assertThat(redisTemplate.opsForValue().get(keys.issuedEver())).isEqualTo("2");
        assertThat(redisTemplate.opsForValue().get(keys.stock())).isEqualTo("98");
        assertThat(redisTemplate.hasKey(keys.meta())).isFalse();
    }

    @Test
    @DisplayName("값이 4필드 codec 으로 다시 읽히고, 상태는 D · 멱등키는 재구성 표식이다")
    void writesDoneValuesWithRebuiltMarker() {
        warmup.seedCounters(ROUND_ID, List.of(new RebuiltIssued(42, 1_700_000_000_000L)), 0);

        Object stored = redisTemplate.opsForHash().get(keys.issued(), "42");
        IssuedValue value = codec.decode((String) stored);

        assertThat(value.status()).isEqualTo(IssuedValue.Status.DONE);
        assertThat(value.claimedAtEpochMillis()).isEqualTo(1_700_000_000_000L);
        assertThat(value.idempotencyKey()).isEqualTo(RedisIssuanceWarmup.REBUILT_MARKER);
        assertThat(value.requestToken()).isEqualTo(RedisIssuanceWarmup.REBUILT_MARKER);
    }

    @Test
    @DisplayName("배치 크기를 넘겨도 HLEN 과 issued_ever 가 정확히 같다 — 이 둘의 차가 LUA_GAP 이다")
    void keepsHashLengthAndCounterEqualAcrossBatches() {
        int count = RedisIssuanceWarmup.HSET_BATCH_SIZE * 2 + 1;
        List<RebuiltIssued> members = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            members.add(new RebuiltIssued(i, 1_700_000_000_000L + i));
        }

        warmup.seedCounters(ROUND_ID, members, 0);

        assertThat(redisTemplate.opsForHash().size(keys.issued())).isEqualTo(count);
        assertThat(redisTemplate.opsForValue().get(keys.issuedEver()))
                .isEqualTo(Integer.toString(count));
    }

    @Test
    @DisplayName("앞선 워밍업이 남긴 field 를 지우고 쓴다")
    void unlinksStaleHashBeforeWriting() {
        redisTemplate.opsForHash().put(keys.issued(), "999", "P|1|stale|stale");

        warmup.seedCounters(ROUND_ID, List.of(new RebuiltIssued(1, 1L)), 5);

        assertThat(redisTemplate.opsForHash().hasKey(keys.issued(), "999")).isFalse();
        assertThat(redisTemplate.opsForHash().size(keys.issued())).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 회원이 두 번 들어오면 아무것도 쓰지 않고 거절한다")
    void rejectsDuplicateMemberWithoutWriting() {
        assertThatThrownBy(() -> warmup.seedCounters(ROUND_ID, List.of(
                new RebuiltIssued(1, 1L), new RebuiltIssued(1, 2L)), 5))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(redisTemplate.hasKey(keys.issued())).isFalse();
        assertThat(redisTemplate.hasKey(keys.issuedEver())).isFalse();
        assertThat(redisTemplate.hasKey(keys.stock())).isFalse();
    }

    /**
     * 재구성이 재고를 다시 세운 것이 곧 "어긋남을 되돌렸다" 다. 표식이 남아 있으면 그 회차
     * 만료는 <b>고친 뒤에도</b> 계속 멈춰 있고, 푸는 방법이 배치 재기동뿐이 된다.
     */
    @Test
    @DisplayName("시딩이 복원 중단 표식을 함께 푼다")
    void clearsTheRestorationHaltMark() {
        RestorationHaltStore haltStore = new RedisRestorationHaltStore(redisTemplate);
        haltStore.halt(ROUND_ID);
        assertThat(haltStore.isHalted(ROUND_ID)).isTrue();

        warmup.seedCounters(ROUND_ID, List.of(new RebuiltIssued(1, 1L)), 5);

        assertThat(haltStore.isHalted(ROUND_ID)).isFalse();
    }

    /** 표식은 회차별이다. 한 회차를 재구성해도 다른 회차의 중단은 그대로 남아야 한다. */
    @Test
    @DisplayName("표식은 회차별이고 다른 회차를 건드리지 않는다")
    void haltMarkIsPerRound() {
        RestorationHaltStore haltStore = new RedisRestorationHaltStore(redisTemplate);
        long otherRound = ROUND_ID + 1;
        try {
            haltStore.halt(ROUND_ID);
            haltStore.halt(otherRound);

            warmup.seedCounters(ROUND_ID, List.of(new RebuiltIssued(1, 1L)), 5);

            assertThat(haltStore.isHalted(ROUND_ID)).isFalse();
            assertThat(haltStore.isHalted(otherRound)).isTrue();
        } finally {
            haltStore.clear(otherRound);
        }
    }

    /**
     * 표식은 <b>스스로 만료돼야 한다.</b> 해제 경로가 재시딩 하나뿐인데, 재시딩은 이미 열린
     * 회차를 거절하고({@code GATE_ALREADY_OPEN}·{@code ROUND_ALREADY_OPENED}) {@code -2} 는
     * 열린 회차에서만 난다 — TTL 이 없으면 <b>단방향 래치</b>다. 그 회차 만료가 영구 정지하고
     * {@code active_count} 가 영영 안 줄어든다.
     *
     * <p>{@code -2} 는 이벤트가 아니라 상태라, 만료 뒤 배치가 한 청크를 다시 태워 재판정한다.
     * 어긋남이 그대로면 곧바로 다시 멈춘다.
     */
    @Test
    @DisplayName("복원 중단 표식에는 TTL 이 있다 — 해제 경로가 재시딩뿐이라 래치가 되면 안 된다")
    void theHaltMarkExpiresOnItsOwn() {
        RestorationHaltStore haltStore = new RedisRestorationHaltStore(redisTemplate);

        haltStore.halt(ROUND_ID);

        Long ttl = redisTemplate.getExpire(keys.restorationHalt());
        assertThat(ttl)
                .as("TTL 이 없으면(-1) 아무도 못 푸는 영구 정지가 된다")
                .isGreaterThan(0L)
                .isLessThanOrEqualTo(RedisRestorationHaltStore.TTL.getSeconds());
    }

    /**
     * <b>다시 멈춰도 TTL 을 밀지 않는다.</b> 표식을 세우는 경로는 만료 배치만이 아니다 —
     * 취소·사용취소도 같은 회차에서 계속 {@code -2} 를 받는다(06 이 취소는 안 멈추기로 했다).
     * 갱신하면 시간당 취소 한 건만 들어와도 만료 시각이 영원히 밀려, TTL 로 끊으려던
     * <b>단방향 래치가 그대로 성립한다.</b> 만료 시각은 최초 중단 시점 기준이다.
     */
    @Test
    @DisplayName("다시 멈춰도 TTL 을 밀지 않는다 — 취소 트래픽이 재판정을 무한정 미루면 안 된다")
    void reHaltingDoesNotPushTheDeadline() {
        RestorationHaltStore haltStore = new RedisRestorationHaltStore(redisTemplate);
        haltStore.halt(ROUND_ID);
        redisTemplate.expire(keys.restorationHalt(), java.time.Duration.ofSeconds(5));

        haltStore.halt(ROUND_ID);

        assertThat(redisTemplate.getExpire(keys.restorationHalt()))
                .as("이미 서 있으면 그대로 둔다")
                .isLessThanOrEqualTo(5L);
        assertThat(haltStore.isHalted(ROUND_ID)).isTrue();
    }
}
