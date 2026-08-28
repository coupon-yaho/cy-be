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
        redisTemplate.delete(List.of(keys.issued(), keys.issuedEver(), keys.stock(), keys.meta()));
        warmup = new RedisIssuanceWarmup(redisTemplate);
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
}
