package com.kafkick.infra.redis.coupon.v2;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import com.kafkick.core.coupon.v2.query.CouponDefinitionL2CachePort;
import com.kafkick.core.coupon.v2.query.CouponDefinitionSnapshot;

import tools.jackson.databind.ObjectMapper;

/**
 * 여러 API 인스턴스가 공유하는 정의 목록 L2 와, 그 miss 를 하나로 합치는 로드 권한이다.
 *
 * <p>키는 두 개다. 값 키({@code cy:v2:definitions})와 로드 권한 키
 * ({@code cy:v2:definitions:load}). 권한은 {@code SET NX PX} 한 번으로 잡고, 반납은
 * <b>토큰이 같을 때만</b> 지운다 — lease 가 먼저 끝나 다음 인스턴스가 잡은 권한을, 늦게 끝난
 * 앞 인스턴스가 지워 버리면 락이 락이 아니게 된다.
 *
 * <p><b>이 클래스는 예외를 밖으로 내지 않는다.</b> L2 는 DB 부하를 줄이는 보조 계층이고,
 * Redis 장애가 목록 조회의 새 실패 원인이 되면 L2 를 넣기 전보다 가용성이 나빠진다.
 */
public class RedisCouponDefinitionL2Cache implements CouponDefinitionL2CachePort {

    private static final Logger log = LoggerFactory.getLogger(RedisCouponDefinitionL2Cache.class);

    static final String VALUE_KEY = "cy:v2:definitions";
    static final String LOAD_KEY = "cy:v2:definitions:load";

    private static final RedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public RedisCouponDefinitionL2Cache(StringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = Objects.requireNonNull(redis, "redis");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    @Override
    public Optional<CouponDefinitionSnapshot> find() {
        try {
            String json = redis.opsForValue().get(VALUE_KEY);
            if (json == null) {
                return Optional.empty();
            }
            return Optional.of(objectMapper.readValue(json, CouponDefinitionSnapshot.class));
        } catch (RuntimeException failure) {
            // 깨진 값도 miss 와 같게 다룬다. 여기서 던지면 조회가 Redis 에 묶인다.
            log.warn("쿠폰 정의 L2 읽기 실패 — DB 로 물러난다: {}", failure.toString());
            return Optional.empty();
        }
    }

    @Override
    public void put(CouponDefinitionSnapshot snapshot, Duration ttl) {
        try {
            redis.opsForValue().set(VALUE_KEY, objectMapper.writeValueAsString(snapshot), ttl);
        } catch (RuntimeException failure) {
            log.warn("쿠폰 정의 L2 쓰기 실패 — 이번 회차는 L1 만으로 돈다: {}", failure.toString());
        }
    }

    @Override
    public Optional<String> tryAcquireLoad(Duration lease) {
        String token = UUID.randomUUID().toString();
        try {
            return Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(LOAD_KEY, token, lease))
                    ? Optional.of(token)
                    : Optional.empty();
        } catch (RuntimeException failure) {
            // Redis 가 안 되면 공유 락도 없다. 각 인스턴스가 자기 로드를 하는 편이 503 보다 낫다.
            log.warn("쿠폰 정의 L2 로드 권한 획득 실패 — 자기 로드로 물러난다: {}", failure.toString());
            return Optional.of(token);
        }
    }

    @Override
    public void releaseLoad(String token) {
        try {
            redis.execute(RELEASE_SCRIPT, List.of(LOAD_KEY), token);
        } catch (RuntimeException failure) {
            // 반납에 실패해도 lease 가 상한이다. 다음 인스턴스는 그만큼만 늦어진다.
            log.warn("쿠폰 정의 L2 로드 권한 반납 실패 — lease 만료를 기다린다: {}", failure.toString());
        }
    }
}
