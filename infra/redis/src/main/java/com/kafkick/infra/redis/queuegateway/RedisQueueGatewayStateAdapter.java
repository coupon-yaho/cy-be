package com.kafkick.infra.redis.queuegateway;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.queuegateway.QueueGatewayCouponRoundState;
import com.kafkick.core.queuegateway.QueueGatewayStatePort;

/** 외부 대기열 게이트웨이가 소비하는 네임스페이스에 API 상태를 미러링합니다. */
public final class RedisQueueGatewayStateAdapter implements QueueGatewayStatePort {

    static final String CAPACITY_KEY = "capacity:coupon-svc:v1";
    static final String ACTIVE_COUPONS_KEY = "coupons:active";
    static final String POLICY_KEY = "coupon:policy";
    static final String SNAPSHOT_COORDINATION_KEY = "coupon-svc:queue-gateway:snapshot";
    static final String STOCK_PREFIX = "stock:{";

    private static final RedisScript<Long> PUBLISH_SCRIPT = new DefaultRedisScript<>("""
            local activeType = redis.call('TYPE', KEYS[1])['ok']
            if activeType ~= 'none' and activeType ~= 'set' then
              return redis.error_reply('invalid type for ' .. KEYS[1])
            end
            local policyType = redis.call('TYPE', KEYS[2])['ok']
            if policyType ~= 'none' and policyType ~= 'hash' then
              return redis.error_reply('invalid type for ' .. KEYS[2])
            end
            local coordinationType = redis.call('TYPE', KEYS[3])['ok']
            if coordinationType ~= 'none' and coordinationType ~= 'hash' then
              return redis.error_reply('invalid type for ' .. KEYS[3])
            end

            local incomingVersion = tonumber(ARGV[1])
            local appliedValue = redis.call('HGET', KEYS[3], 'applied')
            local appliedVersion = tonumber(appliedValue)
            if appliedValue and not appliedVersion then
              return redis.error_reply('invalid applied snapshot version')
            end
            if incomingVersion <= (appliedVersion or 0) then
              return 0
            end

            local previous = redis.call('SMEMBERS', KEYS[1])
            local incoming = {}
            local count = tonumber(ARGV[3])
            for index = 1, count do
              local offset = 4 + ((index - 1) * 3)
              local couponId = ARGV[offset]
              incoming[couponId] = true
              local stockKey = 'stock:{' .. couponId .. '}'
              local stockType = redis.call('TYPE', stockKey)['ok']
              if stockType ~= 'none' and stockType ~= 'string' then
                return redis.error_reply('invalid type for ' .. stockKey)
              end
            end
            for _, couponId in ipairs(previous) do
              local stockKey = 'stock:{' .. couponId .. '}'
              local stockType = redis.call('TYPE', stockKey)['ok']
              if stockType ~= 'none' and stockType ~= 'string' then
                return redis.error_reply('invalid type for ' .. stockKey)
              end
            end

            for _, couponId in ipairs(previous) do
              if not incoming[couponId] then
                redis.call('SREM', KEYS[1], couponId)
                redis.call('HDEL', KEYS[2], couponId)
                redis.call('DEL', 'stock:{' .. couponId .. '}')
              end
            end
            for index = 1, count do
              local offset = 4 + ((index - 1) * 3)
              local couponId = ARGV[offset]
              local hasStock = ARGV[offset + 1]
              local stock = ARGV[offset + 2]
              if hasStock == '1' then
                redis.call('SET', 'stock:{' .. couponId .. '}', stock)
              else
                redis.call('DEL', 'stock:{' .. couponId .. '}')
              end
              redis.call('HSET', KEYS[2], couponId, cjson.encode({mode = ARGV[2]}))
              redis.call('SADD', KEYS[1], couponId)
            end
            redis.call('HSET', KEYS[3], 'applied', incomingVersion)
            return count
            """, Long.class);

    private final StringRedisTemplate redis;

    public RedisQueueGatewayStateAdapter(StringRedisTemplate redis) {
        this.redis = Objects.requireNonNull(redis, "redis");
    }

    @Override
    public void reportCapacity(String instanceId, long creditsPerSecond, Instant reportedAt) {
        validateInstanceId(instanceId);
        Objects.requireNonNull(reportedAt, "reportedAt");
        if (creditsPerSecond < 0L) {
            throw new IllegalArgumentException("creditsPerSecond는 0 이상이어야 합니다.");
        }
        String value = "{\"credits\":" + creditsPerSecond
                + ",\"ts\":" + reportedAt.getEpochSecond() + "}";
        redis.opsForHash().put(CAPACITY_KEY, instanceId, value);
    }

    @Override
    public void removeCapacity(String instanceId) {
        validateInstanceId(instanceId);
        redis.opsForHash().delete(CAPACITY_KEY, instanceId);
    }

    @Override
    public long reserveCouponRoundSnapshotVersion() {
        Long version = redis.opsForHash().increment(SNAPSHOT_COORDINATION_KEY, "next", 1L);
        if (version == null || version <= 0L) {
            throw new IllegalStateException("Redis가 유효한 게이트웨이 스냅샷 버전을 반환하지 않았습니다.");
        }
        return version;
    }

    @Override
    public void publishCouponRounds(
            long snapshotVersion,
            List<QueueGatewayCouponRoundState> couponRounds,
            QueueMode queueMode
    ) {
        if (snapshotVersion <= 0L) {
            throw new IllegalArgumentException("snapshotVersion은 양수여야 합니다.");
        }
        Objects.requireNonNull(couponRounds, "couponRounds");
        Objects.requireNonNull(queueMode, "queueMode");
        List<QueueGatewayCouponRoundState> snapshot = List.copyOf(couponRounds);
        HashSet<Long> ids = new HashSet<>();
        ArrayList<String> arguments = new ArrayList<>(3 + snapshot.size() * 3);
        arguments.add(Long.toString(snapshotVersion));
        arguments.add(queueMode.name());
        arguments.add(Integer.toString(snapshot.size()));
        for (QueueGatewayCouponRoundState round : snapshot) {
            Objects.requireNonNull(round, "couponRounds element");
            if (!ids.add(round.couponId())) {
                throw new IllegalArgumentException("couponId가 중복되었습니다: " + round.couponId());
            }
            arguments.add(Long.toString(round.couponId()));
            arguments.add(round.stockStatus().carriesValue() ? "1" : "0");
            arguments.add(round.remainingStock() == null ? "" : Long.toString(round.remainingStock()));
        }
        redis.execute(PUBLISH_SCRIPT,
                List.of(ACTIVE_COUPONS_KEY, POLICY_KEY, SNAPSHOT_COORDINATION_KEY),
                arguments.toArray());
    }

    private static void validateInstanceId(String instanceId) {
        if (instanceId == null || instanceId.isBlank()) {
            throw new IllegalArgumentException("instanceId는 비어 있을 수 없습니다.");
        }
    }

    static String stockKey(long couponId) {
        return STOCK_PREFIX + couponId + "}";
    }
}
