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

    /**
     * 회차 스냅샷을 한 번에 바꾼다. <b>도중에 물러서지 않고 통째로 거부한다.</b>
     *
     * <h2>거부 조건 — 전부 {@code redis.error_reply} 로 이 공급을 중단한다</h2>
     *
     * <ul>
     *   <li>{@code coupons:active} 가 set 이 아니다</li>
     *   <li>{@code coupon:policy} 가 hash 가 아니다</li>
     *   <li>스냅샷 조정 키가 hash 가 아니거나 그 {@code applied} 가 수로 안 읽힌다</li>
     *   <li><b>{@code stock:&#123;회차&#125;} 가 string 이 아니다</b> — 이번 스냅샷에 든
     *       회차와 <b>직전 스냅샷에 있던 회차</b>를 모두 검사한다. 쓰기 전에 훑으므로
     *       한 회차가 걸리면 나머지도 안 바뀐다</li>
     * </ul>
     *
     * <p><b>거부되면 기존 스냅샷은 그대로 남고</b> 호출부는 예외를 받는다. 잘못된 값으로
     * 덮어쓰는 것보다 낫다.
     *
     * <p><b>증상을 정확히 적는다.</b> 이 실패는 <b>회차·재고·정책 스냅샷만</b> 멈춘다.
     * 가용량 보고는 별도 스케줄러가 {@code reportCapacity} 로 계속 쓰므로
     * <i>"가용량 보고가 없다"</i> 는 경고는 <b>안 뜬다.</b> 보이는 것은 회차 목록이 낡는
     * 것뿐이라, 그 경고를 기다리면 아무 신호도 못 받는다.
     *
     * <p><b>운영에서 이걸 만나는 경우는 하나다</b> — 그 키들을 다른 형식으로 쓰던 Redis 에
     * 이 공급기를 붙였을 때다. 그때는 위 네 부류 키의 {@code TYPE} 을 먼저 본다.
     *
     * <p>버전이 같거나 낮으면 거부가 아니라 {@code 0} 을 돌려주고 아무것도 안 바꾼다.
     * 늦게 도착한 스냅샷이 새 것을 덮지 않게 하는 것이라 실패가 아니다.
     *
     * <h2>재고를 모를 때는 키를 지운다</h2>
     *
     * <p>활성 회차인데 재고를 못 읽었으면(V2 조회 실패 등) <b>낡은 값을 남기지 않고
     * 키를 지운다.</b> 게이트웨이가 <b>키 없음을 매진이 아니라 미상으로</b> 읽기 때문이다 —
     * {@code AllocationRedisPort.stocks} 가 <i>"못 읽으면 담지 않는다 … 빠진 자리를 0 으로
     * 접으면 재고 키를 잃은 쿠폰이 매진이 된다"</i> 고 적고 그렇게 구현돼 있다.
     *
     * <p>반대로 낡은 값을 남기면 그쪽은 그것을 <b>아는 값</b>으로 취급한다. 모르는 것을
     * 모른다고 말하는 편이 맞다.
     */
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
                -- **모름은 키를 지워서 말한다.** 게이트웨이가 키 없음을 매진이 아니라
                -- 미상으로 읽는다(cy-waiting AllocationRedisPort.stocks: "못 읽으면 담지
                -- 않는다 … 0 으로 접으면 재고 키를 잃은 쿠폰이 매진이 된다").
                -- 낡은 값을 남기면 그쪽이 그것을 아는 값으로 취급한다.
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
