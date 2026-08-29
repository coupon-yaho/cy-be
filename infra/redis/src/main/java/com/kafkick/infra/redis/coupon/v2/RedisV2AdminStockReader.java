package com.kafkick.infra.redis.coupon.v2;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.admin.stock.AdminStockSnapshot;
import com.kafkick.core.admin.stock.V2AdminStockReader;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/** Redis의 V2 게이트 meta와 잔여 재고를 관리자 재고 관측으로 변환합니다. */
public final class RedisV2AdminStockReader implements V2AdminStockReader {

    private static final Logger log = LoggerFactory.getLogger(RedisV2AdminStockReader.class);
    private static final long MISSING = 0L;
    private static final long VALID = 1L;

    private static final RedisScript<List> READ_SCRIPT = new DefaultRedisScript<>("""
            local stockType = redis.call('TYPE', KEYS[1])['ok']
            local metaType = redis.call('TYPE', KEYS[2])['ok']
            if stockType == 'none' and metaType == 'none' then return {0} end
            if stockType ~= 'string' or metaType ~= 'hash' then return {-1} end
            local stock = redis.call('GET', KEYS[1])
            local meta = redis.call('HMGET', KEYS[2], 'status', 'totalQuantity')
            if not stock or not meta[1] or not meta[2] then return {-1} end
            local function canonicalNonNegative(value)
              return value == '0' or string.match(value, '^[1-9][0-9]*$') ~= nil
            end
            if not canonicalNonNegative(stock) or not canonicalNonNegative(meta[2]) then return {-1} end
            return {1, stock, meta[1], meta[2]}
            """, List.class);

    private final StringRedisTemplate redisTemplate;

    /** 관리자 조회 전용 Redis 통로를 주입받습니다. */
    public RedisV2AdminStockReader(StringRedisTemplate redisTemplate) {
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate");
    }

    /**
     * 회차마다 meta와 stock을 한 Lua에서 읽어 부분 시점 조합을 막고, 실패는 회차별 상태로 보존합니다.
     * Redis 통신 자체가 실패하면 요청한 모든 V2 회차를 UNAVAILABLE로 반환합니다.
     */
    @Override
    public Map<Long, CouponMetricsSource.Observation<AdminStockSnapshot>> read(
            List<Request> requests,
            Instant observedAt
    ) {
        Objects.requireNonNull(requests, "requests");
        Objects.requireNonNull(observedAt, "observedAt");
        LinkedHashMap<Long, CouponMetricsSource.Observation<AdminStockSnapshot>> results =
                new LinkedHashMap<>();
        try {
            List<Request> snapshotRequests = List.copyOf(requests);
            List<Object> rawResults = redisTemplate.executePipelined(new SessionCallback<>() {
                /** 각 회차 Lua를 한 pipeline에 넣어 회차별 원자성과 한 번의 네트워크 왕복을 함께 지킵니다. */
                @Override
                @SuppressWarnings({"rawtypes", "unchecked"})
                public Object execute(RedisOperations operations) {
                    for (Request request : snapshotRequests) {
                        IssuanceKeys keys = IssuanceKeys.of(request.couponId());
                        operations.execute(READ_SCRIPT, List.of(keys.stock(), keys.meta()));
                    }
                    return null;
                }
            });
            if (rawResults.size() != snapshotRequests.size()) {
                throw new IllegalStateException("V2 관리자 재고 pipeline 응답 수가 요청 수와 다릅니다.");
            }
            for (int index = 0; index < snapshotRequests.size(); index++) {
                Request request = snapshotRequests.get(index);
                Object raw = rawResults.get(index);
                results.put(request.couponId(), map(
                        request, raw instanceof List<?> values ? values : null, observedAt));
            }
            return Map.copyOf(results);
        } catch (RuntimeException exception) {
            log.warn("V2 관리자 Redis 재고 조회에 실패했습니다: requestCount={}, exceptionType={}",
                    requests.size(), exception.getClass().getSimpleName(), exception);
            for (Request request : requests) {
                results.put(request.couponId(), unavailable());
            }
            return Map.copyOf(results);
        }
    }

    /** Lua의 값·상태 코드를 검증된 관리자 재고 관측으로 변환합니다. */
    private static CouponMetricsSource.Observation<AdminStockSnapshot> map(
            Request request,
            List<?> raw,
            Instant observedAt
    ) {
        if (raw == null || raw.isEmpty()) {
            return unavailable();
        }
        long code = number(raw.getFirst());
        if (code == MISSING) {
            // 예약 회차는 아직 워밍업될 수 있지만 OPEN/CLOSED 회차의 키 부재는 이미 운영 장애입니다.
            return request.campaignStatus() == CouponRoundStatus.SCHEDULED
                    ? pending() : unavailable();
        }
        if (code != VALID || raw.size() != 4) {
            return unavailable();
        }
        try {
            long remaining = number(raw.get(1));
            String gateStatus = raw.get(2).toString();
            long total = number(raw.get(3));
            if (!("OPEN".equals(gateStatus) || "CLOSED".equals(gateStatus))
                    || total != request.expectedTotalQuantity()
                    || remaining < 0L
                    || remaining > total) {
                return unavailable();
            }
            return new CouponMetricsSource.Observation<>(
                    new AdminStockSnapshot(total, remaining), SourceStatus.VALID, observedAt);
        } catch (RuntimeException exception) {
            return unavailable();
        }
    }

    /** Redis Lua 반환값이 정수 타입 또는 정수 문자열인지 확인합니다. */
    private static long number(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }

    /** 아직 만들어져야 할 값이 없는 관측입니다. */
    private static CouponMetricsSource.Observation<AdminStockSnapshot> pending() {
        return new CouponMetricsSource.Observation<>(null, SourceStatus.PENDING, null);
    }

    /** 장애나 파손 때문에 신뢰할 값을 만들 수 없는 관측입니다. */
    private static CouponMetricsSource.Observation<AdminStockSnapshot> unavailable() {
        return new CouponMetricsSource.Observation<>(null, SourceStatus.UNAVAILABLE, null);
    }
}
