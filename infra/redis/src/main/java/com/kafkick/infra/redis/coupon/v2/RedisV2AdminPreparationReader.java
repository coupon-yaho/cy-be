package com.kafkick.infra.redis.coupon.v2;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import com.kafkick.core.admin.preparation.V2AdminPreparationReader;
import com.kafkick.core.admin.preparation.V2PreparationSource;
import com.kafkick.core.coupon.v2.port.GateStatus;
import com.kafkick.core.observation.SourceStatus;

/** Redis의 V2 워밍업 네 키와 게이트 meta를 관리자 준비 관측으로 변환합니다. */
public final class RedisV2AdminPreparationReader implements V2AdminPreparationReader {

    private static final Logger log = LoggerFactory.getLogger(RedisV2AdminPreparationReader.class);
    private static final long MISSING = 0L;
    private static final long VALID = 1L;

    private static final RedisScript<List> READINESS_SCRIPT = new DefaultRedisScript<>("""
            local stockType = redis.call('TYPE', KEYS[1])['ok']
            local issuedType = redis.call('TYPE', KEYS[2])['ok']
            local metaType = redis.call('TYPE', KEYS[3])['ok']
            local issuedEverType = redis.call('TYPE', KEYS[4])['ok']

            if stockType == 'none' and issuedType == 'none'
                and metaType == 'none' and issuedEverType == 'none' then
              return {0}
            end

            local function canonicalNonNegative(value)
              return value ~= false
                  and (value == '0' or string.match(value, '^[1-9][0-9]*$') ~= nil)
            end

            local warmupReady = 1
            if stockType ~= 'string' or issuedEverType ~= 'string'
                or (issuedType ~= 'hash' and issuedType ~= 'none') then
              warmupReady = 0
            else
              local stock = redis.call('GET', KEYS[1])
              local issuedEver = redis.call('GET', KEYS[4])
              if not canonicalNonNegative(stock) or not canonicalNonNegative(issuedEver) then
                warmupReady = 0
              else
                local issuedSize = issuedType == 'hash' and redis.call('HLEN', KEYS[2]) or 0
                if tonumber(stock) > tonumber(ARGV[4])
                    or issuedSize ~= tonumber(issuedEver)
                    or (issuedType == 'none' and issuedEver ~= '0') then
                  warmupReady = 0
                end
              end
            end

            local gateReady = 1
            if metaType ~= 'hash' then
              gateReady = 0
            else
              local meta = redis.call('HMGET', KEYS[3], '%s', '%s', '%s', '%s', '%s')
              if not meta[1] or not meta[2] or not meta[3] or not meta[4] or not meta[5]
                  or meta[1] ~= '%s'
                  or meta[2] ~= ARGV[1]
                  or meta[3] ~= ARGV[2]
                  or meta[4] ~= ARGV[3]
                  or meta[5] ~= ARGV[4] then
                gateReady = 0
              end
            end

            return {1, warmupReady, gateReady}
            """.formatted(
            RedisIssuanceGate.META_STATUS,
            RedisIssuanceGate.META_OPEN_AT,
            RedisIssuanceGate.META_CLOSE_AT,
            RedisIssuanceGate.META_GRADE_MASK,
            RedisIssuanceGate.META_TOTAL_QUANTITY,
            GateStatus.OPEN.wireValue()), List.class);

    private final StringRedisTemplate redisTemplate;

    /** 관리자 조회 전용 Redis 통로를 주입받습니다. */
    public RedisV2AdminPreparationReader(StringRedisTemplate redisTemplate) {
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate");
    }

    /**
     * 회차마다 네 키를 한 Lua에서 검증하고 회차별 결과를 요청 순서의 불변 Map으로 반환합니다.
     * Redis 통신이나 pipeline 프레임이 실패하면 요청한 모든 회차를 UNAVAILABLE로 반환합니다.
     */
    @Override
    public Map<Long, V2PreparationSource> read(List<Request> requests, Instant observedAt) {
        Objects.requireNonNull(requests, "requests");
        Objects.requireNonNull(observedAt, "observedAt");
        List<Request> snapshotRequests = List.copyOf(requests);
        if (snapshotRequests.isEmpty()) {
            return Map.of();
        }

        LinkedHashMap<Long, V2PreparationSource> results = new LinkedHashMap<>();
        try {
            List<Object> rawResults = redisTemplate.executePipelined(new SessionCallback<>() {
                /** 회차별 원자 판정을 한 pipeline에 넣어 네트워크 왕복만 합칩니다. */
                @Override
                @SuppressWarnings({"rawtypes", "unchecked"})
                public Object execute(RedisOperations operations) {
                    for (Request request : snapshotRequests) {
                        IssuanceKeys keys = IssuanceKeys.of(request.couponId());
                        operations.execute(
                                READINESS_SCRIPT,
                                List.of(keys.stock(), keys.issued(), keys.meta(), keys.issuedEver()),
                                Long.toString(request.opensAt().toEpochMilli()),
                                Long.toString(request.closesAt().toEpochMilli()),
                                Integer.toString(request.expectedGradeMask()),
                                Long.toString(request.expectedTotalQuantity()));
                    }
                    return null;
                }
            });
            if (rawResults == null || rawResults.size() != snapshotRequests.size()) {
                throw new IllegalStateException("V2 관리자 준비 pipeline 응답 수가 요청 수와 다릅니다.");
            }
            for (int index = 0; index < snapshotRequests.size(); index++) {
                Request request = snapshotRequests.get(index);
                Object raw = rawResults.get(index);
                results.put(request.couponId(), map(
                        raw instanceof List<?> values ? values : null, observedAt));
            }
            return immutableCopy(results);
        } catch (RuntimeException exception) {
            log.warn("V2 관리자 Redis 준비 조회에 실패했습니다: requestCount={}, exceptionType={}",
                    snapshotRequests.size(), exception.getClass().getSimpleName(), exception);
            results.clear();
            // batch 경계 실패 뒤 일부 이전 결과를 남기면 같은 snapshot 안에서 신뢰 수준이 갈립니다.
            for (Request request : snapshotRequests) {
                results.put(request.couponId(), V2PreparationSource.unavailable());
            }
            return immutableCopy(results);
        }
    }

    /** Lua의 상태 코드와 두 0·1 플래그를 검증된 V2 준비 원천으로 변환합니다. */
    private static V2PreparationSource map(List<?> raw, Instant observedAt) {
        if (raw == null || raw.isEmpty()) {
            return V2PreparationSource.unavailable();
        }
        try {
            long code = number(raw.getFirst());
            if (code == MISSING && raw.size() == 1) {
                return new V2PreparationSource(null, null, SourceStatus.PENDING, null);
            }
            if (code != VALID || raw.size() != 3) {
                return V2PreparationSource.unavailable();
            }
            long warmupReady = number(raw.get(1));
            long gateReady = number(raw.get(2));
            if (!isBooleanFlag(warmupReady) || !isBooleanFlag(gateReady)) {
                // 알 수 없는 Lua 값은 새 계약 신호일 수 있으므로 false로 축약하지 않습니다.
                return V2PreparationSource.unavailable();
            }
            return new V2PreparationSource(
                    warmupReady == 1L, gateReady == 1L, SourceStatus.VALID, observedAt);
        } catch (RuntimeException exception) {
            return V2PreparationSource.unavailable();
        }
    }

    /** Redis Lua 반환값이 정수 타입 또는 정수 문자열인지 확인합니다. */
    private static long number(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }

    /** Lua 준비 플래그가 계약한 0 또는 1인지 확인합니다. */
    private static boolean isBooleanFlag(long value) {
        return value == 0L || value == 1L;
    }

    /** 요청 순서를 보존하면서 호출자가 결과를 변경할 수 없는 Map을 만듭니다. */
    private static Map<Long, V2PreparationSource> immutableCopy(
            LinkedHashMap<Long, V2PreparationSource> source
    ) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }
}
