package com.kafkick.infra.redis.runtimeconfig;

import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.RuntimeConfigCommand;
import com.kafkick.core.runtimeconfig.RuntimeConfigErrorCode;
import com.kafkick.core.runtimeconfig.RuntimeConfigRevisionConflictException;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;
import com.kafkick.core.support.exception.BusinessException;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public final class RedisRuntimeConfigStore implements RuntimeConfigStore {

    private static final String CONFIG_KEY = "config:runtime";
    private static final String AUDIT_KEY = "config:runtime:audit";
    private static final String REQUEST_ID = "requestId";
    private static final long CAS_SUCCESS = -1;
    private static final long CONFIG_MISSING = -2;
    private static final long CONFIG_CORRUPTED = -3;
    // 저장(LTRIM)과 불확정 결과 확인(range)이 같은 범위를 봐야 한다. 상수 하나로 묶어 어긋나지 않게 한다.
    private static final int AUDIT_RETENTION = 1000;
    private static final RedisScript<Long> CAS_SCRIPT = new DefaultRedisScript<>("""
            local configType = redis.call('TYPE', KEYS[1])['ok']
            if configType ~= 'none' and configType ~= 'string' then
              return -3
            end
            local auditType = redis.call('TYPE', KEYS[2])['ok']
            if auditType ~= 'none' and auditType ~= 'list' then
              return -3
            end
            local cur = redis.call('GET', KEYS[1])
            if cur then
              local decoded = {pcall(cjson.decode, cur)}
              if not decoded[1] then
                return -3
              end
              local currentConfig = decoded[2]
              if type(currentConfig) ~= 'table' then
                return -3
              end
              local rev = tonumber(currentConfig['revision'])
              if rev == nil or rev < 0 or rev ~= math.floor(rev) then
                return -3
              end
              if rev ~= tonumber(ARGV[1]) then
                return rev
              end
              local audit = cjson.decode(ARGV[3])
              audit['previousRevision'] = rev
              audit['beforeConfig'] = currentConfig
              ARGV[3] = cjson.encode(audit)
            else
              return -2
            end
            redis.call('SET', KEYS[1], ARGV[2])
            redis.call('RPUSH', KEYS[2], ARGV[3])
            redis.call('LTRIM', KEYS[2], -tonumber(ARGV[4]), -1)
            return -1
            """, Long.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final AtomicReference<RuntimeConfigSnapshot> lastKnownGood = new AtomicReference<>();

    public RedisRuntimeConfigStore(StringRedisTemplate redis, ObjectMapper objectMapper, Clock clock) {
        this.redis = Objects.requireNonNull(redis, "redis");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public RuntimeConfigSnapshot get() {
        RuntimeConfigSnapshot cachedAtReadStart = lastKnownGood.get();
        try {
            String json = redis.opsForValue().get(CONFIG_KEY);
            if (json == null) {
                RuntimeConfigSnapshot cached = lastKnownGood.get();
                if (cached == null) {
                    throw missing(null);
                }
                return stale(cached);
            }
            RuntimeConfigSnapshot snapshot = objectMapper.readValue(json, RuntimeConfigSnapshot.class);
            lastKnownGood.compareAndSet(cachedAtReadStart, snapshot);
            return snapshot;
        } catch (DataAccessException exception) {
            return staleOrThrow(exception);
        } catch (JacksonException exception) {
            throw corrupted(exception);
        }
    }

    @Override
    public RuntimeConfigSnapshot update(RuntimeConfigCommand command, long expectedRevision) {
        RuntimeConfigSnapshot cachedAtUpdateStart = lastKnownGood.get();
        if (expectedRevision < 0) {
            throw new BusinessException(RuntimeConfigErrorCode.INVALID_REVISION);
        }
        RuntimeConfigSnapshot after = new RuntimeConfigSnapshot(
                command.engineVersion(), command.releaseStage(), command.queueMode(),
                expectedRevision + 1, clock.instant(), command.updatedBy(), SourceStatus.VALID);
        String requestId = requestId();
        RuntimeConfigAuditLog auditLog = new RuntimeConfigAuditLog(
                expectedRevision, after.revision(), null, after, command.updatedBy(),
                after.updatedAt(), requestId);
        String afterJson = writeForUpdate(after);
        String auditJson = writeForUpdate(auditLog);
        try {
            Long conflictRevision = redis.execute(
                    CAS_SCRIPT, List.of(CONFIG_KEY, AUDIT_KEY),
                    Long.toString(expectedRevision), afterJson, auditJson,
                    Integer.toString(AUDIT_RETENTION));
            if (conflictRevision == null) {
                throw unavailable(null);
            }
            if (conflictRevision == CONFIG_MISSING) {
                throw missing(null);
            }
            if (conflictRevision == CONFIG_CORRUPTED) {
                throw corrupted(null);
            }
            if (conflictRevision >= 0) {
                throw new RuntimeConfigRevisionConflictException(conflictRevision);
            }
            if (conflictRevision != CAS_SUCCESS) {
                throw unavailable(null);
            }
            lastKnownGood.compareAndSet(cachedAtUpdateStart, after);
            return after;
        } catch (RuntimeConfigRevisionConflictException exception) {
            throw exception;
        } catch (DataAccessException exception) {
            if (exception instanceof InvalidDataAccessApiUsageException) {
                throw unavailable(exception);
            }
            return confirmUncertainUpdate(after, requestId, cachedAtUpdateStart, exception);
        }
    }

    @Override
    public Optional<RuntimeConfigSnapshot> getLastKnownGood() {
        return Optional.ofNullable(lastKnownGood.get());
    }

    private RuntimeConfigSnapshot staleOrThrow(Exception cause) {
        RuntimeConfigSnapshot cached = lastKnownGood.get();
        if (cached == null) {
            throw unavailable(cause);
        }
        return stale(cached);
    }

    private static RuntimeConfigSnapshot stale(RuntimeConfigSnapshot cached) {
        return new RuntimeConfigSnapshot(
                cached.engineVersion(), cached.releaseStage(), cached.queueMode(), cached.revision(),
                cached.updatedAt(), cached.updatedBy(), SourceStatus.STALE);
    }

    private RuntimeConfigSnapshot confirmUncertainUpdate(
            RuntimeConfigSnapshot expected,
            String requestId,
            RuntimeConfigSnapshot cachedAtUpdateStart,
            DataAccessException cause
    ) {
        if ("system".equals(requestId)) {
            throw outcomeUnknown(cause);
        }
        try {
            List<String> recentAudits = redis.opsForList().range(AUDIT_KEY, -AUDIT_RETENTION, -1);
            if (recentAudits != null) {
                for (int index = recentAudits.size() - 1; index >= 0; index--) {
                    RuntimeConfigAuditLog audit = objectMapper.readValue(
                            recentAudits.get(index), RuntimeConfigAuditLog.class);
                    if (requestId.equals(audit.requestId()) && expected.equals(audit.afterConfig())) {
                        lastKnownGood.compareAndSet(cachedAtUpdateStart, audit.afterConfig());
                        return audit.afterConfig();
                    }
                }
            }
        } catch (DataAccessException | JacksonException ignored) {
            // 결과를 확정할 수 없으므로 아래의 전용 오류로 통일한다.
        }
        throw outcomeUnknown(cause);
    }

    private String write(Object value) throws JacksonException {
        return objectMapper.writeValueAsString(value);
    }

    private String writeForUpdate(Object value) {
        try {
            return write(value);
        } catch (JacksonException exception) {
            throw serializationFailed(exception);
        }
    }

    private static String requestId() {
        String value = MDC.get(REQUEST_ID);
        return value == null || value.isBlank() ? "system" : value;
    }

    private static BusinessException unavailable(Throwable cause) {
        return new BusinessException(
                RuntimeConfigErrorCode.STORE_UNAVAILABLE,
                RuntimeConfigErrorCode.STORE_UNAVAILABLE.getMessage(), cause);
    }

    private static BusinessException corrupted(Throwable cause) {
        return new BusinessException(
                RuntimeConfigErrorCode.STORE_CORRUPTED,
                RuntimeConfigErrorCode.STORE_CORRUPTED.getMessage(), cause);
    }

    private static BusinessException missing(Throwable cause) {
        return new BusinessException(
                RuntimeConfigErrorCode.STORE_MISSING,
                RuntimeConfigErrorCode.STORE_MISSING.getMessage(), cause);
    }

    private static BusinessException outcomeUnknown(Throwable cause) {
        return new BusinessException(
                RuntimeConfigErrorCode.UPDATE_OUTCOME_UNKNOWN,
                RuntimeConfigErrorCode.UPDATE_OUTCOME_UNKNOWN.getMessage(), cause);
    }

    private static BusinessException serializationFailed(Throwable cause) {
        return new BusinessException(
                RuntimeConfigErrorCode.SERIALIZATION_FAILED,
                RuntimeConfigErrorCode.SERIALIZATION_FAILED.getMessage(), cause);
    }
}
