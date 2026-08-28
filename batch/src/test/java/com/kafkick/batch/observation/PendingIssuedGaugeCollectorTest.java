package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.infra.redis.coupon.v2.IssuanceKeys;

class PendingIssuedGaugeCollectorTest {

    // EPOCH 근처가 아니라 실제 운영 시각이어야 한다 — epoch 기준 나이 계산 결함이 가려진다.
    private static final Instant NOW = Instant.parse("2026-08-28T09:00:10Z");
    private static final String ROUND_TAG = "couponRoundId";

    @Test
    void keepsClosedRoundVisibleWhileItsV2BenchmarkRunIsNotFinalized() {
        Fixture fixture = fixture();
        when(fixture.jdbc.queryForList(any(String.class))).thenReturn(List.of(
            Map.of("coupon_id", 7L, "engine_version", "V2")));
        Cursor<Map.Entry<Object, Object>> values = cursor(
            Map.entry("1", "P|1000|token-1|key-1"),
            Map.entry("2", "D|" + NOW.minusSeconds(9).toEpochMilli() + "|token-2|key-2"),
            Map.entry("3", "broken"));
        when(fixture.hash.scan(eq(IssuanceKeys.of(7L).issued()), any(ScanOptions.class)))
            .thenReturn(values);

        fixture.collector.collect();

        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 7L, 1);
        assertGauge(fixture.registry, DomainMeterNames.CORRUPT_FIELD_COUNT, 7L, 1);
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 7L,
            SourceStatusCode.of(SourceStatus.VALID));
        ArgumentCaptor<ScanOptions> options = ArgumentCaptor.forClass(ScanOptions.class);
        verify(fixture.hash).scan(eq(IssuanceKeys.of(7L).issued()), options.capture());
        assertThat(options.getValue().getCount()).isEqualTo(200);
        ArgumentCaptor<String> targetSql = ArgumentCaptor.forClass(String.class);
        verify(fixture.jdbc).queryForList(targetSql.capture());
        assertThat(targetSql.getValue())
            .contains("FROM benchmark_runs", "run_status <> 'FINALIZED'")
            .doesNotContain("FROM coupons", "status = 'OPEN'");
    }

    @Test
    void failedRedisReadPublishesNaNAndUnavailableInsteadOfZeroOrThePreviousValue() {
        Fixture fixture = fixture();
        when(fixture.jdbc.queryForList(any(String.class))).thenReturn(List.of(
            Map.of("coupon_id", 7L, "engine_version", "V2")));
        Cursor<Map.Entry<Object, Object>> values =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        when(fixture.hash.scan(eq(IssuanceKeys.of(7L).issued()), any(ScanOptions.class)))
            .thenReturn(values)
            .thenThrow(new IllegalStateException("redis timeout"));

        fixture.collector.collect();
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 7L, 1);

        fixture.collector.collect();

        assertThat(gauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 7L).value()).isNaN();
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 7L,
            SourceStatusCode.of(SourceStatus.UNAVAILABLE));
        assertThat(gauge(fixture.registry, DomainMeterNames.CORRUPT_FIELD_COUNT, 7L).value()).isNaN();
    }

    @Test
    void v1RunIsNotApplicableAndNeverTouchesRedis() {
        Fixture fixture = fixture();
        when(fixture.jdbc.queryForList(any(String.class))).thenReturn(List.of(
            Map.of("coupon_id", 8L, "engine_version", "V1")));
        clearInvocations(fixture.redis, fixture.hash);

        fixture.collector.collect();

        assertThat(gauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 8L).value()).isNaN();
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 8L,
            SourceStatusCode.of(SourceStatus.N_A));
        verifyNoInteractions(fixture.redis, fixture.hash);
    }

    @Test
    void roundThatLeftTheTargetSetKeepsItsValueUntilTwoIntervalsPass() {
        Fixture fixture = fixture();
        when(fixture.jdbc.queryForList(any(String.class)))
            .thenReturn(List.of(Map.of("coupon_id", 7L, "engine_version", "V2")))
            .thenReturn(List.of());
        Cursor<Map.Entry<Object, Object>> values =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        when(fixture.hash.scan(eq(IssuanceKeys.of(7L).issued()), any(ScanOptions.class)))
            .thenReturn(values);

        fixture.collector.collect();
        fixture.collector.collect(); // FINALIZED되어 대상 조회에서 빠짐

        fixture.now.set(NOW.plusSeconds(59));
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 7L, 1);
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 7L,
            SourceStatusCode.of(SourceStatus.VALID));
    }

    @Test
    void valueOlderThanTwoIntervalsIsPublishedAsNaNAndUnavailableInsteadOfAFrozenValid() {
        Fixture fixture = fixture();
        when(fixture.jdbc.queryForList(any(String.class)))
            .thenReturn(List.of(Map.of("coupon_id", 7L, "engine_version", "V2")))
            .thenReturn(List.of());
        Cursor<Map.Entry<Object, Object>> values =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        when(fixture.hash.scan(eq(IssuanceKeys.of(7L).issued()), any(ScanOptions.class)))
            .thenReturn(values);

        fixture.collector.collect();
        fixture.collector.collect(); // FINALIZED되어 대상 조회에서 빠짐

        fixture.now.set(NOW.plusSeconds(61));

        assertThat(gauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 7L).value()).isNaN();
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 7L,
            SourceStatusCode.of(SourceStatus.UNAVAILABLE));
        assertThat(gauge(fixture.registry, DomainMeterNames.CORRUPT_FIELD_COUNT, 7L).value()).isNaN();
    }

    @Test
    void meterRegistrationFailureOfOneRoundDoesNotBlindTheRoundsBehindIt() {
        Fixture fixture = fixture(registryRejecting("9"));
        when(fixture.jdbc.queryForList(any(String.class))).thenReturn(List.of(
            Map.of("coupon_id", 9L, "engine_version", "V2"),
            Map.of("coupon_id", 11L, "engine_version", "V2")));
        Cursor<Map.Entry<Object, Object>> values =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        when(fixture.hash.scan(eq(IssuanceKeys.of(11L).issued()), any(ScanOptions.class)))
            .thenReturn(values);

        assertThatCode(fixture.collector::collect).doesNotThrowAnyException();

        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 11L, 1);
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 11L,
            SourceStatusCode.of(SourceStatus.VALID));
    }

    @Test
    void targetLookupFailureMarksOnlyTheRoundsThatActuallyHaveMeters() {
        Fixture fixture = fixture(registryRejecting("9"));
        when(fixture.jdbc.queryForList(any(String.class)))
            .thenReturn(List.of(
                Map.of("coupon_id", 7L, "engine_version", "V2"),
                Map.of("coupon_id", 9L, "engine_version", "V2")))
            .thenThrow(new IllegalStateException("db timeout"));
        Cursor<Map.Entry<Object, Object>> values =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        when(fixture.hash.scan(eq(IssuanceKeys.of(7L).issued()), any(ScanOptions.class)))
            .thenReturn(values);

        fixture.collector.collect();

        assertThatCode(fixture.collector::collect).doesNotThrowAnyException();
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 7L,
            SourceStatusCode.of(SourceStatus.UNAVAILABLE));
    }

    // --- A: 한 바퀴가 interval 을 넘어도 대상에 남은 회차는 강등되지 않는다 ---

    @Test
    void slowCycleDoesNotFlipAStillTargetedRoundToUnavailable() {
        Fixture fixture = fixture();
        when(fixture.jdbc.queryForList(any(String.class))).thenReturn(List.of(
            Map.of("coupon_id", 7L, "engine_version", "V2")));
        Cursor<Map.Entry<Object, Object>> first =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        Cursor<Map.Entry<Object, Object>> second =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        when(fixture.hash.scan(eq(IssuanceKeys.of(7L).issued()), any(ScanOptions.class)))
            .thenReturn(first)
            .thenReturn(second);

        fixture.collector.collect();
        // 회차가 많아 한 바퀴가 interval(30s)을 넘겼다. 갱신은 정상적으로 이어진다.
        fixture.now.set(NOW.plusSeconds(70));
        fixture.collector.collect();
        fixture.now.set(NOW.plusSeconds(140));

        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 7L, 1);
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 7L,
            SourceStatusCode.of(SourceStatus.VALID));
    }

    // --- B: 이탈한 회차는 유예 뒤 미터가 해제된다 ---

    @Test
    void departedRoundIsRetiredInsteadOfPublishingUnavailableForever() {
        Fixture fixture = fixture();
        when(fixture.jdbc.queryForList(any(String.class)))
            .thenReturn(List.of(Map.of("coupon_id", 7L, "engine_version", "V2")))
            .thenReturn(List.of())
            .thenReturn(List.of());
        Cursor<Map.Entry<Object, Object>> values =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        when(fixture.hash.scan(eq(IssuanceKeys.of(7L).issued()), any(ScanOptions.class)))
            .thenReturn(values);

        fixture.collector.collect();
        fixture.collector.collect();
        assertThat(fixture.registry.find(DomainMeterNames.STALE_PENDING_COUNT)
            .tag(ROUND_TAG, "7").gauge()).isNotNull();

        fixture.now.set(NOW.plusSeconds(121));
        fixture.collector.collect();

        assertThat(fixture.registry.find(DomainMeterNames.STALE_PENDING_COUNT)
            .tag(ROUND_TAG, "7").gauge()).isNull();
        assertThat(fixture.registry.find(DomainMeterNames.CORRUPT_FIELD_COUNT_STATE)
            .tag(ROUND_TAG, "7").gauge()).isNull();
    }

    @Test
    void stillTargetedRoundIsNeverDowngradedNoMatterHowOldItsLastReadIs() {
        Fixture fixture = fixture();
        when(fixture.jdbc.queryForList(any(String.class))).thenReturn(List.of(
            Map.of("coupon_id", 7L, "engine_version", "V2")));
        Cursor<Map.Entry<Object, Object>> values =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        when(fixture.hash.scan(eq(IssuanceKeys.of(7L).issued()), any(ScanOptions.class)))
            .thenReturn(values);

        fixture.collector.collect();
        // 갱신 없이 나이만 먹는다. 회차는 여전히 대상이므로 강등도 해제도 없어야 한다.
        fixture.now.set(NOW.plusSeconds(600));

        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 7L, 1);
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 7L,
            SourceStatusCode.of(SourceStatus.VALID));
        assertThat(fixture.registry.find(DomainMeterNames.STALE_PENDING_COUNT)
            .tag(ROUND_TAG, "7").gauge()).isNotNull();
    }

    @Test
    void departedV3RoundGetsTheSameGraceAsEveryOtherStatus() {
        Fixture fixture = fixture();
        when(fixture.jdbc.queryForList(any(String.class)))
            .thenReturn(List.of(Map.of("coupon_id", 12L, "engine_version", "V3")))
            .thenReturn(List.of());

        fixture.collector.collect();
        fixture.collector.collect();

        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 12L,
            SourceStatusCode.of(SourceStatus.PENDING));
        assertThat(fixture.registry.find(DomainMeterNames.STALE_PENDING_COUNT)
            .tag(ROUND_TAG, "12").gauge()).isNotNull();
    }

    @Test
    void partialMeterRegistrationLeavesNoGaugeReadingAnAbandonedObservation() {
        java.util.concurrent.atomic.AtomicInteger rejections =
            new java.util.concurrent.atomic.AtomicInteger(1);
        SimpleMeterRegistry registry = new SimpleMeterRegistry() {
            @Override
            protected <T> io.micrometer.core.instrument.Gauge newGauge(
                io.micrometer.core.instrument.Meter.Id id,
                T obj,
                java.util.function.ToDoubleFunction<T> valueFunction
            ) {
                if (DomainMeterNames.CORRUPT_FIELD_COUNT.equals(id.getName())
                    && rejections.getAndDecrement() > 0) {
                    throw new IllegalStateException("meter limit reached");
                }
                return super.newGauge(id, obj, valueFunction);
            }
        };
        Fixture fixture = fixture(registry);
        when(fixture.jdbc.queryForList(any(String.class))).thenReturn(List.of(
            Map.of("coupon_id", 9L, "engine_version", "V2")));
        Cursor<Map.Entry<Object, Object>> first =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        Cursor<Map.Entry<Object, Object>> second =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        when(fixture.hash.scan(eq(IssuanceKeys.of(9L).issued()), any(ScanOptions.class)))
            .thenReturn(first)
            .thenReturn(second);

        fixture.collector.collect(); // 3번째 게이지 등록에서 실패한다
        fixture.collector.collect(); // 다음 사이클에 온전히 복구되어야 한다

        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 9L, 1);
        assertGauge(fixture.registry, DomainMeterNames.CORRUPT_FIELD_COUNT, 9L, 0);
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 9L,
            SourceStatusCode.of(SourceStatus.VALID));
        assertGauge(fixture.registry, DomainMeterNames.CORRUPT_FIELD_COUNT_STATE, 9L,
            SourceStatusCode.of(SourceStatus.VALID));
    }

    // --- D: engine_version 오타 한 행이 다른 회차를 끄지 않는다 ---

    @Test
    void unreadableEngineVersionMarksOnlyItsOwnRound() {
        Fixture fixture = fixture();
        when(fixture.jdbc.queryForList(any(String.class))).thenReturn(List.of(
            Map.of("coupon_id", 5L, "engine_version", "v2"),
            Map.of("coupon_id", 7L, "engine_version", "V2")));
        Cursor<Map.Entry<Object, Object>> values =
            cursor(Map.entry("1", "P|" + NOW.minusSeconds(9).toEpochMilli() + "|token-1|key-1"));
        when(fixture.hash.scan(eq(IssuanceKeys.of(7L).issued()), any(ScanOptions.class)))
            .thenReturn(values);

        assertThatCode(fixture.collector::collect).doesNotThrowAnyException();

        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 7L, 1);
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 7L,
            SourceStatusCode.of(SourceStatus.VALID));
        assertThat(gauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 5L).value()).isNaN();
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 5L,
            SourceStatusCode.of(SourceStatus.UNAVAILABLE));
    }

    // --- E: V3 는 "없는 값"이 아니라 "아직 못 읽은 값"이다 ---

    @Test
    void v3RunIsPendingNotNotApplicableAndNeverTouchesRedis() {
        Fixture fixture = fixture();
        when(fixture.jdbc.queryForList(any(String.class))).thenReturn(List.of(
            Map.of("coupon_id", 12L, "engine_version", "V3")));
        clearInvocations(fixture.redis, fixture.hash);

        fixture.collector.collect();

        assertThat(gauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT, 12L).value()).isNaN();
        assertGauge(fixture.registry, DomainMeterNames.STALE_PENDING_COUNT_STATE, 12L,
            SourceStatusCode.of(SourceStatus.PENDING));
        verifyNoInteractions(fixture.redis, fixture.hash);
    }

    private static SimpleMeterRegistry registryRejecting(String roundTag) {
        return new SimpleMeterRegistry() {
            @Override
            protected <T> io.micrometer.core.instrument.Gauge newGauge(
                io.micrometer.core.instrument.Meter.Id id,
                T obj,
                java.util.function.ToDoubleFunction<T> valueFunction
            ) {
                if (roundTag.equals(id.getTag(ROUND_TAG))) {
                    throw new IllegalStateException("meter registration failed");
                }
                return super.newGauge(id, obj, valueFunction);
            }
        };
    }

    private static Fixture fixture() {
        return fixture(new SimpleMeterRegistry());
    }

    private static Fixture fixture(SimpleMeterRegistry registry) {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        HashOperations<String, Object, Object> hash = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hash);
        java.util.concurrent.atomic.AtomicReference<Instant> now = new java.util.concurrent.atomic.AtomicReference<>(NOW);
        PendingIssuedGaugeCollector collector = new PendingIssuedGaugeCollector(
            jdbc, redis,
            new PendingIssuedGaugeProperties(
                true, Duration.ofSeconds(30), Duration.ofSeconds(5), 200, null),
            registry, movableTimeProvider(now));
        return new Fixture(jdbc, redis, hash, registry, collector, now);
    }

    private static Gauge gauge(SimpleMeterRegistry registry, String name, long roundId) {
        return registry.get(name).tag(ROUND_TAG, Long.toString(roundId)).gauge();
    }

    private static void assertGauge(
        SimpleMeterRegistry registry, String name, long roundId, double expected
    ) {
        assertThat(gauge(registry, name, roundId).value()).isEqualTo(expected);
    }

    @SafeVarargs
    private static Cursor<Map.Entry<Object, Object>> cursor(Map.Entry<Object, Object>... entries) {
        @SuppressWarnings("unchecked")
        Cursor<Map.Entry<Object, Object>> cursor = mock(Cursor.class);
        java.util.Iterator<Map.Entry<Object, Object>> iterator = List.of(entries).iterator();
        when(cursor.hasNext()).thenAnswer(invocation -> iterator.hasNext());
        when(cursor.next()).thenAnswer(invocation -> iterator.next());
        return cursor;
    }

    private static TimeProvider movableTimeProvider(java.util.concurrent.atomic.AtomicReference<Instant> now) {
        TimeProvider timeProvider = mock(TimeProvider.class);
        when(timeProvider.instant()).thenAnswer(invocation -> now.get());
        return timeProvider;
    }

    private record Fixture(
        JdbcTemplate jdbc,
        StringRedisTemplate redis,
        HashOperations<String, Object, Object> hash,
        SimpleMeterRegistry registry,
        PendingIssuedGaugeCollector collector,
        java.util.concurrent.atomic.AtomicReference<Instant> now
    ) { }
}
