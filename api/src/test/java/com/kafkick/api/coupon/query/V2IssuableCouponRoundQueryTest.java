package com.kafkick.api.coupon.query;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Ticker;
import com.kafkick.core.coupon.v2.query.CouponDefinition;
import com.kafkick.core.coupon.v2.query.CouponDefinitionL2CachePort;
import com.kafkick.core.coupon.v2.query.CouponDefinitionQueryService;
import com.kafkick.core.coupon.v2.query.CouponDefinitionSnapshot;
import com.kafkick.core.coupon.v2.query.DisabledCouponDefinitionL2Cache;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.support.TimeProvider;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

class V2IssuableCouponRoundQueryTest {

    private static final Instant NOW = Instant.parse("2026-08-28T00:00:00Z");

    @Test
    void cachesDefinitionsWithoutReadingMemberOrStockState() {
        AtomicInteger definitionLoads = new AtomicInteger();
        V2IssuableCouponRoundQuery query = query(new DisabledCouponDefinitionL2Cache(),
                definitionLoads, List.of(definition(7L, CouponRoundStatus.OPEN, -1, 60)));

        assertThat(query.findOpenDefinitions(NOW)).extracting(CouponDefinition::couponRoundId)
                .containsExactly(7L);
        assertThat(query.findOpenDefinitions(NOW)).extracting(CouponDefinition::couponRoundId)
                .containsExactly(7L);

        assertThat(definitionLoads).hasValue(1);
    }

    @Test
    void showsARoundTheMomentItOpensEvenBeforeTheBatchFlipsItsStatus() {
        // status 를 뒤집는 것은 batch 의 1분 cron 이다. 글자를 기다리면 오픈 정각에 열린 회차가
        // 최대 1분 뒤에야 목록에 뜬다 — 선착순 회차에서는 그것이 곧 실패다.
        V2IssuableCouponRoundQuery query = query(new DisabledCouponDefinitionL2Cache(),
                new AtomicInteger(), List.of(definition(9L, CouponRoundStatus.SCHEDULED, 0, 60)));

        assertThat(query.findOpenDefinitions(NOW)).extracting(CouponDefinition::couponRoundId)
                .containsExactly(9L);
    }

    @Test
    void stillHidesARoundWhoseOpenTimeHasNotArrived() {
        V2IssuableCouponRoundQuery query = query(new DisabledCouponDefinitionL2Cache(),
                new AtomicInteger(), List.of(definition(9L, CouponRoundStatus.OPEN, 30, 60)));

        assertThat(query.findOpenDefinitions(NOW)).isEmpty();
    }

    @Test
    void servesTheSharedCacheWithoutTouchingTheDatabase() {
        AtomicInteger definitionLoads = new AtomicInteger();
        RecordingL2 l2 = new RecordingL2();
        l2.value = new CouponDefinitionSnapshot(
                List.of(definition(7L, CouponRoundStatus.OPEN, -1, 60)), NOW.plusSeconds(60));

        assertThat(query(l2, definitionLoads, List.of()).findOpenDefinitions(NOW))
                .extracting(CouponDefinition::couponRoundId).containsExactly(7L);
        assertThat(definitionLoads).hasValue(0);
    }

    @Test
    void waitsForTheInstanceThatHoldsTheLoadPermitInsteadOfQueryingTheDatabaseToo() {
        AtomicInteger definitionLoads = new AtomicInteger();
        RecordingL2 l2 = new RecordingL2();
        l2.permitTaken = true;
        // 두 번째 폴링에서 권한을 쥔 인스턴스의 결과가 도착한다.
        l2.appearAfterFinds = 2;
        l2.value = new CouponDefinitionSnapshot(
                List.of(definition(7L, CouponRoundStatus.OPEN, -1, 60)), NOW.plusSeconds(60));

        assertThat(query(l2, definitionLoads, List.of()).findOpenDefinitions(NOW))
                .extracting(CouponDefinition::couponRoundId).containsExactly(7L);
        assertThat(definitionLoads).as("남의 로드를 기다렸으므로 DB 로 가지 않는다").hasValue(0);
    }

    @Test
    void fallsBackToTheDatabaseWhenTheAwaitedLoadNeverArrives() {
        AtomicInteger definitionLoads = new AtomicInteger();
        RecordingL2 l2 = new RecordingL2();
        l2.permitTaken = true;

        // 여기서 멈추면 503 이다. 권한을 쥔 인스턴스가 죽은 경우가 정확히 그렇다.
        assertThat(query(l2, definitionLoads, List.of(definition(7L, CouponRoundStatus.OPEN, -1, 60)))
                .findOpenDefinitions(NOW))
                .extracting(CouponDefinition::couponRoundId).containsExactly(7L);
        assertThat(definitionLoads).hasValue(1);
    }

    @Test
    void releasesTheLoadPermitEvenWhenTheDatabaseFails() {
        RecordingL2 l2 = new RecordingL2();
        CouponDefinitionQueryService failing = new CouponDefinitionQueryService(asOf -> {
            throw new IllegalStateException("db down");
        });
        V2IssuableCouponRoundQuery query = new V2IssuableCouponRoundQuery(
                cache(), failing, l2, l2Properties(), timeProvider());

        try {
            query.findOpenDefinitions(NOW);
        } catch (RuntimeException expected) {
            // 여기서 반납을 빠뜨리면 lease 가 끝날 때까지 아무 인스턴스도 로드하지 못한다.
        }
        assertThat(l2.released).isEqualTo(1);
    }

    @Test
    void capsTheSharedCacheLifetimeAtTheNextRoundBoundary() {
        RecordingL2 l2 = new RecordingL2();
        // 경계가 3초 뒤다. L2 를 ttl(10초)로 두면 오픈 정각에 리로드해도 옛 목록을 되받는다.
        query(l2, new AtomicInteger(), List.of(definition(7L, CouponRoundStatus.OPEN, -1, 3)))
                .findOpenDefinitions(NOW);

        assertThat(l2.putTtl).isEqualTo(Duration.ofSeconds(3));
    }

    private V2IssuableCouponRoundQuery query(
            CouponDefinitionL2CachePort l2, AtomicInteger loads, List<CouponDefinition> rows) {
        CouponDefinitionQueryService service = new CouponDefinitionQueryService(asOf -> {
            loads.incrementAndGet();
            return rows;
        });
        return new V2IssuableCouponRoundQuery(cache(), service, l2, l2Properties(), timeProvider());
    }

    private static CouponDefinitionL1Cache<List<CouponDefinition>> cache() {
        return new CouponDefinitionL1Cache<>(
                Clock.fixed(NOW, ZoneOffset.UTC), Ticker.systemTicker(),
                new CouponDefinitionL1CacheProperties(
                        Duration.ofSeconds(10), Duration.ofSeconds(5),
                        Duration.ofSeconds(60), 10L),
                Runnable::run, new SimpleMeterRegistry());
    }

    private static CouponDefinitionL2CacheProperties l2Properties() {
        return new CouponDefinitionL2CacheProperties(
                Duration.ofSeconds(10), Duration.ofSeconds(3),
                Duration.ofMillis(60), Duration.ofMillis(1));
    }

    private static TimeProvider timeProvider() {
        return new TimeProvider(Clock.systemUTC());
    }

    private static CouponDefinition definition(
            long id, CouponRoundStatus status, int openOffset, int closeOffset) {
        return new CouponDefinition(id, 1L, "coupon", CouponPolicyType.FIXED_AMOUNT,
                null, null, 1_000, 30, NOW.plusSeconds(openOffset), NOW.plusSeconds(closeOffset),
                status);
    }

    /** L2 대역. 실패를 삼키는 계약은 Redis 어댑터 쪽 테스트가 본다. */
    private static final class RecordingL2 implements CouponDefinitionL2CachePort {
        private final List<String> acquired = new ArrayList<>();
        private CouponDefinitionSnapshot value;
        private boolean permitTaken;
        private int appearAfterFinds;
        private int finds;
        private int released;
        private Duration putTtl;

        @Override
        public Optional<CouponDefinitionSnapshot> find() {
            finds++;
            if (value == null || finds <= appearAfterFinds - 1 && appearAfterFinds > 0) {
                return Optional.empty();
            }
            return Optional.of(value);
        }

        @Override
        public void put(CouponDefinitionSnapshot snapshot, Duration ttl) {
            this.value = snapshot;
            this.putTtl = ttl;
        }

        @Override
        public Optional<String> tryAcquireLoad(Duration lease) {
            if (permitTaken) {
                return Optional.empty();
            }
            String token = "token-" + acquired.size();
            acquired.add(token);
            return Optional.of(token);
        }

        @Override
        public void releaseLoad(String token) {
            released++;
        }
    }
}
