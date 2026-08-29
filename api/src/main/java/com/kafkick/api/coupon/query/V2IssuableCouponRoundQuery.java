package com.kafkick.api.coupon.query;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.v2.query.CouponDefinition;
import com.kafkick.core.coupon.v2.query.CouponDefinitionL2CachePort;
import com.kafkick.core.coupon.v2.query.CouponDefinitionQueryService;
import com.kafkick.core.coupon.v2.query.CouponDefinitionSnapshot;
import com.kafkick.core.support.TimeProvider;

/**
 * V2 목록: L1에는 발급 판정과 무관한 쿠폰 정의만 둔다.
 *
 * <p>경로는 L1(인스턴스) → L2(공용 Redis) → DB 다. L1 miss 는 Caffeine 이 인스턴스 안에서
 * 하나로 합치고, 그렇게 남은 인스턴스별 miss 를 L2 의 로드 권한이 한 번으로 합친다.
 *
 * <p><b>노출 여부는 DB 의 status 글자가 아니라 시각으로 판정한다.</b> status 를 뒤집는 것은
 * batch 의 1분 cron 이라, 글자를 기다리면 오픈 정각에 열린 회차가 최대 1분 뒤에야 목록에 뜬다.
 * 게다가 그 지연 때문에 {@link #nextBoundary} 가 open_at 에 맞춰 리로드해 봐야 여전히
 * SCHEDULED 를 보게 되어, 경계 로직 자체가 아무 값도 못 냈다. 시각으로 판정하면 둘 다 풀린다.
 * SQL 이 {@code status IN ('SCHEDULED','OPEN')} 으로 CLOSED 를 이미 걸러 주므로, 여기서
 * 시각만 보아도 닫힌 회차가 새어 나오지 않는다.
 */
@Component
public final class V2IssuableCouponRoundQuery {

    private final CouponDefinitionL1Cache<List<CouponDefinition>> definitions;
    private final CouponDefinitionQueryService definitionQueryService;
    private final CouponDefinitionL2CachePort l2Cache;
    private final CouponDefinitionL2CacheProperties l2Properties;
    private final TimeProvider timeProvider;

    public V2IssuableCouponRoundQuery(
            CouponDefinitionL1Cache<List<CouponDefinition>> definitions,
            CouponDefinitionQueryService definitionQueryService,
            CouponDefinitionL2CachePort l2Cache,
            CouponDefinitionL2CacheProperties l2Properties,
            TimeProvider timeProvider
    ) {
        this.definitions = Objects.requireNonNull(definitions);
        this.definitionQueryService = Objects.requireNonNull(definitionQueryService);
        this.l2Cache = Objects.requireNonNull(l2Cache);
        this.l2Properties = Objects.requireNonNull(l2Properties);
        this.timeProvider = Objects.requireNonNull(timeProvider);
    }

    public List<CouponDefinition> findOpenDefinitions(Instant asOf) {
        List<CouponDefinition> allDefinitions =
                definitions.get(CouponDefinitionCacheKey.ALL, () -> loadThroughL2(asOf));
        return allDefinitions.stream()
                .filter(definition -> !definition.openAt().isAfter(asOf))
                .filter(definition -> definition.closeAt().isAfter(asOf))
                .toList();
    }

    private CouponDefinitionL1Cache.LoadedValue<List<CouponDefinition>> loadThroughL2(Instant asOf) {
        Optional<CouponDefinitionSnapshot> shared = l2Cache.find();
        if (shared.isPresent()) {
            return toLoaded(shared.get());
        }
        Optional<String> token = l2Cache.tryAcquireLoad(l2Properties.lockLease());
        if (token.isEmpty()) {
            // 다른 인스턴스가 이미 DB 로 갔다. 그 답을 기다리는 편이 같은 질의를 한 번 더
            // 던지는 것보다 싸다. 그래도 안 오면 DB 로 간다 — 여기서 멈추면 503 이다.
            Optional<CouponDefinitionSnapshot> awaited = awaitSharedLoad();
            return awaited.map(this::toLoaded).orElseGet(() -> loadFromDatabase(asOf, null));
        }
        return loadFromDatabase(asOf, token.get());
    }

    private Optional<CouponDefinitionSnapshot> awaitSharedLoad() {
        Instant deadline = timeProvider.instant().plus(l2Properties.waitTimeout());
        while (true) {
            Optional<CouponDefinitionSnapshot> shared = l2Cache.find();
            if (shared.isPresent()) {
                return shared;
            }
            if (!timeProvider.instant().isBefore(deadline)) {
                return Optional.empty();
            }
            try {
                Thread.sleep(l2Properties.pollInterval().toMillis());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return Optional.empty();
            }
        }
    }

    private CouponDefinitionL1Cache.LoadedValue<List<CouponDefinition>> loadFromDatabase(
            Instant asOf, String token) {
        try {
            List<CouponDefinition> loaded = definitionQueryService.findCandidates(asOf);
            CouponDefinitionSnapshot snapshot =
                    new CouponDefinitionSnapshot(loaded, nextBoundary(loaded, asOf));
            // L2 수명도 회차 경계에서 끊는다. TTL 만 보고 두면 L1(최대 ttl) 위에 L2(최대 ttl)가
            // 얹혀 최악 지연이 두 배가 되고, 오픈 정각에 리로드해도 L2 의 옛 목록을 되받는다.
            l2Cache.put(snapshot, cappedTtl(snapshot.nextBoundary(), asOf));
            return toLoaded(snapshot);
        } finally {
            if (token != null) {
                l2Cache.releaseLoad(token);
            }
        }
    }

    private java.time.Duration cappedTtl(Instant nextBoundary, Instant loadedAt) {
        java.time.Duration untilBoundary = java.time.Duration.between(loadedAt, nextBoundary);
        if (untilBoundary.compareTo(l2Properties.ttl()) >= 0) {
            return l2Properties.ttl();
        }
        // 경계가 이미 지났거나 0 이면 Redis 가 SET 을 거부한다. 최소 1ms 로 올린다.
        return untilBoundary.isNegative() || untilBoundary.isZero()
                ? java.time.Duration.ofMillis(1) : untilBoundary;
    }

    private CouponDefinitionL1Cache.LoadedValue<List<CouponDefinition>> toLoaded(
            CouponDefinitionSnapshot snapshot) {
        return new CouponDefinitionL1Cache.LoadedValue<>(
                snapshot.definitions(), snapshot.nextBoundary());
    }

    private static Instant nextBoundary(List<CouponDefinition> definitions, Instant loadedAt) {
        return definitions.stream()
                .flatMap(definition -> java.util.stream.Stream.of(
                        definition.openAt(), definition.closeAt()))
                .filter(boundary -> boundary.isAfter(loadedAt))
                .min(Comparator.naturalOrder())
                .orElseGet(() -> loadedAt.plusSeconds(10));
    }
}
