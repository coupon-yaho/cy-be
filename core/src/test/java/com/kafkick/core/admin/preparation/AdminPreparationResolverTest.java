package com.kafkick.core.admin.preparation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundCatalog;
import com.kafkick.core.admin.couponroundsource.PreparationSource;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

/** DB 쿠폰 회차 모집단에서 V2 Redis 준비 조회 대상과 응답 모집단을 확정하는 규칙을 검증합니다. */
class AdminPreparationResolverTest {

    private static final Instant SNAPSHOT = Instant.parse("2026-08-29T09:00:00Z");
    private static final Instant OPENS_AT = Instant.parse("2026-08-29T10:00:00Z");
    private static final Instant CLOSES_AT = Instant.parse("2026-08-29T11:00:00Z");

    /** V1·열린 V2·DB 미준비 회차가 Redis 요청에 섞이는 회귀를 방지합니다. */
    @Test
    @DisplayName("DB 준비가 끝난 V2 예약 회차만 Redis로 조회한다")
    void readsOnlyDbReadyScheduledV2CouponRounds() {
        RecordingReader reader = new RecordingReader(Map.of(
                10L, new V2PreparationSource(true, true, SourceStatus.VALID, SNAPSHOT)));
        AdminCouponRoundCatalog catalog = catalog(
                couponRound(10L, EngineVersion.V2, CouponRoundStatus.SCHEDULED, true, true, validStock()),
                couponRound(11L, EngineVersion.V1, CouponRoundStatus.SCHEDULED, true, true, validStock()),
                couponRound(12L, EngineVersion.V2, CouponRoundStatus.OPEN, true, true, validStock()),
                couponRound(13L, EngineVersion.V2, CouponRoundStatus.SCHEDULED, false, true, validStock()),
                couponRound(14L, EngineVersion.V2, CouponRoundStatus.SCHEDULED, true, true, unavailableStock()));

        Map<Long, V2PreparationSource> result =
                new AdminPreparationResolver(reader).resolve(catalog, SNAPSHOT);

        assertThat(reader.requests).singleElement().satisfies(request -> {
            assertThat(request.couponId()).isEqualTo(10L);
            assertThat(request.opensAt()).isEqualTo(OPENS_AT);
            assertThat(request.closesAt()).isEqualTo(CLOSES_AT);
            assertThat(request.expectedGradeMask()).isEqualTo(3);
            assertThat(request.expectedTotalQuantity()).isEqualTo(100L);
            assertThat(request.expectedRemainingQuantity()).isEqualTo(75L);
        });
        assertThat(result).containsOnlyKeys(10L, 11L, 12L, 13L, 14L);
        assertThat(result.get(10L).status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.get(11L).status()).isEqualTo(SourceStatus.N_A);
        assertThat(result.get(12L).status()).isEqualTo(SourceStatus.N_A);
        assertThat(result.get(13L).status()).isEqualTo(SourceStatus.N_A);
        assertThat(result.get(14L).status()).isEqualTo(SourceStatus.N_A);
        assertThatThrownBy(result::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    /** 원천 카탈로그를 읽지 못한 경우 빈 모집단으로 Redis를 호출하지 않는지 검증합니다. */
    @Test
    @DisplayName("값 없는 DB 카탈로그는 Redis를 호출하지 않고 빈 결과를 반환한다")
    void skipsRedisForValueLessCatalog() {
        AtomicInteger calls = new AtomicInteger();
        V2AdminPreparationReader reader = (requests, observedAt) -> {
            calls.incrementAndGet();
            return Map.of();
        };
        AdminCouponRoundCatalog catalog = new AdminCouponRoundCatalog(
                SourceStatus.UNAVAILABLE, null, List.of());

        Map<Long, V2PreparationSource> result =
                new AdminPreparationResolver(reader).resolve(catalog, SNAPSHOT);

        assertThat(result).isEmpty();
        assertThat(calls).hasValue(0);
    }

    /** Reader가 일부 ID를 빼면 누락된 회차만이 아니라 같은 batch의 모집단 전체를 미판정합니다. */
    @Test
    @DisplayName("Reader 응답 ID가 누락되면 요청한 V2 회차 전체를 UNAVAILABLE로 만든다")
    void rejectsMissingReaderPopulation() {
        AdminCouponRoundCatalog catalog = catalog(
                couponRound(10L, EngineVersion.V2, CouponRoundStatus.SCHEDULED, true, true, validStock()),
                couponRound(11L, EngineVersion.V2, CouponRoundStatus.SCHEDULED, true, true, validStock()));
        V2AdminPreparationReader reader = (requests, observedAt) -> Map.of(
                10L, new V2PreparationSource(true, true, SourceStatus.VALID, SNAPSHOT));

        Map<Long, V2PreparationSource> result =
                new AdminPreparationResolver(reader).resolve(catalog, SNAPSHOT);

        assertThat(result.values()).extracting(V2PreparationSource::status)
                .containsOnly(SourceStatus.UNAVAILABLE);
    }

    /** Reader가 요청하지 않은 ID를 섞어 잘못된 모집단을 정상으로 확장하지 않는지 검증합니다. */
    @Test
    @DisplayName("Reader 응답 ID가 초과되면 요청한 V2 회차 전체를 UNAVAILABLE로 만든다")
    void rejectsExtraReaderPopulation() {
        AdminCouponRoundCatalog catalog = catalog(
                couponRound(10L, EngineVersion.V2, CouponRoundStatus.SCHEDULED, true, true, validStock()));
        V2AdminPreparationReader reader = (requests, observedAt) -> Map.of(
                10L, new V2PreparationSource(true, true, SourceStatus.VALID, SNAPSHOT),
                99L, new V2PreparationSource(true, true, SourceStatus.VALID, SNAPSHOT));

        Map<Long, V2PreparationSource> result =
                new AdminPreparationResolver(reader).resolve(catalog, SNAPSHOT);

        assertThat(result).containsOnlyKeys(10L);
        assertThat(result.get(10L).status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 어댑터가 예외를 던져도 전체 Overview가 깨지지 않고 V2 대상만 미판정하는지 검증합니다. */
    @Test
    @DisplayName("Reader 예외는 V2 조회 대상만 UNAVAILABLE로 격리한다")
    void isolatesReaderFailureFromNonTargetCouponRounds() {
        AdminCouponRoundCatalog catalog = catalog(
                couponRound(10L, EngineVersion.V2, CouponRoundStatus.SCHEDULED, true, true, validStock()),
                couponRound(11L, EngineVersion.V1, CouponRoundStatus.SCHEDULED, true, true, validStock()));
        V2AdminPreparationReader reader = (requests, observedAt) -> {
            throw new IllegalStateException("down");
        };

        Map<Long, V2PreparationSource> result =
                new AdminPreparationResolver(reader).resolve(catalog, SNAPSHOT);

        assertThat(result.get(10L).status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.get(11L).status()).isEqualTo(SourceStatus.N_A);
    }

    /** Reader Bean 부재용 구현이 요청 순서의 불변 UNAVAILABLE 결과를 만드는지 검증합니다. */
    @Test
    @DisplayName("fallback Reader는 모든 요청을 순서대로 UNAVAILABLE로 반환한다")
    void unavailableFallbackReturnsImmutablePopulation() {
        List<V2AdminPreparationReader.Request> requests = List.of(request(20L), request(10L));

        Map<Long, V2PreparationSource> result = AdminPreparationResolver.unavailableV2Reader()
                .read(requests, SNAPSHOT);

        assertThat(result.keySet()).containsExactly(20L, 10L);
        assertThat(result.values()).extracting(V2PreparationSource::status)
                .containsOnly(SourceStatus.UNAVAILABLE);
        assertThatThrownBy(result::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    /** 중복 ID가 Map에서 조용히 덮여 요청·응답 모집단 크기가 달라지는 것을 거부하는지 검증합니다. */
    @Test
    @DisplayName("fallback Reader는 중복 요청 ID를 거부한다")
    void unavailableFallbackRejectsDuplicateIds() {
        assertThatThrownBy(() -> AdminPreparationResolver.unavailableV2Reader().read(
                List.of(request(10L), request(10L)), SNAPSHOT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 여러 쿠폰 회차를 VALID DB 모집단으로 생성합니다. */
    private static AdminCouponRoundCatalog catalog(AdminCouponRoundCatalog.CouponRoundData... couponRounds) {
        return new AdminCouponRoundCatalog(SourceStatus.VALID, SNAPSHOT, List.of(couponRounds));
    }

    /** 엔진·상태·DB 준비·재고 상태가 명시된 쿠폰 회차를 생성합니다. */
    private static AdminCouponRoundCatalog.CouponRoundData couponRound(
            long couponId,
            EngineVersion engineVersion,
            CouponRoundStatus status,
            boolean configurationReady,
            boolean databaseStockReady,
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock
    ) {
        return new AdminCouponRoundCatalog.CouponRoundData(
                couponId, "쿠폰 회차 " + couponId, "브랜드", engineVersion, status,
                OPENS_AT, CLOSES_AT, stock,
                new PreparationSource(
                        configurationReady, databaseStockReady,
                        CouponPolicyType.FIXED_AMOUNT, 3, SourceStatus.VALID, SNAPSHOT));
    }

    /** DB 정본 총수량과 active_count를 값 보유 상태로 생성합니다. */
    private static CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> validStock() {
        return new CouponMetricsSource.Observation<>(
                new CouponMetricsSource.StockCounts(100L, 25L), SourceStatus.VALID, SNAPSHOT);
    }

    /** DB 재고를 읽지 못한 상태를 생성합니다. */
    private static CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> unavailableStock() {
        return new CouponMetricsSource.Observation<>(null, SourceStatus.UNAVAILABLE, null);
    }

    /** fallback Reader 검증에 사용할 정상 V2 예약 요청을 생성합니다. */
    private static V2AdminPreparationReader.Request request(long couponId) {
        return new V2AdminPreparationReader.Request(
                couponId, CouponRoundStatus.SCHEDULED, OPENS_AT, CLOSES_AT, 3, 100L, 100L);
    }

    /** 실제 Reader 입력을 기록하고 지정된 응답을 반환합니다. */
    private static final class RecordingReader implements V2AdminPreparationReader {
        private final Map<Long, V2PreparationSource> response;
        private List<Request> requests = new ArrayList<>();

        private RecordingReader(Map<Long, V2PreparationSource> response) {
            this.response = new LinkedHashMap<>(response);
        }

        /** 요청 복사본을 보존해 Resolver가 선택한 실제 모집단을 검증할 수 있게 합니다. */
        @Override
        public Map<Long, V2PreparationSource> read(List<Request> requests, Instant observedAt) {
            this.requests = List.copyOf(requests);
            return Map.copyOf(response);
        }
    }
}
