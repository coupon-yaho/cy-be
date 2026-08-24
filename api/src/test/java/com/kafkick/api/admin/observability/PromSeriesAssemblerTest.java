package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesEntry;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesKey;
import com.kafkick.api.observation.http.ResultClassifier.ResultClass;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/** 계열이 서로 독립적으로 죽는지, 예산이 뒤부터 자르는지 검증합니다. */
class PromSeriesAssemblerTest {

    private static final TimeProvider FIXED_TIME =
            new TimeProvider(Clock.fixed(Instant.parse("2026-08-21T00:00:00Z"), ZoneOffset.UTC));

    /** 조회 구간은 창이 정하고 step 은 설정이 정한다. 둘이 응답 meta 로 드러나야 화면이 축을 그린다. */
    @Test
    @DisplayName("조회 구간은 창 길이만큼 과거이고 step 은 설정값이다")
    void rangeFollowsWindowAndStep() {
        PrometheusSeriesProperties properties = PrometheusSeriesProperties.defaults();
        AdminMetricsSeriesResponse response = assembler(FakePromRangeQuery.alwaysOnePoint(), properties)
                .assemble(MetricsWindow.FIVE_MINUTES);

        assertThat(response.meta().rangeEnd()).isEqualTo(Instant.parse("2026-08-21T00:00:00Z"));
        assertThat(response.meta().rangeStart()).isEqualTo(Instant.parse("2026-08-20T23:55:00Z"));
        assertThat(response.meta().stepSeconds()).isEqualTo(properties.step().toSeconds());
        assertThat(response.window()).isEqualTo(MetricsWindow.FIVE_MINUTES);
    }

    /** 계열 네 종이 모두 자리를 갖는다. 하나라도 빠지면 화면이 패널을 못 만든다. */
    @Test
    @DisplayName("네 계열 종류가 모두 응답에 자리를 갖는다")
    void allFourKeysArePresent() {
        AdminMetricsSeriesResponse response =
                assembler(FakePromRangeQuery.alwaysOnePoint(), PrometheusSeriesProperties.defaults())
                        .assemble(MetricsWindow.ONE_MINUTE);

        assertThat(response.series()).extracting(SeriesEntry::key)
                .containsExactlyInAnyOrder(SeriesKey.values());
    }

    /**
     * <b>이 티켓의 핵심 계약이다.</b> 계열 하나가 죽어도 나머지는 그려져야 한다 — 한 질의에 여러
     * 계열을 담으면 이 성질이 성립하지 않는다.
     */
    @Test
    @DisplayName("계열 하나가 실패해도 나머지 계열은 VALID 로 나간다")
    void oneFailingSeriesDoesNotKillTheOthers() {
        AdminMetricsSeriesResponse response = assembler(
                FakePromRangeQuery.failingOnly(MetricAggregation.HTTP_LATENCY_SECONDS),
                PrometheusSeriesProperties.defaults())
                .assemble(MetricsWindow.ONE_MINUTE);

        assertThat(stateOf(response, SeriesKey.LATENCY_P99)).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(stateOf(response, SeriesKey.THROUGHPUT)).isEqualTo(SourceStatus.VALID);
        assertThat(stateOf(response, SeriesKey.FAILURE_RATE)).isEqualTo(SourceStatus.VALID);
        assertThat(stateOf(response, SeriesKey.CONSISTENCY_GAP)).isEqualTo(SourceStatus.VALID);
    }

    /** 원천이 죽은 것과 아직 표본이 없는 것은 운영자가 취할 행동이 반대라 뭉개면 안 된다. */
    @Test
    @DisplayName("일치하는 시계열이 없으면 UNAVAILABLE 이 아니라 PENDING 이다")
    void emptyMatrixIsPendingNotUnavailable() {
        AdminMetricsSeriesResponse response =
                assembler(FakePromRangeQuery.empty(), PrometheusSeriesProperties.defaults())
                        .assemble(MetricsWindow.ONE_MINUTE);

        assertThat(response.series()).allSatisfy(entry ->
                assertThat(entry.state()).isEqualTo(SourceStatus.PENDING));
    }

    /**
     * 예산이 뒤부터 자른다. 첫 계열은 예산과 무관하게 나가야 응답이 통째로 비지 않는다.
     *
     * <p>예산 0 은 "첫 질의 뒤 즉시 만료" 를 뜻한다 — 시간에 의존하지 않고 절단 순서만 본다.</p>
     */
    @Test
    @DisplayName("예산이 다하면 첫 계열만 나가고 나머지는 보내지 않는다")
    void budgetCutsLaterSeriesButNeverTheFirst() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        AdminMetricsSeriesResponse response = assembler(source, new PrometheusSeriesProperties(
                null, null, Duration.ofNanos(1), null, null)).assemble(MetricsWindow.ONE_MINUTE);

        assertThat(source.issued()).hasSize(1);
        // 우선순위 첫 칸은 정합성이다.
        assertThat(stateOf(response, SeriesKey.CONSISTENCY_GAP)).isEqualTo(SourceStatus.VALID);
        assertThat(stateOf(response, SeriesKey.THROUGHPUT)).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(stateOf(response, SeriesKey.FAILURE_RATE)).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(stateOf(response, SeriesKey.LATENCY_P99)).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 실패율은 분자와 분모가 한 질의 안에 있어야 두 응답이 다른 시점을 가리키지 않는다. */
    @Test
    @DisplayName("실패율 질의는 분자와 분모를 한 질의 안에서 나눈다")
    void failureRateDividesInsideOneQuery() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        assembler(source, PrometheusSeriesProperties.defaults()).assemble(MetricsWindow.ONE_MINUTE);

        // 문자열을 옮겨 적지 않는다 — 옮겨 적으면 분류가 늘어날 때 이 테스트가 옛 정의를 지킨다.
        String expected = ResultClass.promLabelAlternation(ResultClass.systemFailures());
        assertThat(source.issued()).anySatisfy(promQl -> {
            assertThat(promQl).contains(" / ");
            assertThat(promQl).contains(expected);
        });
    }

    /** rate 집계 창은 평가 간격과 같아야 이웃한 점이 표본을 겹쳐 세거나 비우지 않는다. */
    @Test
    @DisplayName("rate 집계 창은 step 과 같다")
    void rateWindowEqualsStep() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        PrometheusSeriesProperties properties = PrometheusSeriesProperties.defaults();
        assembler(source, properties).assemble(MetricsWindow.ONE_MINUTE);

        String range = "[" + properties.step().toSeconds() + "s]";
        assertThat(source.issued()).filteredOn(promQl -> promQl.startsWith("sum(rate("))
                .isNotEmpty()
                .allSatisfy(promQl -> assertThat(promQl).contains(range));
    }

    /**
     * 예산에 얼마나 근접했는지는 이 값으로만 보인다. 상수로 채우면 잘리기 시작해도 원인을 못 읽는다.
     * 시간에 의존하지 않도록 대역이 실제로 쓴 시간의 하한만 본다.
     */
    @Test
    @DisplayName("collectionDurationMs 는 실제로 쓴 시간을 예산과 같은 시계로 잰다")
    void collectionDurationReflectsRealElapsedTime() {
        Duration perQuery = Duration.ofMillis(20);
        AdminMetricsSeriesResponse response = assembler(
                FakePromRangeQuery.slow(perQuery), PrometheusSeriesProperties.defaults())
                .assemble(MetricsWindow.ONE_MINUTE);

        // 첫 계열은 예산과 무관하게 나가므로 최소 한 질의는 반드시 돈다.
        assertThat(response.meta().collectionDurationMs())
                .isGreaterThanOrEqualTo(perQuery.toMillis());
    }

    /**
     * LIVE 와 FINAL 은 같은 미터 이름으로 온다. 거르지 않으면 같은 gap 종류가 계열 둘로 그려지고
     * 화면이 부하 중 추세와 합격 판정을 섞어 읽는다 — 스냅샷 경로가 live() 로 가르는 것과 같다.
     */
    @Test
    @DisplayName("정합성 gap 질의는 LIVE 평가만 고른다")
    void consistencyGapQuerySelectsLivePhaseOnly() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        assembler(source, PrometheusSeriesProperties.defaults()).assemble(MetricsWindow.ONE_MINUTE);

        assertThat(source.issued())
                .filteredOn(promQl -> promQl.startsWith(MetricAggregation.CONSISTENCY_GAP))
                .singleElement()
                .satisfies(promQl -> assertThat(promQl).contains(
                        DomainMeterNames.TAG_PHASE + "=\"" + DomainMeterNames.PHASE_LIVE + "\""));
    }

    /** 이 티켓은 범위 셀렉터를 넣지 않는다. GLOBAL 을 명시해야 화면이 좁혀진 값으로 오해하지 않는다. */
    @Test
    @DisplayName("범위는 GLOBAL 로 명시되어 나간다")
    void scopeIsExplicitlyGlobal() {
        AdminMetricsSeriesResponse response =
                assembler(FakePromRangeQuery.alwaysOnePoint(), PrometheusSeriesProperties.defaults())
                        .assemble(MetricsWindow.ONE_MINUTE);

        assertThat(response.scope().type().name()).isEqualTo("GLOBAL");
        assertThat(response.scope().couponId()).isNull();
        assertThat(response.scope().benchmarkRunId()).isNull();
    }

    private static PromSeriesAssembler assembler(
            PromRangeQuery source, PrometheusSeriesProperties properties) {
        return new PromSeriesAssembler(source, FIXED_TIME, properties);
    }

    private static SourceStatus stateOf(AdminMetricsSeriesResponse response, SeriesKey key) {
        return response.series().stream()
                .filter(entry -> entry.key() == key)
                .findFirst()
                .orElseThrow(() -> new AssertionError("계열이 응답에 없습니다: " + key))
                .state();
    }
}
