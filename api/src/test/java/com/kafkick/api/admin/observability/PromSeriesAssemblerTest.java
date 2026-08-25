package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.Marker;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesEntry;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesKey;
import com.kafkick.api.observation.http.ResultClassifier.ResultClass;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
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
                .assemble(global(MetricsWindow.FIVE_MINUTES));

        assertThat(response.meta().rangeEnd()).isEqualTo(Instant.parse("2026-08-21T00:00:00Z"));
        assertThat(response.meta().rangeStart()).isEqualTo(Instant.parse("2026-08-20T23:55:00Z"));
        assertThat(response.meta().stepSeconds()).isEqualTo(properties.step().toSeconds());
        assertThat(response.window()).isEqualTo(MetricsWindow.FIVE_MINUTES);
    }

    /** 계열 종류가 모두 자리를 갖는다. 하나라도 빠지면 화면이 패널을 못 만든다. */
    @Test
    @DisplayName("모든 계열 종류가 각자 자리와 상태를 갖는다")
    void everyKeyIsPresentWithItsOwnState() {
        AdminMetricsSeriesResponse response =
                assembler(FakePromRangeQuery.alwaysOnePoint(), PrometheusSeriesProperties.defaults())
                        .assemble(global(MetricsWindow.ONE_MINUTE));

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
                .assemble(global(MetricsWindow.ONE_MINUTE));

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
                        .assemble(global(MetricsWindow.ONE_MINUTE));

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
                null, null, Duration.ofNanos(1), null, null))
                .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(source.issued()).hasSize(1);
        // 우선순위 첫 칸은 정합성이다.
        assertThat(stateOf(response, SeriesKey.CONSISTENCY_GAP)).isEqualTo(SourceStatus.VALID);
        assertThat(stateOf(response, SeriesKey.THROUGHPUT)).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(stateOf(response, SeriesKey.FAILURE_RATE)).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(stateOf(response, SeriesKey.QUEUE_ADMISSION)).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(stateOf(response, SeriesKey.IN_FLIGHT)).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(stateOf(response, SeriesKey.ERROR_CLASS_RATE)).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(stateOf(response, SeriesKey.FAILURE_REASON_RATE)).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(stateOf(response, SeriesKey.LATENCY_P99)).isEqualTo(SourceStatus.UNAVAILABLE);
        // 원천이 없는 계열은 애초에 질의를 보내지 않으므로 예산과 무관하게 PENDING 이다.
        assertThat(stateOf(response, SeriesKey.QUEUE_PERSISTENCE)).isEqualTo(SourceStatus.PENDING);
        // 기준선도 계열 뒤라 함께 잘린다.
        assertThat(response.markers()).isEmpty();
    }

    /** 실패율은 분자와 분모가 한 질의 안에 있어야 두 응답이 다른 시점을 가리키지 않는다. */
    @Test
    @DisplayName("실패율 질의는 분자와 분모를 한 질의 안에서 나눈다")
    void failureRateDividesInsideOneQuery() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        assembler(source, PrometheusSeriesProperties.defaults()).assemble(global(MetricsWindow.ONE_MINUTE));

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
        assembler(source, properties).assemble(global(MetricsWindow.ONE_MINUTE));

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
                .assemble(global(MetricsWindow.ONE_MINUTE));

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
        assembler(source, PrometheusSeriesProperties.defaults()).assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(source.issued())
                .filteredOn(promQl -> promQl.startsWith(MetricAggregation.CONSISTENCY_GAP))
                .singleElement()
                .satisfies(promQl -> assertThat(promQl).contains(
                        DomainMeterNames.TAG_PHASE + "=\"" + DomainMeterNames.PHASE_LIVE + "\""));
    }

    /**
     * 범위를 생략하면 GLOBAL 을 <b>명시</b>해야 한다. 키를 비우면 운영 Jackson(non_null)이 통째로
     * 지워 화면이 undefined 를 읽고, 좁혀진 값인지 아닌지를 말할 수 없게 된다.
     */
    @Test
    @DisplayName("범위를 생략하면 GLOBAL 로 명시되어 나간다")
    void scopeIsExplicitlyGlobal() {
        AdminMetricsSeriesResponse response =
                assembler(FakePromRangeQuery.alwaysOnePoint(), PrometheusSeriesProperties.defaults())
                        .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(response.scope().type().name()).isEqualTo("GLOBAL");
        assertThat(response.scope().couponId()).isNull();
        assertThat(response.scope().benchmarkRunId()).isNull();
    }

    /**
     * 원천이 없는 계열은 질의를 보내면 안 된다. 보내 봐야 빈 matrix 가 오고 예산만 쓴다 —
     * 그 예산은 값이 있는 계열의 것이다.
     */
    @Test
    @DisplayName("원천이 없는 계열은 질의를 보내지 않고 PENDING 으로 나간다")
    void seriesWithoutSourceCostNoQuery() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        AdminMetricsSeriesResponse response =
                assembler(source, PrometheusSeriesProperties.defaults())
                        .assemble(global(MetricsWindow.ONE_MINUTE));

        // 질의가 나가는 계열 아홉 + 기준선 하나. 이 수가 곧 예산 실측의 모집단이다.
        // OBS-46 이 지연 축을 둘로 갈라 하나 늘었다.
        assertThat(source.issued()).hasSize(10);
        assertThat(stateOf(response, SeriesKey.QUEUE_PERSISTENCE)).isEqualTo(SourceStatus.PENDING);
        assertThat(stateOf(response, SeriesKey.QUEUE_TELEMETRY)).isEqualTo(SourceStatus.PENDING);
        assertThat(pointsOf(response, SeriesKey.QUEUE_PERSISTENCE)).isEmpty();
    }

    /**
     * 기준선은 점 규격이 계열과 다르다. 계열 배열에 섞으면 {@code value} 자리가 비어 화면이
     * 0 으로 그린다.
     */
    @Test
    @DisplayName("재고가 0 이 되면 기준선이 서고 계열 배열에는 섞이지 않는다")
    void stockExhaustionBecomesMarkerNotSeries() {
        Instant exhaustedAt = Instant.parse("2026-08-20T23:59:30Z");
        FakePromRangeQuery source = FakePromRangeQuery.pointsFor(
                MetricAggregation.promName(DomainMeterNames.STOCK_REMAINING),
                List.of(new PromRangePoint(Instant.parse("2026-08-20T23:59:25Z"), 7d),
                        new PromRangePoint(exhaustedAt, 0d),
                        new PromRangePoint(Instant.parse("2026-08-20T23:59:35Z"), 0d)));

        AdminMetricsSeriesResponse response = assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(response.markers()).singleElement().satisfies(marker -> {
            assertThat(marker.at()).isEqualTo(exhaustedAt);
            assertThat(marker.label()).isEqualTo(Marker.STOCK_EXHAUSTED);
        });
        assertThat(response.series()).extracting(SeriesEntry::key)
                .doesNotContain((SeriesKey) null)
                .containsExactlyInAnyOrder(SeriesKey.values());
    }

    /**
     * 구간이 시작될 때 이미 0 이면 소진 시각은 이 구간 밖이다. 첫 점에 선을 그으면 없는 사건을
     * 만든다 — 운영자는 그 시각에 무슨 일이 있었는지를 찾게 된다.
     */
    @Test
    @DisplayName("구간 시작부터 재고가 0 이면 기준선을 세우지 않는다")
    void exhaustionBeforeTheRangeIsNotDated() {
        FakePromRangeQuery source = FakePromRangeQuery.pointsFor(
                MetricAggregation.promName(DomainMeterNames.STOCK_REMAINING),
                List.of(new PromRangePoint(Instant.parse("2026-08-20T23:59:25Z"), 0d),
                        new PromRangePoint(Instant.parse("2026-08-20T23:59:30Z"), 0d)));

        assertThat(assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE)).markers()).isEmpty();
    }

    /**
     * NaN 은 "재고가 0" 이 아니라 "이유는 상태 미터가 낸다" 는 표시다. 소진으로 읽으면 수집이
     * 끊긴 순간마다 없는 기준선이 선다.
     */
    @Test
    @DisplayName("재고 표본의 NaN 은 소진이 아니다")
    void nanStockIsNotExhaustion() {
        FakePromRangeQuery source = FakePromRangeQuery.pointsFor(
                MetricAggregation.promName(DomainMeterNames.STOCK_REMAINING),
                List.of(new PromRangePoint(Instant.parse("2026-08-20T23:59:25Z"), 7d),
                        new PromRangePoint(Instant.parse("2026-08-20T23:59:30Z"), Double.NaN)));

        assertThat(assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE)).markers()).isEmpty();
    }

    /**
     * 실패 분류 계열은 성공이 아닌 네 분류를 모두 펴야 한다. 정책 거절과 클라이언트 오류를 빼면
     * 그 트래픽이 어디로 갔는지 아무도 설명하지 못한다.
     */
    @Test
    @DisplayName("실패 분류 질의는 성공이 아닌 네 분류를 모두 담고 분모를 한 질의에서 나눈다")
    void errorClassQueryCoversEveryNonSuccessClass() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE));

        // 문자열을 옮겨 적지 않는다. 분류가 늘면 이 기대값도 함께 늘어야 한다.
        String expected = ResultClass.promLabelAlternation(EnumSet.copyOf(
                Arrays.stream(ResultClass.values()).filter(value -> !value.isSuccess()).toList()));
        assertThat(source.issued())
                .filteredOn(promQl -> promQl.contains("sum by (result)"))
                .singleElement()
                .satisfies(promQl -> {
                    assertThat(promQl).contains(expected);
                    assertThat(promQl).contains(" / on() group_left ");
                    assertThat(promQl).endsWith(" * 100");
                });
    }

    /**
     * 회차는 라벨이 아니라 값이라 셀렉터로 자를 수 없다. 도메인 계열만 회차 식별자 미터로
     * 걸리고, 회차 라벨이 없는 HTTP 계열은 전역 값이 그대로 나간다 — /metrics 와 같은 동작이다.
     */
    @Test
    @DisplayName("couponId 는 회차 식별자 미터가 있는 계열에만 걸린다")
    void couponScopeGatesOnlyDomainSeries() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        AdminMetricsSeriesResponse response = assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(new MetricsQuery(MetricsWindow.ONE_MINUTE, 42L, null));

        assertThat(response.scope().couponId()).isEqualTo(42L);
        assertThat(source.issued())
                .filteredOn(promQl -> promQl.startsWith("(" + MetricAggregation.CONSISTENCY_GAP))
                .singleElement()
                .satisfies(promQl -> assertThat(promQl)
                        .endsWith("and on() (" + MetricAggregation.CONSISTENCY_COUPON_ID + " == 42)"));
        assertThat(source.issued())
                .filteredOn(promQl -> promQl.startsWith("(" + MetricAggregation.QUEUE_LENGTH))
                .singleElement()
                .satisfies(promQl -> assertThat(promQl)
                        .endsWith("and on() (" + MetricAggregation.OBSERVED_COUPON_ID + " == 42)"));
        // HTTP 계열은 좁혀지지 않는다. 좁혀진 척하면 화면이 전역 값을 회차 값으로 읽는다.
        assertThat(source.issued())
                .filteredOn(promQl -> promQl.contains(MetricAggregation.HTTP_RESULT_TOTAL))
                .isNotEmpty()
                .allSatisfy(promQl -> assertThat(promQl).doesNotContain("and on()"));
    }

    /**
     * <b>응답 meta 는 증거가 아니다.</b> meta 의 rangeStart·rangeEnd·stepSeconds 는 조립기가
     * 질의에 쓴 것과 <b>같은 지역 변수</b>를 되비칠 뿐이라, 어느 계열에만 다른 구간을 넘겨도
     * meta 는 멀쩡하다. 실제로 원천에 나간 인자를 봐야 한다.
     */
    @Test
    @DisplayName("모든 질의가 같은 조회 구간과 간격으로 나간다")
    void everyQueryUsesTheSameRangeAndStep() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        PrometheusSeriesProperties properties = PrometheusSeriesProperties.defaults();
        AdminMetricsSeriesResponse response =
                assembler(source, properties).assemble(global(MetricsWindow.FIVE_MINUTES));

        Instant end = Instant.parse("2026-08-21T00:00:00Z");
        Instant start = end.minus(MetricsWindow.FIVE_MINUTES.duration());
        assertThat(source.requests()).isNotEmpty().allSatisfy(request -> {
            assertThat(request.start()).isEqualTo(start);
            assertThat(request.end()).isEqualTo(end);
            assertThat(request.step()).isEqualTo(properties.step());
        });
        // meta 와 실제 인자가 같은 값이어야 화면의 축과 원천의 구간이 어긋나지 않는다.
        assertThat(response.meta().rangeStart()).isEqualTo(start);
        assertThat(response.meta().rangeEnd()).isEqualTo(end);
        assertThat(response.meta().stepSeconds()).isEqualTo(properties.step().toSeconds());
    }

    /**
     * 두 경로가 같은 미터를 다른 모집단으로 접으면 현재값과 추세선이 어긋난다. 스냅샷의
     * {@code saturation.inFlight.globalSum} 은 {@code job="api"} 로 접은 값이다 — batch 도 같은
     * 이름의 미터를 낼 수 있으므로 필터를 빼면 관측기 자신의 수치가 추세선에만 섞인다.
     */
    @Test
    @DisplayName("in-flight 질의는 스냅샷과 같은 job=api 모집단을 쓴다")
    void inFlightQueryMatchesSnapshotPopulation() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(source.issued())
                .filteredOn(promQl -> promQl.contains(MetricAggregation.HTTP_IN_FLIGHT))
                .singleElement()
                .satisfies(promQl -> assertThat(promQl).isEqualTo(
                        "sum(" + MetricAggregation.HTTP_IN_FLIGHT + "{job=\"api\"})"));
    }

    /**
     * 반대 방향이다. 대기열은 batch 가 유일한 원천이라 {@code job} 을 걸면 표본이 통째로
     * 사라진다 — in-flight 와 같은 필터를 무심코 복사하면 이 계열이 영원히 PENDING 이 된다.
     */
    @Test
    @DisplayName("대기열 질의에는 job 셀렉터가 없다")
    void admissionQueueQueryCarriesNoJobSelector() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(source.issued())
                .filteredOn(promQl -> promQl.contains(MetricAggregation.QUEUE_LENGTH))
                .singleElement()
                .satisfies(promQl -> assertThat(promQl)
                        .isEqualTo(MetricAggregation.QUEUE_LENGTH));
    }

    /**
     * <b>우선순위가 이 티켓의 설계 판단이다.</b> 예산이 모자라면 뒤가 잘리므로 순서가 곧 "무엇을
     * 포기하는가" 이고, 순서를 조용히 바꾸면 평소엔 아무 일도 없다가 부하 때만 다른 계열이 빈다 —
     * 정확히 이 화면이 필요한 순간에. 그래서 상태가 아니라 <b>질의가 나간 순서</b>를 고정한다.
     *
     * <p>근거는 조립기 주석에 있다. 요약하면 부하 중에만 값이 생기는 계열(대기열 · in-flight)이
     * 앞이고, 스냅샷으로 대체할 수 있거나 잘려도 다른 값의 해석을 바꾸지 않는 계열이 뒤다.</p>
     */
    @Test
    @DisplayName("질의는 정해진 우선순위 순서로 나간다")
    void priorityOrderIsPinned() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE));

        List<String> order = List.of(
                MetricAggregation.CONSISTENCY_GAP,
                MetricAggregation.QUEUE_LENGTH,
                MetricAggregation.HTTP_IN_FLIGHT,
                "sum(rate(",
                " / sum(rate(",
                "sum by (result)",
                OverviewPrometheusContract.OUTCOME_TOTAL,
                // 지연 축 둘. 성공이 먼저다 — 시스템 실패 축만 남으면 비교 대상이 없다.
                MetricAggregation.HTTP_LATENCY_SECONDS,
                MetricAggregation.HTTP_LATENCY_SECONDS,
                MetricAggregation.promName(DomainMeterNames.STOCK_REMAINING));
        assertThat(source.issued()).hasSameSizeAs(order);
        for (int i = 0; i < order.size(); i++) {
            assertThat(source.issued().get(i))
                    .as("우선순위 %d 번째", i + 1)
                    .contains(order.get(i));
        }
    }

    /**
     * <b>봉투의 {@code scope} 만 보면 화면이 조용히 틀린다.</b> COUPON 을 주면 봉투에는 COUPON 이
     * 실리지만 회차로 좁혀지는 것은 회차 식별자 미터를 함께 내는 도메인 계열뿐이다 — HTTP 미터
     * 계열에 회차 표식을 달면 전역 처리량이 회차 값으로 읽힌다. 어느 계열이 실제로 좁혀졌는지는
     * 계열마다 붙는 이 값이 정본이다.
     */
    @Test
    @DisplayName("COUPON 범위에서 실제로 좁혀진 계열만 scoped 다")
    void onlyGatedSeriesReportScoped() {
        AdminMetricsSeriesResponse response =
                assembler(FakePromRangeQuery.alwaysOnePoint(), PrometheusSeriesProperties.defaults())
                        .assemble(new MetricsQuery(MetricsWindow.ONE_MINUTE, 42L, null));

        assertThat(response.series()).filteredOn(SeriesEntry::scoped)
                .extracting(SeriesEntry::key)
                .containsOnly(SeriesKey.CONSISTENCY_GAP, SeriesKey.QUEUE_ADMISSION);
        assertThat(response.series()).filteredOn(entry -> !entry.scoped())
                .extracting(SeriesEntry::key)
                .doesNotContain(SeriesKey.CONSISTENCY_GAP, SeriesKey.QUEUE_ADMISSION)
                .isNotEmpty();
    }

    /** GLOBAL 요청에는 좁혀진 계열이 있을 수 없다. 하나라도 true 면 요청하지 않은 필터가 걸린 것이다. */
    @Test
    @DisplayName("GLOBAL 범위에서는 어떤 계열도 scoped 가 아니다")
    void globalScopeMarksNothingAsScoped() {
        AdminMetricsSeriesResponse response =
                assembler(FakePromRangeQuery.alwaysOnePoint(), PrometheusSeriesProperties.defaults())
                        .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(response.series()).noneMatch(SeriesEntry::scoped);
    }

    /**
     * 상태가 scoped 를 뒤집으면 화면이 같은 계열을 폴링마다 다르게 라벨링한다. 예산에 잘린
     * 계열도 "보냈다면 걸렸을 값" 을 그대로 실어야 한다.
     */
    @Test
    @DisplayName("예산에 잘린 계열도 scoped 를 그대로 싣는다")
    void budgetCutKeepsScopedFlag() {
        AdminMetricsSeriesResponse response = assembler(
                FakePromRangeQuery.alwaysOnePoint(), new PrometheusSeriesProperties(
                        null, null, Duration.ofNanos(1), null, null))
                .assemble(new MetricsQuery(MetricsWindow.ONE_MINUTE, 42L, null));

        assertThat(response.series()).filteredOn(SeriesEntry::scoped)
                .extracting(SeriesEntry::key)
                .containsOnly(SeriesKey.CONSISTENCY_GAP, SeriesKey.QUEUE_ADMISSION);
        // 잘린 것은 맞다 — 플래그가 상태와 독립이라는 것이 이 테스트의 요지다.
        assertThat(stateOf(response, SeriesKey.QUEUE_ADMISSION)).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /**
     * 기준선도 회차로 좁혀야 한다. 좁히지 않으면 회차 화면에 <b>다른 회차의</b> 재고 소진 시각이
     * 세로선으로 그어지고, 운영자는 이 회차에 없던 사건을 찾게 된다.
     */
    @Test
    @DisplayName("기준선 질의에도 회차 게이트가 걸린다")
    void markerQueryIsGatedByCoupon() {
        FakePromRangeQuery source = FakePromRangeQuery.alwaysOnePoint();
        assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(new MetricsQuery(MetricsWindow.ONE_MINUTE, 42L, null));

        String stock = MetricAggregation.promName(DomainMeterNames.STOCK_REMAINING);
        assertThat(source.issued())
                .filteredOn(promQl -> promQl.contains(stock))
                .singleElement()
                .satisfies(promQl -> assertThat(promQl).isEqualTo(
                        "(" + stock + ") and on() ("
                                + MetricAggregation.OBSERVED_COUPON_ID + " == 42)"));
    }

    /**
     * <b>빈 기준선 목록의 이유가 상태에만 있다.</b> 예산에 잘리는 것은 부하가 걸린 회차 —
     * 재고 소진 시각이 가장 궁금한 바로 그 회차다. 목록만 보면 "소진이 없었다" 와 같은 모양이라
     * 운영자가 용량이 충분했다고 잘못 읽는다.
     */
    @Test
    @DisplayName("예산에 잘린 기준선은 빈 목록이 아니라 UNAVAILABLE 이다")
    void budgetCutMarkersAreUnavailableNotEmpty() {
        AdminMetricsSeriesResponse response = assembler(
                FakePromRangeQuery.alwaysOnePoint(), new PrometheusSeriesProperties(
                        null, null, Duration.ofNanos(1), null, null))
                .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(response.markers()).isEmpty();
        assertThat(response.markersState()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 원천이 죽은 것과 소진이 없었던 것도 반대 행동을 부른다. */
    @Test
    @DisplayName("기준선 질의가 실패하면 UNAVAILABLE 이다")
    void failingMarkerQueryIsUnavailable() {
        AdminMetricsSeriesResponse response = assembler(
                FakePromRangeQuery.down(), PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(response.markers()).isEmpty();
        assertThat(response.markersState()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 재고 표본이 아직 없는 것은 장애가 아니다. */
    @Test
    @DisplayName("재고 표본이 없으면 기준선은 PENDING 이다")
    void missingStockSamplesMakeMarkersPending() {
        AdminMetricsSeriesResponse response = assembler(
                FakePromRangeQuery.empty(), PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(response.markersState()).isEqualTo(SourceStatus.PENDING);
    }

    /** 물어봤고 소진이 없었던 것은 VALID 다. 빈 목록이 사실이라는 뜻이다. */
    @Test
    @DisplayName("소진이 없었으면 빈 목록이지만 VALID 다")
    void noExhaustionIsValidEmptyList() {
        FakePromRangeQuery source = FakePromRangeQuery.pointsFor(
                MetricAggregation.promName(DomainMeterNames.STOCK_REMAINING),
                List.of(new PromRangePoint(Instant.parse("2026-08-20T23:59:25Z"), 7d),
                        new PromRangePoint(Instant.parse("2026-08-20T23:59:30Z"), 5d)));

        AdminMetricsSeriesResponse response = assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(response.markers()).isEmpty();
        assertThat(response.markersState()).isEqualTo(SourceStatus.VALID);
    }

    /**
     * 재고 미터는 원천이 batch 하나여야 한다. 갈렸다면 어느 쪽 소진 시각인지 고를 근거가 없다 —
     * 하나를 골라 그으면 다른 회차의 시각을 이 회차에 붙인다.
     */
    @Test
    @DisplayName("재고 시계열이 둘 이상이면 기준선을 고르지 않고 UNAVAILABLE 이다")
    void ambiguousStockSeriesYieldsUnavailableMarkers() {
        FakePromRangeQuery source = FakePromRangeQuery.multipleSeriesFor(
                MetricAggregation.promName(DomainMeterNames.STOCK_REMAINING),
                List.of(Map.of("__name__", "any", "instance", "batch-a"),
                        Map.of("__name__", "any", "instance", "batch-b")));

        AdminMetricsSeriesResponse response = assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(response.markers()).isEmpty();
        assertThat(response.markersState()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /**
     * 라벨이 계열을 가른다. {@code __name__} 은 계열 종류가 이미 담고 있어 빼지만 나머지는 남아야
     * 화면이 같은 종류의 계열 여럿을 구분한다 — 다 빼면 네 분류가 한 이름 없는 선으로 겹친다.
     */
    @Test
    @DisplayName("라벨에서 __name__ 만 빠지고 나머지는 남는다")
    void displayLabelsDropOnlyTheMetricName() {
        FakePromRangeQuery source = FakePromRangeQuery.multipleSeriesFor(
                "sum by (result)",
                List.of(Map.of("__name__", "app_http_result_total", "result", "policy_reject"),
                        Map.of("__name__", "app_http_result_total", "result", "client_invalid")));

        AdminMetricsSeriesResponse response = assembler(source, PrometheusSeriesProperties.defaults())
                .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(response.series())
                .filteredOn(entry -> entry.key() == SeriesKey.ERROR_CLASS_RATE)
                .hasSize(2)
                .allSatisfy(entry -> assertThat(entry.labels()).doesNotContainKey("__name__"))
                .extracting(entry -> entry.labels().get("result"))
                .containsExactlyInAnyOrder("policy_reject", "client_invalid");
    }

    /**
     * <b>원천이 생겨도 아무도 이 자리를 안 고치는 것을 막는다.</b> 원천이 없어 질의를 보내지 않는
     * 계열은 정상 원천에서도 PENDING 으로 남는다. OBS-15 가 Kafka lag 을 열면 이 목록이 줄어야
     * 하고, 줄이지 않으면 이 테스트가 red 가 되어 배선을 잊은 것이 드러난다.
     */
    @Test
    @DisplayName("질의 없이 PENDING 인 계열은 원천이 없다고 선언한 둘뿐이다")
    void onlyDeclaredSourcelessSeriesStayPending() {
        AdminMetricsSeriesResponse response =
                assembler(FakePromRangeQuery.alwaysOnePoint(), PrometheusSeriesProperties.defaults())
                        .assemble(global(MetricsWindow.ONE_MINUTE));

        assertThat(response.series())
                .filteredOn(entry -> entry.state() == SourceStatus.PENDING)
                .extracting(SeriesEntry::key)
                // QUEUE_PERSISTENCE 는 OBS-15(Kafka consumer lag), QUEUE_TELEMETRY 는 서버가 잴 수
                // 없는 정의라 원천이 없다. 여기 목록이 곧 "아직 배선하지 않았다" 는 선언이다.
                .containsExactlyInAnyOrder(SeriesKey.QUEUE_PERSISTENCE, SeriesKey.QUEUE_TELEMETRY);
    }

    private static List<AdminMetricsSeriesResponse.SeriesPoint> pointsOf(
            AdminMetricsSeriesResponse response, SeriesKey key) {
        return response.series().stream()
                .filter(entry -> entry.key() == key)
                .findFirst()
                .orElseThrow(() -> new AssertionError("계열이 응답에 없습니다: " + key))
                .points();
    }

    private static MetricsQuery global(MetricsWindow window) {
        return new MetricsQuery(window, null, null);
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
