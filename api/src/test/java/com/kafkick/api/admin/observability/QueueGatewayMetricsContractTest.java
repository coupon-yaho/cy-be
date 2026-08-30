package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.QueueMetric;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.QueueZone;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesEntry;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse.SeriesKey;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

class QueueGatewayMetricsContractTest {

    private static final Instant NOW = Instant.parse("2026-08-31T04:00:00Z");
    private static final TimeProvider TIME =
            new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void globalWaitingUsesTheMaximumReplicaGaugeAndOldestObservationTime() {
        ObservedValue<Double> waiting = waiting(globalQuery(), List.of(
                sample(QueueGatewayPrometheusContract.WAITING, 100d),
                sample(QueueGatewayPrometheusContract.WAITING, 120d),
                sample(QueueGatewayPrometheusContract.SNAPSHOT_AGE, 1d),
                sample(QueueGatewayPrometheusContract.SCRAPE_AGE, 2d),
                up(1d)));

        assertThat(waiting.value()).isEqualTo(120d);
        assertThat(waiting.state()).isEqualTo(SourceStatus.VALID);
        assertThat(waiting.observedAt()).isEqualTo(NOW.minusSeconds(2));
    }

    @Test
    void freshZeroIsNoTrafficAndOldSnapshotCarriesAStaleValue() {
        assertThat(waiting(globalQuery(), gatewaySamples(0d, 1d, 1d)).state())
                .isEqualTo(SourceStatus.NO_TRAFFIC);

        ObservedValue<Double> stale = waiting(globalQuery(), gatewaySamples(120d, 6d, 1d));
        assertThat(stale.value()).isEqualTo(120d);
        assertThat(stale.state()).isEqualTo(SourceStatus.STALE);
        assertThat(stale.observedAt()).isEqualTo(NOW.minusSeconds(6));
    }

    @Test
    void missingMaterialsArePendingButTargetDownIsUnavailable() {
        assertThat(waiting(globalQuery(), List.of(
                sample(QueueGatewayPrometheusContract.WAITING, 120d), up(1d))).state())
                .isEqualTo(SourceStatus.PENDING);
        assertThat(waiting(globalQuery(), gatewaySamples(120d, -1d, 1d)).state())
                .isEqualTo(SourceStatus.PENDING);
        assertThat(waiting(globalQuery(), gatewaySamples(120d, 1d, 0d)).state())
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(waiting(globalQuery(), List.of(
                sample(QueueGatewayPrometheusContract.SNAPSHOT_AGE, 1d), up(1d))).state())
                .isEqualTo(SourceStatus.PENDING);
    }

    @Test
    void couponScopeStaysPendingBecauseTheGuideDefinesNoCouponLabel() {
        ObservedValue<Double> waiting = waiting(
                new MetricsQuery(MetricsWindow.ONE_MINUTE, 10L, null),
                gatewaySamples(120d, 1d, 1d));

        assertThat(waiting.state()).isEqualTo(SourceStatus.PENDING);
        assertThat(waiting.value()).isNull();
    }

    @Test
    void undocumentedAdmissionValuesRemainPending() {
        PromQuery client = promQl -> {
            if (promQl.contains(QueueGatewayPrometheusContract.WAITING)) {
                return gatewaySamples(120d, 1d, 1d);
            }
            if (promQl.startsWith("rate(")) {
                return List.of(new PromSample(MetricAggregation.HTTP_RESULT_TOTAL,
                        Map.of("uri_group", "entry", "result", "queue_accepted",
                                "instance", "api-1"), 7d, NOW));
            }
            if (promQl.startsWith("max(time() - timestamp(")) {
                return List.of(new PromSample("age", Map.of(), 1d, NOW));
            }
            return List.of();
        };
        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.saturation().queues().stream()
                .filter(zone -> zone.zone() == QueueZone.ADMISSION)
                .flatMap(zone -> zone.metrics().stream())
                .filter(metric -> !QueueMetric.ADMISSION_WAITING.equals(metric.label()))
                .map(metric -> metric.value().state()))
                .containsOnly(SourceStatus.PENDING);
    }

    @Test
    void gatewayQueryFailureIsIsolatedAsUnavailable() {
        PromQuery client = promQl -> {
            if (promQl.contains(QueueGatewayPrometheusContract.WAITING)) {
                throw new PromQueryException("prometheus down");
            }
            return List.of();
        };

        assertThat(waiting(assemble(client, globalQuery())).state())
                .isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    void globalSeriesUsesOneGatewayRangeQueryAndCarriesStaleStatus() {
        List<String> queries = new ArrayList<>();
        PromRangeQuery range = (promQl, start, end, step) -> {
            queries.add(promQl);
            if (!promQl.contains(QueueGatewayPrometheusContract.WAITING)) {
                return List.of(new PromRangeSeries(Map.of(),
                        List.of(new PromRangePoint(NOW, 1d))));
            }
            return List.of(
                    range("waiting", 100d, 120d),
                    range("snapshot_age", 6d),
                    range("scrape_age", 1d),
                    range("up", 1d));
        };

        AdminMetricsSeriesResponse response = new PromSeriesAssembler(
                range, TIME, PrometheusSeriesProperties.defaults(),
                new QueueGatewayPrometheusProperties(true, Duration.ofSeconds(5)))
                .assemble(globalQuery());
        SeriesEntry admission = response.series().stream()
                .filter(entry -> entry.key() == SeriesKey.QUEUE_ADMISSION)
                .findFirst().orElseThrow();

        assertThat(admission.state()).isEqualTo(SourceStatus.STALE);
        assertThat(admission.points()).extracting(point -> point.value())
                .containsExactly(100d, 120d);
        assertThat(queries.stream().filter(query -> query.contains(
                QueueGatewayPrometheusContract.WAITING))).hasSize(1);
    }

    @Test
    void couponSeriesStaysPendingWithoutIssuingAGlobalGatewayQuery() {
        List<String> queries = new ArrayList<>();
        PromRangeQuery range = (promQl, start, end, step) -> {
            queries.add(promQl);
            return List.of(new PromRangeSeries(Map.of(), List.of(new PromRangePoint(NOW, 1d))));
        };
        MetricsQuery coupon = new MetricsQuery(MetricsWindow.ONE_MINUTE, 10L, null);

        AdminMetricsSeriesResponse response = new PromSeriesAssembler(
                range, TIME, PrometheusSeriesProperties.defaults(),
                new QueueGatewayPrometheusProperties(true, Duration.ofSeconds(5)))
                .assemble(coupon);
        SeriesEntry admission = response.series().stream()
                .filter(entry -> entry.key() == SeriesKey.QUEUE_ADMISSION)
                .findFirst().orElseThrow();

        assertThat(admission.state()).isEqualTo(SourceStatus.PENDING);
        assertThat(queries).noneMatch(query -> query.contains(
                QueueGatewayPrometheusContract.WAITING));
    }

    private static ObservedValue<Double> waiting(MetricsQuery query, List<PromSample> gateway) {
        PromQuery client = promQl -> promQl.contains(QueueGatewayPrometheusContract.WAITING)
                ? gateway : List.of();
        return waiting(assemble(client, query));
    }

    private static AdminMetricsResponse assemble(PromQuery client, MetricsQuery query) {
        return new PromMetricsAssembler(
                client, TIME, Duration.ofSeconds(120), Duration.ofMillis(900),
                new QueueGatewayPrometheusProperties(true, Duration.ofSeconds(5)))
                .assemble(query);
    }

    private static ObservedValue<Double> waiting(AdminMetricsResponse response) {
        QueueMetric metric = response.saturation().queues().stream()
                .filter(zone -> zone.zone() == QueueZone.ADMISSION)
                .flatMap(zone -> zone.metrics().stream())
                .filter(candidate -> QueueMetric.ADMISSION_WAITING.equals(candidate.label()))
                .findFirst()
                .orElseThrow();
        return metric.value();
    }

    private static List<PromSample> gatewaySamples(double waiting, double age, double up) {
        List<PromSample> samples = new ArrayList<>();
        samples.add(sample(QueueGatewayPrometheusContract.WAITING, waiting));
        samples.add(sample(QueueGatewayPrometheusContract.SNAPSHOT_AGE, age));
        samples.add(sample(QueueGatewayPrometheusContract.SCRAPE_AGE, 1d));
        samples.add(up(up));
        return List.copyOf(samples);
    }

    private static PromSample sample(String name, double value) {
        return new PromSample(name, Map.of("job", QueueGatewayPrometheusContract.JOB), value, NOW);
    }

    private static PromSample up(double value) {
        return sample(MetricAggregation.UP, value);
    }

    private static PromRangeSeries range(String signal, double... values) {
        List<PromRangePoint> points = new ArrayList<>();
        for (int index = 0; index < values.length; index++) {
            points.add(new PromRangePoint(NOW.minusSeconds(values.length - index - 1L), values[index]));
        }
        return new PromRangeSeries(Map.of("signal", signal), points);
    }

    private static MetricsQuery globalQuery() {
        return new MetricsQuery(MetricsWindow.ONE_MINUTE, null, null);
    }
}
