package com.kafkick.api.admin.observability.mockserver;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.kafkick.api.admin.observability.MetricAggregation;
import com.kafkick.api.admin.observability.PromQuery;
import com.kafkick.api.admin.observability.PromQueryException;
import com.kafkick.api.admin.observability.PromSample;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;

public final class MockPromQuery implements PromQuery {

    private static final int INSTANCE_COUNT = 4;
    private static final long CYCLE_SECONDS = 120;

    private final String scenario;
    private final long startedAtNanos;

    public MockPromQuery(String scenario, long startedAtNanos) {
        this.scenario = scenario == null ? "" : scenario;
        this.startedAtNanos = startedAtNanos;
    }

    @Override
    public List<PromSample> query(String promQl) {
        if ("promDown".equals(scenario)) {
            throw new PromQueryException("mock scenario: Prometheus unavailable");
        }
        if (promQl.contains(MetricAggregation.CONSISTENCY_GAP)
                || promQl.contains(MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH)) {
            delayForBudgetScenario();
            return consistencySamples();
        }
        if (promQl.contains(MetricAggregation.HTTP_LATENCY_SECONDS)
                && promQl.contains("quantile")) {
            return latencySamples();
        }
        if (promQl.contains(MetricAggregation.HTTP_RESULT_TOTAL)
                && promQl.contains("rate(")) {
            return resultRateSamples();
        }
        if (promQl.contains("time() - timestamp")) {
            return freshnessSamples();
        }
        String message = "지원하지 않는 Prometheus 질의입니다: " + promQl;
        System.err.println(message);
        throw new PromQueryException(message);
    }

    private void delayForBudgetScenario() {
        if (!"budget".equals(scenario)) {
            return;
        }
        try {
            Thread.sleep(600);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new PromQueryException("mock budget delay interrupted", interrupted);
        }
    }

    private List<PromSample> consistencySamples() {
        Instant now = Instant.now();
        Instant observedAt = observedAt(now);
        Map<String, String> live = Map.of(DomainMeterNames.TAG_PHASE, DomainMeterNames.PHASE_LIVE);
        List<PromSample> samples = new ArrayList<>();
        for (ConsistencyGapType gap : ConsistencyGapType.values()) {
            Map<String, String> labels = Map.of(
                    DomainMeterNames.TAG_PHASE, DomainMeterNames.PHASE_LIVE,
                    DomainMeterNames.TAG_GAP_TYPE, DomainMeterNames.gapTagValue(gap));
            samples.add(sample(MetricAggregation.CONSISTENCY_GAP, labels, gap.ordinal(), now));
            samples.add(sample(MetricAggregation.CONSISTENCY_GAP_STATE, labels,
                    SourceStatusCode.of(SourceStatus.VALID), now));
        }
        samples.add(sample(MetricAggregation.OVER_ISSUED, live, phaseSeconds() >= 45 ? 2 : 0, now));
        samples.add(sample(MetricAggregation.OVER_ISSUED_STATE, live,
                SourceStatusCode.of(SourceStatus.VALID), now));
        samples.add(sample(MetricAggregation.CONSISTENCY_SEVERITY, live,
                phaseSeconds() >= 45 ? 1 : 0, now));
        samples.add(sample(MetricAggregation.CONSISTENCY_SEVERITY_STATE, live,
                SourceStatusCode.of(SourceStatus.VALID), now));
        samples.add(sample(MetricAggregation.CONSISTENCY_COUPON_ID, Map.of(), 1, now));
        samples.add(sample(MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH,
                Map.of(DomainMeterNames.TAG_COLLECT_PATH, DomainMeterNames.PATH_CONSISTENCY),
                observedAt.toEpochMilli() / 1000d, now));
        return samples;
    }

    private List<PromSample> latencySamples() {
        Instant now = Instant.now();
        double load = totalRps();
        List<PromSample> samples = new ArrayList<>();
        String[] quantiles = {"0.5", "0.95", "0.99"};
        for (int instance = 1; instance <= INSTANCE_COUNT; instance++) {
            for (int quantile = 0; quantile < quantiles.length; quantile++) {
                double seconds = (0.012 + quantile * 0.018) * (1 + load / 10_000d)
                        * (1 + instance * 0.04);
                samples.add(sample(MetricAggregation.HTTP_LATENCY_SECONDS, Map.of(
                        "uri_group", "issue",
                        "outcome", "success",
                        "result", "success",
                        "quantile", quantiles[quantile],
                        "instance", instanceName(instance)), seconds, now));
            }
        }
        return samples;
    }

    private List<PromSample> resultRateSamples() {
        Instant now = Instant.now();
        double total = totalRps();
        double policyReject = policyRejectRps(total);
        double clientInvalid = total * 0.012;
        double dependencyFailure = total * 0.004;
        double applicationFailure = total * 0.002;
        double success = Math.max(0, total * 0.93);
        double queueAccepted = total * 0.052;

        List<PromSample> samples = new ArrayList<>();
        addAcrossInstances(samples, "issue", "success", success, now);
        addAcrossInstances(samples, "issue", "policy_reject", policyReject, now);
        addAcrossInstances(samples, "issue", "client_invalid", clientInvalid, now);
        addAcrossInstances(samples, "issue", "dependency_failure", dependencyFailure, now);
        addAcrossInstances(samples, "issue", "application_failure", applicationFailure, now);
        addAcrossInstances(samples, "entry", "queue_accepted", queueAccepted, now);
        addAcrossInstances(samples, "queue", "success", total * 0.02, now);
        addAcrossInstances(samples, "read", "success", total * 0.03, now);
        addAcrossInstances(samples, "use", "success", total * 0.01, now);
        return samples;
    }

    private void addAcrossInstances(
            List<PromSample> samples, String uriGroup, String result, double total, Instant now) {
        double weightSum = 10d;
        for (int instance = 1; instance <= INSTANCE_COUNT; instance++) {
            samples.add(sample(MetricAggregation.HTTP_RESULT_TOTAL, Map.of(
                    "uri_group", uriGroup,
                    "result", result,
                    "instance", instanceName(instance)), total * instance / weightSum, now));
        }
    }

    private List<PromSample> freshnessSamples() {
        Instant now = Instant.now();
        return List.of(sample(MetricAggregation.HTTP_FRESHNESS_AGE_SECONDS, Map.of("instance", instanceName(1)),
                "stale".equals(scenario) ? 300 : 0, now));
    }

    private double totalRps() {
        if ("idle".equals(scenario)) {
            return 0;
        }
        if ("loaded".equals(scenario) || "stale".equals(scenario)
                || "promDown".equals(scenario) || "budget".equals(scenario)) {
            return loadedRps();
        }
        long second = phaseSeconds();
        if (second < 20) {
            return 5_000d * second / 20d;
        }
        if (second < 60) {
            return loadedRps();
        }
        if (second < 85) {
            return 5_000d * (85 - second) / 25d;
        }
        return 0;
    }

    private double loadedRps() {
        return 5_000d + Math.sin(System.nanoTime() / 1_000_000_000d * 1.7) * 180d;
    }

    private double policyRejectRps(double total) {
        if (total == 0) {
            return 0;
        }
        long second = phaseSeconds();
        double surge = second < 45 ? 0.008 : Math.min(0.06, 0.008 + (second - 45) * 0.006);
        return total * surge;
    }

    private long phaseSeconds() {
        return Duration.ofNanos(System.nanoTime() - startedAtNanos).toSeconds() % CYCLE_SECONDS;
    }

    private Instant observedAt(Instant now) {
        return "stale".equals(scenario) ? now.minusSeconds(300) : now;
    }

    private static PromSample sample(
            String metricName, Map<String, String> labels, double value, Instant evaluatedAt) {
        return new PromSample(metricName, labels, value, evaluatedAt);
    }

    private static String instanceName(int instance) {
        return "mock-api-" + instance + ":8080";
    }
}
