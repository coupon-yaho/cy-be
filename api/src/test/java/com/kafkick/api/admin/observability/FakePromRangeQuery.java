package com.kafkick.api.admin.observability;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** 계약 테스트가 range 원천의 성공·실패·지연을 계열 단위로 흉내 내는 대역입니다. */
final class FakePromRangeQuery implements PromRangeQuery {

    private final Function<String, List<PromRangeSeries>> handler;
    private final Duration perQueryDelay;
    private final List<String> issued = new ArrayList<>();
    private final List<Request> requests = new ArrayList<>();

    private FakePromRangeQuery(Function<String, List<PromRangeSeries>> handler, Duration perQueryDelay) {
        this.handler = handler;
        this.perQueryDelay = perQueryDelay;
    }

    /** 모든 질의가 표본 하나짜리 계열을 돌려주는 대역입니다. */
    static FakePromRangeQuery alwaysOnePoint() {
        return new FakePromRangeQuery(
                promQl -> List.of(new PromRangeSeries(
                        Map.of("__name__", "any"),
                        List.of(new PromRangePoint(Instant.parse("2026-08-21T00:00:00Z"), 1d)))),
                Duration.ZERO);
    }

    /** 모든 질의가 실패하는 대역입니다. */
    static FakePromRangeQuery down() {
        return new FakePromRangeQuery(promQl -> {
            throw new PromQueryException("대역이 실패를 흉내 냅니다: " + promQl);
        }, Duration.ZERO);
    }

    /** {@code failingFragment} 를 담은 질의만 실패하고 나머지는 성공하는 대역입니다. */
    static FakePromRangeQuery failingOnly(String failingFragment) {
        return new FakePromRangeQuery(promQl -> {
            if (promQl.contains(failingFragment)) {
                throw new PromQueryException("대역이 이 계열만 실패시킵니다: " + promQl);
            }
            return List.of(new PromRangeSeries(
                    Map.of("__name__", "any"),
                    List.of(new PromRangePoint(Instant.parse("2026-08-21T00:00:00Z"), 1d))));
        }, Duration.ZERO);
    }

    /** 질의마다 지정한 시간을 쓰는 느린 대역입니다. 예산 절단을 재현합니다. */
    static FakePromRangeQuery slow(Duration perQueryDelay) {
        return new FakePromRangeQuery(
                promQl -> List.of(new PromRangeSeries(
                        Map.of("__name__", "any"),
                        List.of(new PromRangePoint(Instant.parse("2026-08-21T00:00:00Z"), 1d)))),
                perQueryDelay);
    }

    /** 첫 표본이 NaN 인 대역입니다. 0/0 무트래픽 실패율을 재현합니다. */
    static FakePromRangeQuery withNaNFirstPoint() {
        return new FakePromRangeQuery(
                promQl -> List.of(new PromRangeSeries(
                        Map.of("__name__", "any"),
                        List.of(new PromRangePoint(Instant.parse("2026-08-21T00:00:00Z"), Double.NaN),
                                new PromRangePoint(Instant.parse("2026-08-21T00:00:05Z"), 42d)))),
                Duration.ZERO);
    }

    /**
     * {@code fragment} 를 담은 질의에만 지정한 점들을 돌려주고 나머지는 표본 하나를 돌려주는
     * 대역입니다. 기준선처럼 값의 모양이 결과를 가르는 계열에 씁니다.
     */
    static FakePromRangeQuery pointsFor(String fragment, List<PromRangePoint> points) {
        return new FakePromRangeQuery(promQl -> List.of(new PromRangeSeries(
                Map.of("__name__", "any"),
                promQl.contains(fragment)
                        ? points
                        : List.of(new PromRangePoint(Instant.parse("2026-08-21T00:00:00Z"), 1d)))),
                Duration.ZERO);
    }

    /**
     * {@code fragment} 를 담은 질의에 시계열 <b>여럿</b>을 돌려주는 대역입니다. 라벨당 계열이
     * 생기는 계열(실패 분류·사유)과 원천이 하나여야 하는 미터가 갈린 경우를 재현합니다.
     */
    static FakePromRangeQuery multipleSeriesFor(String fragment, List<Map<String, String>> labelSets) {
        return new FakePromRangeQuery(promQl -> {
            List<PromRangeSeries> series = new ArrayList<>();
            if (promQl.contains(fragment)) {
                for (Map<String, String> labels : labelSets) {
                    series.add(new PromRangeSeries(labels,
                            List.of(new PromRangePoint(Instant.parse("2026-08-21T00:00:00Z"), 1d))));
                }
                return List.copyOf(series);
            }
            return List.of(new PromRangeSeries(
                    Map.of("__name__", "any"),
                    List.of(new PromRangePoint(Instant.parse("2026-08-21T00:00:00Z"), 1d))));
        }, Duration.ZERO);
    }

    /** 일치하는 시계열이 없어 빈 matrix 가 오는 대역입니다. */
    static FakePromRangeQuery empty() {
        return new FakePromRangeQuery(promQl -> List.of(), Duration.ZERO);
    }

    /** @return 실제로 원천에 나간 PromQL 목록. 예산 절단은 여기에 흔적이 남지 않는다 */
    List<String> issued() {
        return List.copyOf(issued);
    }

    /**
     * @return 나간 요청의 <b>전체 인자</b>. PromQL 만 기록하면 조회 구간·간격이 틀려도 드러나지
     *         않는다 — 응답 meta 는 조립기의 지역 변수를 그대로 되비칠 뿐이라 증거가 못 된다
     */
    List<Request> requests() {
        return List.copyOf(requests);
    }

    /** range 질의 한 번의 인자 전부입니다. */
    record Request(String promQl, Instant start, Instant end, Duration step) { }

    @Override
    public List<PromRangeSeries> query(String promQl, Instant start, Instant end, Duration step) {
        issued.add(promQl);
        requests.add(new Request(promQl, start, end, step));
        if (!perQueryDelay.isZero()) {
            long deadline = System.nanoTime() + perQueryDelay.toNanos();
            while (System.nanoTime() - deadline < 0) {
                Thread.onSpinWait();
            }
        }
        return handler.apply(promQl);
    }
}
