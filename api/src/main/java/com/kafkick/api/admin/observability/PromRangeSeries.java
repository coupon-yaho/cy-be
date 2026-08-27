package com.kafkick.api.admin.observability;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Prometheus matrix 결과의 라벨 집합과 시간순 표본입니다.
 *
 * @param labels 시계열 라벨 전체
 * @param points 시각·값 표본 목록
 */
public record PromRangeSeries(Map<String, String> labels, List<PromRangePoint> points) {

    /** 라벨과 표본을 불변 복사합니다. */
    public PromRangeSeries {
        labels = Map.copyOf(Objects.requireNonNull(labels, "labels"));
        points = List.copyOf(Objects.requireNonNull(points, "points"));
    }

    /**
     * 라벨 값을 읽습니다.
     *
     * @param name 라벨 이름
     * @return 라벨 값; 없으면 빈 문자열
     */
    public String label(String name) {
        return labels.getOrDefault(name, "");
    }
}
