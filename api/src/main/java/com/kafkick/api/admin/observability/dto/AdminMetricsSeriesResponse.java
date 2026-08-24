package com.kafkick.api.admin.observability.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;

import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.MetricsScope;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.observation.SourceStatus;

/**
 * {@code GET /metrics/series} 의 시계열 응답입니다.
 *
 * <p><b>{@link AdminMetricsResponse} 와 역할이 다릅니다.</b> 저쪽은 한 시점 스냅샷이고 화면이
 * 1초 폴링으로 현재를 잇습니다. 이쪽은 <b>진입 시 과거 구간</b>을 채웁니다 — 두 경로를 각각 다른
 * 주기로 부르며, 이 응답이 온 뒤의 1초 갱신은 여전히 화면의 누적 버퍼가 잇습니다.</p>
 *
 * <p><b>계열은 각각 독립적으로 죽습니다.</b> 계열마다 range 질의를 따로 보내고 실패를 그 계열
 * 안에 가둡니다 — 하나가 비어도 나머지는 그려져야 합니다. 죽은 계열은 {@code state} 가
 * {@code UNAVAILABLE} 이고 {@code points} 가 빈 목록입니다.</p>
 *
 * <p><b>응답 전용입니다.</b> 이 타입으로 역직렬화하지 마십시오.</p>
 *
 * @param meta 원천 상태와 무관하게 항상 채워지는 조회 메타데이터
 * @param scope 관측 범위. 이 경로는 현재 GLOBAL 만 냅니다
 * @param window 조회 구간이자 rate 집계 창을 정하는 기준
 * @param series 계열 목록. 계열마다 상태와 라벨이 붙습니다
 */
public record AdminMetricsSeriesResponse(
        Meta meta,
        MetricsScope scope,
        MetricsWindow window,
        List<SeriesEntry> series
) {

    /** 계열 목록을 불변 복사합니다. */
    public AdminMetricsSeriesResponse {
        Objects.requireNonNull(meta, "meta");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(window, "window");
        series = List.copyOf(Objects.requireNonNull(series, "series"));
    }

    /**
     * 조회 자체에 대한 사실입니다. 원천이 실패해도 알 수 있으므로 상태로 감싸지 않습니다.
     *
     * @param schemaVersion 화면 계약이 리터럴 1로 고정한 응답 스키마 판
     * @param rangeStart 조회 구간의 시작 시각
     * @param rangeEnd 조회 구간의 끝 시각
     * @param stepSeconds 평가 간격이자 rate 집계 창(초)
     * @param collectionDurationMs 조립에 실제로 걸린 시간(ms). 예산에 얼마나 근접했는지가 이 값으로만 보인다
     */
    public record Meta(
            int schemaVersion,
            Instant rangeStart,
            Instant rangeEnd,
            long stepSeconds,
            long collectionDurationMs
    ) {

        /** 화면 타입이 {@code number} 가 아니라 리터럴 {@code 1} 입니다. */
        public static final int SCHEMA_VERSION = 1;
    }

    /**
     * 계열 하나입니다.
     *
     * @param key 화면이 패널을 고르는 계열 종류
     * @param labels 이 계열을 같은 종류의 다른 계열과 구분하는 라벨. 집계로 하나만 나오는 종류는 빈 맵
     * @param state 이 계열의 원천 상태
     * @param points 시각순 표본. 상태가 VALID 가 아니면 빈 목록
     */
    public record SeriesEntry(
            SeriesKey key,
            Map<String, String> labels,
            SourceStatus state,
            List<SeriesPoint> points
    ) {

        /** 라벨과 표본을 불변 복사합니다. */
        public SeriesEntry {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(state, "state");
            labels = Map.copyOf(Objects.requireNonNull(labels, "labels"));
            points = List.copyOf(Objects.requireNonNull(points, "points"));
        }

        /**
         * 원천이 죽은 계열을 만듭니다.
         *
         * @param key 계열 종류
         * @return 표본이 없는 UNAVAILABLE 계열
         */
        public static SeriesEntry unavailable(SeriesKey key) {
            return new SeriesEntry(key, Map.of(), SourceStatus.UNAVAILABLE, List.of());
        }
    }

    /**
     * 계열의 한 표본입니다.
     *
     * @param at 표본 시각
     * @param value 표본 값. NaN·무한대는 숫자 관측이 아니므로 null 로 내보낸다 — 0 으로 바꾸면
     *        "정상인데 0" 과 구분되지 않는다
     */
    public record SeriesPoint(
            Instant at,
            // ⚠️ ALWAYS 를 빼지 말 것. 운영은 default-property-inclusion: non_null 이라 null 이면
            //    키가 통째로 사라지고, 화면은 undefined 를 0 으로 그린다 — "계산할 수 없다" 가
            //    "0%" 로 둔갑한다. 실측: 무트래픽 구간의 실패율은 181점이 전부 NaN 이다.
            //    /metrics 는 값마다 state 를 붙여 이 문제를 피하지만 점 단위 상태는 두지 않는다.
            @JsonInclude(JsonInclude.Include.ALWAYS) Double value) {

        /** 표본 시각을 필수로 검증합니다. */
        public SeriesPoint {
            Objects.requireNonNull(at, "at");
        }
    }

    /** 화면이 패널을 고르는 계열 종류입니다. */
    public enum SeriesKey {

        /** 발급 경로 전체 처리량(초당). */
        THROUGHPUT,

        /**
         * 발급 경로 응답시간 p99(ms).
         *
         * <p><b>{@code window} 가 걸리지 않습니다.</b> 백분위 관측 창은 Micrometer expiry 가
         * 정하고 PromQL 로는 바꿀 수 없습니다 — 창을 바꿔도 각 점의 값은 같은 방식으로 계산됩니다.
         * 여기서 창이 정하는 것은 <b>몇 시부터의 점을 보여줄지</b> 뿐입니다.</p>
         */
        LATENCY_P99,

        /** 발급 시도 대비 실패 비율(0~1). */
        FAILURE_RATE,

        /** 정합성 gap. {@code type} 라벨로 종류당 하나씩 나옵니다. */
        CONSISTENCY_GAP
    }
}
