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
 * <p><b>범위는 계열마다 걸리는 곳이 다릅니다.</b> {@code scope} 는 요청을 되비친 것이고, 그
 * 범위가 <b>실제로 걸린 계열</b>은 {@code series[].scoped} 가 말합니다. 회차는 라벨이 아니라
 * 값이라(회차마다 시계열이 갈리면 회차 간 비교 질의를 쓸 수 없다) 회차 식별자 미터를 함께 내는
 * 도메인 계열만 좁힐 수 있고, HTTP 미터에서 나오는 계열은 전역 값 그대로입니다. <b>화면은
 * 봉투의 {@code scope} 가 아니라 이 값으로 패널에 회차 표식을 달아야 합니다</b> — 봉투만 보면
 * 전역 처리량에 회차 라벨이 붙어 깨지지 않고 틀린 숫자가 나갑니다.</p>
 *
 * <p><b>계열은 각각 독립적으로 죽습니다.</b> 계열마다 range 질의를 따로 보내고 실패를 그 계열
 * 안에 가둡니다 — 하나가 비어도 나머지는 그려져야 합니다. 죽은 계열은 {@code state} 가
 * {@code UNAVAILABLE} 이고 {@code points} 가 빈 목록입니다.</p>
 *
 * <p><b>응답 전용입니다.</b> 이 타입으로 역직렬화하지 마십시오.</p>
 *
 * @param meta 원천 상태와 무관하게 항상 채워지는 조회 메타데이터
 * @param scope 관측 범위. GLOBAL 과 COUPON 을 냅니다 — BENCHMARK_RUN 은 회차 경계 원천이 DB 라 아직 거절합니다
 * @param window 조회 구간이자 rate 집계 창을 정하는 기준
 * @param series 계열 목록. 계열마다 상태와 라벨이 붙습니다
 * @param markers 시계열이 아닌 세로 기준선. 계열 배열에 섞지 않습니다
 * @param markersState 기준선 <b>목록 자체</b>의 원천 상태. 빈 목록의 이유가 여기에만 있습니다
 */
public record AdminMetricsSeriesResponse(
        Meta meta,
        MetricsScope scope,
        MetricsWindow window,
        List<SeriesEntry> series,
        List<Marker> markers,
        SourceStatus markersState
) {

    /** 계열 목록과 기준선을 불변 복사합니다. */
    public AdminMetricsSeriesResponse {
        Objects.requireNonNull(meta, "meta");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(window, "window");
        Objects.requireNonNull(markersState, "markersState");
        series = List.copyOf(Objects.requireNonNull(series, "series"));
        markers = List.copyOf(Objects.requireNonNull(markers, "markers"));
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
     * @param scoped 요청한 관측 범위가 <b>이 계열에 실제로 걸렸는지</b>. 봉투의 {@code scope} 는
     *        요청을 되비칠 뿐이라 계열 단위 사실은 여기에만 있습니다 — 아래 설명 참고
     * @param state 이 계열의 원천 상태
     * @param points 시각순 표본. 상태가 VALID 가 아니면 빈 목록
     */
    public record SeriesEntry(
            SeriesKey key,
            Map<String, String> labels,
            boolean scoped,
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
         * <p>{@code scoped} 는 <b>보냈다면 걸렸을 값</b>을 그대로 싣습니다. 상태에 따라 뒤집히면
         * 화면이 같은 계열을 폴링마다 다르게 라벨링합니다.</p>
         *
         * @param key 계열 종류
         * @param scoped 이 계열에 요청 범위가 걸리는지
         * @return 표본이 없는 UNAVAILABLE 계열
         */
        public static SeriesEntry unavailable(SeriesKey key, boolean scoped) {
            return new SeriesEntry(key, Map.of(), scoped, SourceStatus.UNAVAILABLE, List.of());
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

    /**
     * 시계열이 아닌 세로 기준선입니다.
     *
     * <p><b>{@link SeriesPoint} 와 규격이 다릅니다.</b> 점은 값이 있고 기준선은 이름이 있습니다 —
     * 같은 배열에 넣으면 {@code value} 자리가 비어 화면이 0 으로 그립니다. 그래서 계열 목록과
     * 따로 냅니다.</p>
     *
     * <p><b>빈 목록의 이유는 {@code markersState} 가 냅니다.</b> 목록만 보면 "그런 사건이 없었다"
     * 와 "예산에 잘려 못 물어봤다" 가 같은 모양입니다 — 그리고 잘리는 것은 부하가 걸린 회차,
     * 즉 재고 소진 시각이 가장 궁금한 회차입니다. 기준선 하나하나에 상태를 다는 대신 목록 전체에
     * 답니다: 원천이 하나(재고 미터)라 이유도 하나입니다.</p>

     * <p>{@code VALID} 이고 목록이 비었으면 <b>구간 안에 소진이 없었다</b> 는 뜻입니다.
     * {@code PENDING} 은 재고 표본이 아직 없다는 뜻이고, {@code UNAVAILABLE} 은 예산 절단 ·
     * 질의 실패 · 원천 다중으로 <b>물어보지 못했다</b> 는 뜻입니다.</p>
     *
     * @param at 사건 시각
     * @param label 화면에 그대로 붙는 이름
     */
    public record Marker(Instant at, String label) {

        /** 조회 구간 안에서 남은 재고가 양수에서 0 이하로 처음 바뀐 시각. */
        public static final String STOCK_EXHAUSTED = "재고 소진";

        /** 시각과 이름을 필수로 검증합니다. */
        public Marker {
            Objects.requireNonNull(at, "at");
            Objects.requireNonNull(label, "label");
        }
    }

    /**
     * 화면이 패널을 고르는 계열 종류입니다.
     *
     * <p><b>선언 순서가 우선순위가 아닙니다.</b> 잘리는 순서는 조립기가 정합니다
     * ({@code PromSeriesAssembler.assemble}) — 여기 순서를 바꿔도 아무 일도 일어나지 않습니다.</p>
     */
    public enum SeriesKey {

        /** 발급 경로 전체 처리량(초당). */
        THROUGHPUT,

        /**
         * 발급 경로 <b>성공</b> 응답시간 p99(ms).
         *
         * <p><b>성공 축입니다. 전체가 아닙니다.</b> 질의에 {@code outcome="success"} 가 걸려
         * 있어(OBS-31) 정책 거절·클라이언트 오류·시스템 실패의 지연은 이 값에 들어오지 않습니다.
         * 시스템 실패는 {@link #LATENCY_P99_SYSTEM_FAILURE} 를 보십시오. <b>이름을 바꾸지
         * 않는 이유</b>는 {@code run_timeseries} 의 완료 회차 행이 이 이름으로 이미 적재돼 있고
         * archive 는 불변이라, 개명하면 과거 회차와 현재 회차의 비교 축이 갈리기 때문입니다.</p>
         *
         * <p><b>두 축을 더해 '전체 p99' 를 만들 수 없습니다.</b> 백분위는 병합되지 않고
         * {@code _bucket} 이 없어 재계산도 못 합니다 — 전체가 필요하면 Timer 이중 기록이
         * 선행입니다.</p>
         *
         * <p><b>{@code window} 가 걸리지 않습니다.</b> 백분위 관측 창은 Micrometer expiry 가
         * 정하고 PromQL 로는 바꿀 수 없습니다 — 창을 바꿔도 각 점의 값은 같은 방식으로 계산됩니다.
         * 여기서 창이 정하는 것은 <b>몇 시부터의 점을 보여줄지</b> 뿐입니다.</p>
         */
        LATENCY_P99,

        /**
         * 발급 경로 <b>시스템 실패</b> 응답시간 p99(ms).
         *
         * <p>{@link #LATENCY_P99} 와 <b>원천은 같고 축만 다릅니다</b>({@code outcome="system_failure"}).
         * 스냅샷이 {@code latency.systemFailure} 로 내는 것과 같은 정의·같은 단위입니다 —
         * 스냅샷과 추세선이 서로 다른 것을 그리지 않게 하는 것이 이 계열의 존재 이유입니다.</p>
         *
         * <p><b>정책 거절 축은 추세선에 없습니다.</b> 재고 소진 폭주 때 정책 거절이 쏟아지면 그
         * 축의 p99 가 1ms 아래로 떨어져(실측 3087ms → 1.0ms) 같은 그래프에 있으면 장애가
         * 묻힙니다. 정책 거절 지연은 스냅샷에서 봅니다.</p>
         *
         * <p><b>비어 있는 것이 정상입니다.</b> 시스템 실패가 한 건도 없던 구간에는 시계열 자체가
         * 없어 {@code PENDING} 으로 나갑니다 — 화면은 이것을 "0ms" 가 아니라 "그런 실패가
         * 없었다" 로 읽어야 합니다.</p>
         */
        LATENCY_P99_SYSTEM_FAILURE,

        /** 발급 시도 대비 실패 비율(0~1). */
        FAILURE_RATE,

        /**
         * 실패 분류별 비율(0~100%). {@code result} 라벨로 분류당 하나씩 나옵니다.
         *
         * <p>{@link #FAILURE_RATE} 를 대체하지 않습니다 — 저쪽은 시스템 책임 실패만 세는 한 줄이고
         * 이쪽은 정책 거절·클라이언트 오류까지 <b>네 분류를 모두</b> 폅니다. 분모는 양쪽 다 발급
         * 시도라 이 계열들의 합이 저쪽보다 큽니다. 스냅샷의 {@code errors.classes} 와 같은
         * 정의·같은 단위입니다.</p>
         */
        ERROR_CLASS_RATE,

        /**
         * 실패 사유별 발생률(초당). {@code outcome} 라벨로 사유당 하나씩 나옵니다.
         *
         * <p>{@link #ERROR_CLASS_RATE} 와 <b>원천이 다릅니다</b> — 저쪽은 응답 상태로 나눈 분류이고
         * 이쪽은 업무 사유({@code ReasonCode})입니다. 스냅샷의 {@code errors.topReasons} 와 같은
         * 원천이고, <b>서버는 상위 N 개로 자르지 않습니다</b>.</p>
         *
         * <p>⚠️ 계열 수가 사유 종류만큼입니다. 사유가 늘면 점이 아니라 <b>계열</b>이 늘어납니다.</p>
         */
        FAILURE_REASON_RATE,

        /** 처리 중인 요청 수(전 인스턴스 합). 스레드가 아니라 요청입니다. */
        IN_FLIGHT,

        /**
         * 대기열 대기 인원.
         *
         * <p>게이트웨이 관제가 켜지면 외부 게이트웨이 복제본의 최댓값, 꺼지면 batch 단일 원천을
         * 사용합니다. 값 미터가 NaN 인 점은 이유를 상태 미터가 내지만 <b>점에는 상태를 실을
         * 자리가 없어</b> null 로 나갑니다 — 화면은 선을 끊어야 합니다.</p>
         */
        QUEUE_ADMISSION,

        /** 게이트웨이가 보고한 현재 API 처리 가능량 credit. */
        GATEWAY_CAPACITY_CREDIT,

        /** 게이트웨이에 처리 가능량을 보고하는 API 노드 수. */
        GATEWAY_CAPACITY_NODES,

        /** 게이트웨이의 누적 입장 판정 수. */
        GATEWAY_JUDGEMENT_TOTAL,

        /** 게이트웨이의 누적 백엔드 fallback 수. */
        GATEWAY_BACKEND_FALLBACK_TOTAL,

        /** 게이트웨이의 누적 공급 한도 초과 입장 수. */
        GATEWAY_ALLOCATION_OVERSHOOT_TOTAL,

        /**
         * 저장 대기(Kafka consumer lag).
         *
         * <p><b>원천이 아직 없습니다</b>(OBS-15). 질의를 보내지 않고 언제나 {@code PENDING} 입니다 —
         * 0 을 내보내면 "큐가 비었다" 는 거짓말이 됩니다.</p>
         */
        QUEUE_PERSISTENCE,

        /**
         * 화면 표시 지연.
         *
         * <p><b>서버가 잴 수 없습니다.</b> 정의가 "이벤트 시각 ↔ 화면 수신 시각" 이라 원천이 브라우저
         * 쪽입니다. 질의를 보내지 않고 언제나 {@code PENDING} 입니다.</p>
         */
        QUEUE_TELEMETRY,

        /** 정합성 gap. {@code type} 라벨로 종류당 하나씩 나옵니다. */
        CONSISTENCY_GAP
    }
}
