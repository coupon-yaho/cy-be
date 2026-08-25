package com.kafkick.api.admin.observability;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.Valid;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.admin.events.LiveEventAssembler;
import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsSeriesResponse;
import com.kafkick.api.admin.support.LiveEventPollResponse;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.api.caller.Caller;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 개발자·운영 엔지니어가 현재 서비스 상태와 최근 발급 이벤트를 조회하는 관제 HTTP 계약입니다.
 *
 * <p>{@code GET /metrics}는 OBS-6 에서 연결했습니다. 원천은 Prometheus 하나이며 Redis·Kafka·DB 를
 * 직접 읽지 않습니다. {@code GET /metrics/series}는 같은 원천의 과거 구간을 시계열로 주며 예산과
 * 폴링 주기가 {@code /metrics}와 분리되어 있습니다(OBS-33).</p>
 *
 * <p>{@code GET /events}는 OBS-15 에서 연결했습니다. <b>원천이 다릅니다</b> — Redis Stream
 * {@code attempt:live} 하나이고, 그 버퍼는 Kafka {@code attempt-live} 컨슈머가 층화 샘플링을
 * 거쳐 채웁니다. 발급 경로는 이 경로의 어느 지점도 지나지 않습니다.</p>
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminObservabilityController {

    private final PromMetricsAssembler assembler;
    private final PromSeriesAssembler seriesAssembler;
    private final LiveEventAssembler liveEvents;

    public AdminObservabilityController(
            PromMetricsAssembler assembler,
            PromSeriesAssembler seriesAssembler,
            LiveEventAssembler liveEvents) {
        this.assembler = assembler;
        this.seriesAssembler = seriesAssembler;
        this.liveEvents = liveEvents;
    }

    /**
     * 전체 서비스, 특정 쿠폰, 또는 특정 Benchmark 실행 범위의 트래픽·지연·정합성 지표를 조회합니다.
     *
     * <p>{@code window}는 필수이며 {@code 1m}, {@code 5m}, {@code 15m} 중 하나입니다. 되돌아볼
     * 범위가 아니라 <b>비율을 계산할 집계 창</b>입니다 — 응답은 한 시점 스냅샷이고 차트의 과거
     * 구간은 화면이 1 초 폴링으로 누적합니다. 지연 백분위에는 걸리지 않습니다({@link MetricsWindow}
     * 참고). {@code couponId}와 {@code benchmarkRunId}는 서로 다른 관측 범위이므로 동시에 지정할
     * 수 없습니다.</p>
     *
     * <p>원천은 Prometheus 하나입니다. 이 안에서 Redis·DB 를 다시 읽지 않습니다 — 관리자 다섯 명이
     * 보면 초당 다섯 번 재수집이 됩니다. Prometheus 질의가 실패해도 500 이 아니라 해당 값만
     * {@code UNAVAILABLE} 로 나갑니다.</p>
     *
     * @param query 집계 구간과 선택적인 쿠폰 또는 Benchmark 실행 범위
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 값마다 상태가 붙은 한 시점 지표 스냅샷
     */
    @GetMapping("/metrics")
    public ResponseEnvelope<AdminMetricsResponse> metrics(
            @Valid @ModelAttribute MetricsQuery query, Caller caller) {
        return ResponseEnvelope.success(assembler.assemble(query));
    }

    /**
     * 같은 집계 창의 과거 구간을 시계열로 조회합니다.
     *
     * <p><b>{@code /metrics} 와 응답 모양이 다르고 주기도 다릅니다.</b> 저쪽은 한 시점 스냅샷을
     * 1초마다 주고, 이쪽은 진입 시 과거 구간을 채웁니다 — 화면은 두 경로를 각각 다른 주기(이쪽은
     * 5~10초)로 부르고, <b>이 응답이 온 뒤의 1초 갱신은 여전히 화면의 누적 버퍼가 잇습니다</b>.
     * 이 경로가 프론트의 누적 버퍼를 대신하지 않습니다.</p>
     *
     * <p>예산이 {@code /metrics} 와 완전히 분리되어 있습니다({@link PrometheusSeriesProperties}).
     * range 는 평가점 수만큼 expression 을 다시 계산해 instant 보다 자릿수로 느리므로, 같은 예산에
     * 얹으면 {@code /metrics} 의 뒤쪽 질의가 잘립니다. 이 경로가 아무리 느려도 {@code /metrics}
     * 응답 시간은 바뀌지 않습니다.</p>
     *
     * <p>계열은 각각 독립적으로 실패합니다. 하나가 죽어도 500 이 아니라 그 계열만
     * {@code UNAVAILABLE} 로 나갑니다.</p>
     *
     * <p><b>범위는 현재 GLOBAL 만입니다.</b> {@code couponId}·{@code benchmarkRunId} 승계는
     * OBS-34 로 남겼습니다. 두 파라미터가 오면 <b>조용히 무시하지 않고 400 으로 거절합니다</b> —
     * 무시하면 화면이 전역 값을 회차 값으로 읽고, 깨지지 않는 대신 틀린 숫자가 나갑니다.</p>
     *
     * @param window 조회 구간이자 rate 집계 창을 정하는 기준. {@code 1m}, {@code 5m}, {@code 15m}
     * @param couponId 아직 지원하지 않는 쿠폰 범위. 지정하면 400
     * @param benchmarkRunId 아직 지원하지 않는 Benchmark 범위. 지정하면 400
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 계열마다 상태가 붙은 시계열
     * @throws BusinessException 아직 지원하지 않는 범위 파라미터를 지정한 경우 400
     */
    @GetMapping("/metrics/series")
    public ResponseEnvelope<AdminMetricsSeriesResponse> metricsSeries(
            @RequestParam MetricsWindow window,
            @RequestParam(required = false) Long couponId,
            @RequestParam(required = false) Long benchmarkRunId,
            Caller caller) {
        if (couponId != null || benchmarkRunId != null) {
            throw new BusinessException(AdminApiErrorCode.SCOPE_NOT_SUPPORTED);
        }
        return ResponseEnvelope.success(seriesAssembler.assemble(window));
    }

    /**
     * 마지막으로 소비한 cursor 이후의 발급 운영 이벤트를 저장소 수집 순서로 조회합니다.
     *
     * <p>{@code afterCursor}는 Redis Stream ID({@code 1723520000000-3} 꼴)이지만 <b>불투명
     * 문자열</b>입니다. 숫자로 내보내면 안 됩니다 — 밀리초 부분만으로도 JS 의 안전 정수 범위를
     * 넘어 화면이 값을 조용히 반올림하고, 그 커서로는 다시 조회할 수 없습니다.</p>
     *
     * <p><b>cursor 가 만료되면 빈 응답이 아니라 현재 첫 항목부터 돌려줍니다.</b> 버퍼는
     * {@code MAXLEN ~ 200} 으로 계속 잘리므로, 화면이 잠시 멈췄다 돌아오면 그 커서는 이미
     * 트림 구간 밖입니다. 그때 빈 응답을 주면 화면은 "새 이벤트 없음" 으로 읽고 영원히 그
     * 자리에 섭니다 — 버퍼는 초당 수천 건이 도는데 화면만 멈춥니다. 그 경우
     * {@code cursorReset} 과 {@code eventsMayBeMissing} 이 함께 참이 됩니다.</p>
     *
     * <p>놓친 <b>건수</b>는 주지 않습니다. {@code MAXLEN ~} 가 근사 trimming 이라 셀 수 없고,
     * 추정치를 정수로 내보내면 화면이 그것을 사실로 읽습니다.</p>
     *
     * <p>정렬은 도착 순서입니다. {@code occurredAt} 순이 아닙니다 — 파티션 6 · 리스너
     * concurrency 3 이라 그 둘은 다르고, 프로듀서 시계도 인스턴스마다 어긋납니다.</p>
     *
     * <p><b>이 응답을 TPS·성공률의 원천으로 쓰면 안 됩니다.</b> 원천 토픽이 {@code acks=0} 이라
     * 전달 자체가 유실될 수 있고, 그 위에 층화 샘플링이 한 번 더 걸립니다.</p>
     *
     * @param afterCursor 마지막으로 소비한 이벤트를 나타내는 불투명 cursor; 최초 조회이면 생략 가능
     * @param limit 반환할 최대 이벤트 수; 기본 50, 최대 200
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 최근 이벤트 polling 응답 봉투
     */
    @GetMapping("/events")
    public ResponseEnvelope<LiveEventPollResponse> events(
            @RequestParam(required = false) String afterCursor,
            @RequestParam(defaultValue = "50") @Min(value = 1, message = "limit은 1 이상이어야 합니다.")
            @Max(value = 200, message = "limit은 200 이하여야 합니다.") Integer limit,
            Caller caller) {
        return ResponseEnvelope.success(liveEvents.assemble(afterCursor, limit));
    }
}
