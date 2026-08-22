package com.kafkick.api.admin.observability;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.Valid;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.support.LiveEventPollResponse;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.api.caller.Caller;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 개발자·운영 엔지니어가 현재 서비스 상태와 최근 발급 이벤트를 조회하는 관제 HTTP 계약을 선구축합니다.
 *
 * <p>{@code GET /metrics}는 OBS-6 에서 연결했습니다. 원천은 Prometheus 하나이며 Redis·Kafka·DB 를
 * 직접 읽지 않습니다. {@code GET /events}는 아직 원천이 없어 {@code 501 / ADMIN-001}로 응답합니다(OBS-15).</p>
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminObservabilityController {

    private final PromMetricsAssembler assembler;

    public AdminObservabilityController(PromMetricsAssembler assembler) {
        this.assembler = assembler;
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
     * 마지막으로 소비한 cursor 이후의 발급 운영 이벤트를 저장소 수집 순서로 조회합니다.
     *
     * <p>{@code afterCursor}는 마지막 소비 이벤트 다음 항목을 가리키며 과거 페이지용
     * {@code beforeCursor}와 의미를 공유하지 않습니다. cursor가 만료되면 후속 구현은 자동 복구 여부와
     * 이벤트 유실 가능성을 응답 플래그로 알려야 합니다. 현재는 실제 Redis Stream/DB 원천이 연결되지 않았습니다.</p>
     *
     * @param afterCursor 마지막으로 소비한 이벤트를 나타내는 불투명 cursor; 최초 조회이면 생략 가능
     * @param limit 반환할 최대 이벤트 수; 기본 50, 최대 200
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 구현에서 사용할 최근 이벤트 polling 응답 봉투
     * @throws BusinessException 이벤트 원천과 cursor 구현이 아직 연결되지 않은 경우
     */
    @GetMapping("/events")
    public ResponseEnvelope<LiveEventPollResponse> events(
            @RequestParam(required = false) String afterCursor,
            @RequestParam(defaultValue = "50") @Min(value = 1, message = "limit은 1 이상이어야 합니다.")
            @Max(value = 200, message = "limit은 200 이하여야 합니다.") Integer limit,
            Caller caller) {
        throw new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }
}
