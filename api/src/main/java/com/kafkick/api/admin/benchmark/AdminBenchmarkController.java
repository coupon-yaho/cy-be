package com.kafkick.api.admin.benchmark;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.caller.Caller;
import com.kafkick.api.admin.benchmark.dto.BenchmarkListQuery;
import com.kafkick.api.admin.benchmark.dto.BenchmarkListResponse;
import com.kafkick.api.admin.benchmark.dto.BenchmarkStartRequest;
import com.kafkick.api.admin.benchmark.dto.BenchmarkCommandAcceptedResponse;
import com.kafkick.api.admin.benchmark.dto.BenchmarkDetailResponse;
import com.kafkick.api.admin.benchmark.dto.K6ResultUploadRequest;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 관리자 Benchmark 목록 조회의 HTTP 경계를 선구축합니다.
 *
 * <p>이 Controller는 URL, Query binding, Validation, 응답 DTO 타입만 소유합니다. 실제 Benchmark 실행 결과의
 * 조회·비교·저장소 접근은 후속 Benchmark 구현에서 연결하며, 그 전까지 유효한 요청도
 * {@code 501 / ADMIN-001}로 응답합니다.</p>
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminBenchmarkController {

    /**
     * 기간·엔진 버전·시나리오 조건에 맞는 Benchmark 실행 목록을 최신 실행부터 과거 방향으로 조회합니다.
     *
     * <p>{@code beforeCursor}는 현재 페이지의 마지막 실행보다 더 오래된 실행을 요청하는 불투명 cursor이고,
     * {@code limit}은 생략 시 50, 최대 200입니다. 아직 결과 저장소와 Use Case가 연결되지 않았으므로
     * Validation을 통과한 요청에도 {@link AdminApiErrorCode#NOT_IMPLEMENTED}를 발생시킵니다.</p>
     *
     * @param query 조회 기간, 엔진 버전, 시나리오 코드, 과거 방향 cursor와 페이지 크기
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 구현에서 사용할 Benchmark 목록 응답 봉투
     * @throws BusinessException 실제 Benchmark 조회 구현이 아직 연결되지 않은 경우
     */
    @GetMapping("/benchmarks")
    public ResponseEnvelope<BenchmarkListResponse> benchmarks(
            @Valid @ModelAttribute BenchmarkListQuery query, Caller caller) {
        throw new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }

    /**
     * 지정한 Benchmark 실행의 조건, 서버 시계열, k6 공식 결과를 조회합니다.
     *
     * @param benchmarkRunId 조회할 양수 Benchmark 실행 식별자
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 결과 저장소 연결에서 사용할 실행 상세
     * @throws BusinessException Benchmark 결과 저장소가 아직 연결되지 않은 경우
     */
    @GetMapping("/benchmarks/{benchmarkRunId}")
    public ResponseEnvelope<BenchmarkDetailResponse> benchmark(
            @PathVariable @Positive Long benchmarkRunId, Caller caller) {
        throw notImplemented();
    }

    /**
     * 엔진·릴리스·대기열 모드와 시나리오를 고정해 Benchmark 실행을 시작합니다.
     *
     * @param request 실행 조건과 시나리오 코드
     * @param caller 실행 요청을 남길 관리자 감사 주체
     * @return 후속 실행기 연결에서 사용할 명령 접수 결과
     * @throws BusinessException Benchmark 실행기가 아직 연결되지 않은 경우
     */
    @PostMapping("/benchmarks/start")
    public ResponseEnvelope<BenchmarkCommandAcceptedResponse> start(
            @Valid @RequestBody BenchmarkStartRequest request,
            Caller caller) {
        throw notImplemented();
    }

    /**
     * Benchmark 실행 중지 명령을 접수합니다. 계측 세션 중지 API와는 별개입니다.
     *
     * @param benchmarkRunId 중지할 Benchmark 실행 식별자
     * @param caller 중지 요청을 남길 관리자 감사 주체
     * @return 후속 실행기 연결에서 사용할 명령 접수 결과
     * @throws BusinessException Benchmark 실행기가 아직 연결되지 않은 경우
     */
    @PostMapping("/benchmarks/{benchmarkRunId}/stop")
    public ResponseEnvelope<BenchmarkCommandAcceptedResponse> stop(
            @PathVariable @Positive Long benchmarkRunId,
            Caller caller) {
        throw notImplemented();
    }

    /**
     * 중지된 실행을 FINAL 정합성 판정 단계로 넘기는 독립 명령입니다.
     *
     * @param benchmarkRunId 확정할 Benchmark 실행 식별자
     * @param caller 확정 요청을 남길 관리자 감사 주체
     * @return 후속 판정기 연결에서 사용할 명령 접수 결과
     * @throws BusinessException FINAL 판정기가 아직 연결되지 않은 경우
     */
    @PostMapping("/benchmarks/{benchmarkRunId}/finalize")
    public ResponseEnvelope<BenchmarkCommandAcceptedResponse> finalizeRun(
            @PathVariable @Positive Long benchmarkRunId,
            Caller caller) {
        throw notImplemented();
    }

    /**
     * k6가 관측한 공식 비교 결과와 측정 시각을 해당 실행에 연결합니다.
     *
     * @param benchmarkRunId 결과를 연결할 Benchmark 실행 식별자
     * @param request TPS, p99, 실패 건수·비율과 측정 시각
     * @param caller 업로드 요청을 남길 관리자 감사 주체
     * @return 후속 결과 저장소 연결에서 사용할 갱신된 실행 상세
     * @throws BusinessException k6 결과 저장소가 아직 연결되지 않은 경우
     */
    @PostMapping("/benchmarks/{benchmarkRunId}/k6-result")
    public ResponseEnvelope<BenchmarkDetailResponse> uploadK6Result(
            @PathVariable @Positive Long benchmarkRunId,
            @Valid @RequestBody K6ResultUploadRequest request,
            Caller caller) {
        throw notImplemented();
    }

    private BusinessException notImplemented() {
        return new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }
}
