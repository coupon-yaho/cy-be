package com.kafkick.api.admin.measurement;

import jakarta.validation.Valid;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.admin.measurement.dto.MeasurementCommandRequest;
import com.kafkick.api.admin.measurement.dto.MeasurementSessionResponse;
import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.caller.Caller;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 계측 수명주기를 벤치마크 실행 수명주기와 분리한 관리자 명령 계약입니다.
 * 실제 표본 수집기와 메모리 버퍼는 이번 선구축 범위에 포함하지 않습니다.
 */
@RestController
@RequestMapping("/api/v1/admin/measurements")
public class AdminMeasurementController {

    /**
     * 지정한 Benchmark 실행에 대한 서버 계측 세션을 시작합니다.
     *
     * <p>Benchmark 자체를 시작하는 명령과 별개이며, 실제 표본 수집기가 연결되기 전에는 요청을
     * 성공으로 가장하지 않고 {@code 501 / ADMIN-001}을 반환합니다.</p>
     *
     * @param request 계측 대상 Benchmark 실행 식별자
     * @param caller 헤더 검증을 통과한 계측 요청 관리자
     * @return 후속 구현에서 사용할 계측 세션 상태
     * @throws BusinessException 계측 시작 Use Case가 아직 연결되지 않은 경우
     */
    @PostMapping("/start")
    public ResponseEnvelope<MeasurementSessionResponse> start(
            @Valid @RequestBody MeasurementCommandRequest request,
            Caller caller) {
        throw notImplemented();
    }

    /**
     * 지정한 Benchmark 실행의 서버 계측 세션을 중지합니다.
     *
     * <p>Benchmark 실행 중지와 독립된 명령이므로 두 동작을 하나의 endpoint로 합치지 않습니다.</p>
     *
     * @param request 계측 대상 Benchmark 실행 식별자
     * @param caller 헤더 검증을 통과한 계측 요청 관리자
     * @return 후속 구현에서 사용할 계측 세션 상태
     * @throws BusinessException 계측 중지 Use Case가 아직 연결되지 않은 경우
     */
    @PostMapping("/stop")
    public ResponseEnvelope<MeasurementSessionResponse> stop(
            @Valid @RequestBody MeasurementCommandRequest request,
            Caller caller) {
        throw notImplemented();
    }

    private BusinessException notImplemented() {
        return new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }
}
