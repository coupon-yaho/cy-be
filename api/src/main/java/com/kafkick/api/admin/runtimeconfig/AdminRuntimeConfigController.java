package com.kafkick.api.admin.runtimeconfig;

import jakarta.validation.Valid;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.admin.runtimeconfig.dto.RuntimeConfigResponse;
import com.kafkick.api.admin.runtimeconfig.dto.RuntimeConfigUpdateRequest;
import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.caller.Caller;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.support.exception.BusinessException;

/** 런타임 엔진·릴리스·대기열 설정의 조회와 전체 교체 HTTP 계약입니다. */
@RestController
@RequestMapping("/api/v1/admin/runtime-config")
public class AdminRuntimeConfigController {

    /**
     * 현재 엔진·릴리스·대기열 모드와 revision을 조회합니다.
     *
     * @return 후속 Redis 연결에서 사용할 현재 런타임 설정
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @throws BusinessException 설정 Provider가 아직 연결되지 않은 경우
     */
    @GetMapping
    public ResponseEnvelope<RuntimeConfigResponse> get(Caller caller) {
        throw notImplemented();
    }

    /**
     * expectedRevision CAS를 전제로 모든 설정 필드를 한 번에 교체합니다.
     * PATCH처럼 누락 필드를 기존 값으로 해석하지 않습니다.
     *
     * @param request 조회 시점 revision과 교체할 전체 설정
     * @param caller 설정 변경 감사 주체인 요청 관리자
     * @return 후속 구현에서 CAS 적용 후 반환할 새 설정
     * @throws BusinessException RuntimeConfig CAS 구현이 아직 연결되지 않은 경우
     */
    @PutMapping
    public ResponseEnvelope<RuntimeConfigResponse> update(
            @Valid @RequestBody RuntimeConfigUpdateRequest request,
            Caller caller) {
        throw notImplemented();
    }

    private BusinessException notImplemented() {
        return new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }
}
