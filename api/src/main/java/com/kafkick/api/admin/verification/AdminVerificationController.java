package com.kafkick.api.admin.verification;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.caller.Caller;
import com.kafkick.api.admin.verification.dto.VerificationRunAcceptedResponse;
import com.kafkick.api.admin.verification.dto.VerificationRunDetailResponse;
import com.kafkick.api.admin.verification.dto.VerificationRunPageResponse;
import com.kafkick.api.admin.verification.dto.VerificationRunRequest;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.support.exception.BusinessException;

/** 전수 검증 실행과 검증 결과 조회를 서로 분리한 관리자 HTTP 계약입니다. */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminVerificationController {

    /**
     * 기준 시각·범위·데이터셋을 고정해 전수 검증 실행을 요청합니다.
     *
     * @param request 재현 가능한 검증 조건
     * @param caller 헤더 검증을 통과한 검증 요청 관리자
     * @return 후속 Batch 연결에서 사용할 실행 접수 결과
     * @throws BusinessException 검증 실행기가 아직 연결되지 않은 경우
     */
    @PostMapping("/verify")
    public ResponseEnvelope<VerificationRunAcceptedResponse> verify(
            @Valid @RequestBody VerificationRunRequest request,
            Caller caller) {
        throw notImplemented();
    }

    /**
     * 검증 실행을 최신 항목부터 과거 방향으로 조회합니다.
     *
     * @param beforeCursor 직전 페이지보다 오래된 실행을 가리키는 cursor
     * @param limit 반환할 최대 실행 수; 기본 50, 허용 범위 1~200
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 저장소 연결에서 사용할 검증 실행 목록
     * @throws BusinessException 검증 결과 저장소가 아직 연결되지 않은 경우
     */
    @GetMapping("/verification-runs")
    public ResponseEnvelope<VerificationRunPageResponse> runs(
            @RequestParam(required = false) String beforeCursor,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) Integer limit,
            Caller caller) {
        throw notImplemented();
    }

    /**
     * 단일 검증 실행의 최종 판정과 불일치 finding을 조회합니다.
     *
     * @param runId 조회할 양수 검증 실행 식별자
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 저장소 연결에서 사용할 검증 실행 상세
     * @throws BusinessException 검증 결과 저장소가 아직 연결되지 않은 경우
     */
    @GetMapping("/verification-runs/{runId}")
    public ResponseEnvelope<VerificationRunDetailResponse> run(
            @PathVariable @Positive Long runId, Caller caller) {
        throw notImplemented();
    }

    private BusinessException notImplemented() {
        return new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }
}
