package com.kafkick.api.admin.campaign;

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

import com.kafkick.api.admin.campaign.dto.BrandListResponse;
import com.kafkick.api.admin.campaign.dto.CampaignListResponse;
import com.kafkick.api.admin.campaign.dto.CampaignStatusTransitionRequest;
import com.kafkick.api.admin.campaign.dto.CampaignStatusTransitionResponse;
import com.kafkick.api.admin.campaign.dto.TemplateListResponse;
import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.caller.Caller;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 캠페인·브랜드·템플릿 조회와 캠페인 상태 전환의 관리자 HTTP 계약을 제공합니다.
 *
 * <p>일반 생성·수정·삭제는 이 Controller의 범위가 아닙니다. 상태 전환은 감사 기록이 필요한 운영 명령이므로
 * 일반 수정과 분리된 경로를 사용합니다. 저장소가 연결되기 전에는 모든 유효 요청에 501을 반환합니다.</p>
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminCampaignController {

    /**
     * 캠페인을 최신 항목부터 과거 방향으로 조회합니다.
     *
     * @param beforeCursor 직전 페이지의 마지막 항목보다 오래된 결과를 가리키는 cursor
     * @param limit 반환할 최대 항목 수; 기본 50, 허용 범위 1~200
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 저장소 연결에서 사용할 캠페인 목록 응답
     * @throws BusinessException 캠페인 조회 Use Case가 아직 연결되지 않은 경우
     */
    @GetMapping("/campaigns")
    public ResponseEnvelope<CampaignListResponse> campaigns(
            @RequestParam(required = false) String beforeCursor,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) Integer limit,
            Caller caller) {
        throw notImplemented();
    }

    /**
     * 캠페인 필터와 상세 화면에서 선택할 브랜드 목록을 조회합니다.
     *
     * @param beforeCursor 직전 페이지의 마지막 항목보다 오래된 결과를 가리키는 cursor
     * @param limit 반환할 최대 항목 수; 기본 50, 허용 범위 1~200
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 저장소 연결에서 사용할 브랜드 목록 응답
     * @throws BusinessException 브랜드 조회 Use Case가 아직 연결되지 않은 경우
     */
    @GetMapping("/brands")
    public ResponseEnvelope<BrandListResponse> brands(
            @RequestParam(required = false) String beforeCursor,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) Integer limit,
            Caller caller) {
        throw notImplemented();
    }

    /**
     * 정책과 반복 일정이 포함된 캠페인 템플릿 목록을 조회합니다.
     *
     * @param beforeCursor 직전 페이지의 마지막 항목보다 오래된 결과를 가리키는 cursor
     * @param limit 반환할 최대 항목 수; 기본 50, 허용 범위 1~200
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 저장소 연결에서 사용할 템플릿 목록 응답
     * @throws BusinessException 템플릿 조회 Use Case가 아직 연결되지 않은 경우
     */
    @GetMapping("/templates")
    public ResponseEnvelope<TemplateListResponse> templates(
            @RequestParam(required = false) String beforeCursor,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) Integer limit,
            Caller caller) {
        throw notImplemented();
    }

    /**
     * 캠페인 상태 전환 또는 강제 마감 요청을 받습니다.
     *
     * @param campaignId 전환 대상 캠페인 식별자
     * @param request 목표 상태와 감사 사유
     * @param caller 감사 로그에 기록할 요청 관리자
     * @return 후속 구현에서 반환할 상태 전환 결과
     * @throws BusinessException 상태 전환 Use Case가 아직 연결되지 않은 경우
     */
    @PostMapping("/campaigns/{campaignId}/status-transitions")
    public ResponseEnvelope<CampaignStatusTransitionResponse> transition(
            @PathVariable @Positive Long campaignId,
            @Valid @RequestBody CampaignStatusTransitionRequest request,
            Caller caller) {
        throw notImplemented();
    }

    private BusinessException notImplemented() {
        return new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }
}
