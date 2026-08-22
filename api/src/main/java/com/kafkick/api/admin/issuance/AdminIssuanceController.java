package com.kafkick.api.admin.issuance;

import java.util.Objects;

import jakarta.validation.Valid;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.admin.issuance.dto.IssuanceHistoryQuery;
import com.kafkick.api.admin.issuance.dto.IssuanceHistoryPageResponse;
import com.kafkick.api.admin.issuance.dto.IssuanceInquiryQuery;
import com.kafkick.api.admin.issuance.dto.IssuanceInquiryPageResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.api.caller.Caller;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryService;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 운영자가 회원 발급 문의와 쿠폰 발급 상태 이력을 조회하는 HTTP 계약을 선구축합니다.
 *
 * <p>개인정보 원문이나 마스킹되지 않은 발급 코드는 응답 계약에 포함하지 않습니다. 발급 상태 이력은
 * Core 조회 결과를 HTTP 계약으로 조립하고, 회원 발급 문의는 후속 구현까지 {@code 501 / ADMIN-001}을 유지합니다.</p>
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminIssuanceController {

    private final AdminIssuanceHistoryService historyService;
    private final IssuanceHistoryCursorCodec cursorCodec;

    /**
     * 발급 이력 Core Service와 HTTP cursor 변환기를 주입받습니다.
     *
     * @param historyService 발급 이력 페이지와 요약 조회 Service
     * @param cursorCodec Core 위치와 HTTP cursor 간 변환기
     */
    public AdminIssuanceController(
            AdminIssuanceHistoryService historyService,
            IssuanceHistoryCursorCodec cursorCodec
    ) {
        this.historyService = Objects.requireNonNull(historyService, "historyService");
        this.cursorCodec = Objects.requireNonNull(cursorCodec, "cursorCodec");
    }

    /**
     * 회원의 발급 시도 결과와 현재 발급 상태를 최신 항목부터 과거 방향으로 조회합니다.
     *
     * <p>{@code memberId}는 필수입니다. 쿠폰, HTTP 상태, 사유 코드는 선택 필터이며
     * {@code beforeCursor}는 현재 페이지보다 오래된 항목을 가리킵니다. 권위 DB 발급 건만 남고 시도 로그가
     * 유실될 수 있으므로 응답의 HTTP 상태와 사유 코드는 nullable입니다. 실제 조회는 후속 발급 문의 Use Case에서 연결합니다.</p>
     *
     * @param query 회원·쿠폰·결과 필터와 과거 방향 cursor, 페이지 크기
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 구현에서 사용할 발급 문의 목록 응답 봉투
     * @throws BusinessException 발급 문의 조회 구현이 아직 연결되지 않은 경우
     */
    @GetMapping("/members/issuance-inquiries")
    public ResponseEnvelope<IssuanceInquiryPageResponse> issuanceInquiries(
            @Valid @ModelAttribute IssuanceInquiryQuery query, Caller caller) {
        throw new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }

    /**
     * 쿠폰 발급의 상태 전이 이력을 최신 전이부터 과거 방향으로 조회합니다.
     *
     * <p>쿠폰, 기간, 이벤트 유형은 선택 필터입니다. {@code from}과 {@code to}를 함께 지정하면 기간이
     * 역전될 수 없으며, 최초 발급 이벤트는 이전 상태가 없어 응답의 {@code fromStatus}가 null일 수 있습니다.
     * cursor는 API에서 검증해 Core Keyset 위치로 변환하고, 목록·요약 계산은 Core Service에 위임합니다.</p>
     *
     * @param query 쿠폰·기간·상태 전이 유형 필터와 과거 방향 cursor, 페이지 크기
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 마스킹된 발급 상태 이력 페이지와 이벤트별 요약 응답 봉투
     * @throws BusinessException cursor가 유효하지 않은 경우
     */
    @GetMapping("/issuance-histories")
    public ResponseEnvelope<IssuanceHistoryPageResponse> issuanceHistories(
            @Valid @ModelAttribute IssuanceHistoryQuery query, Caller caller) {
        AdminIssuanceHistoryResult result = historyService.getHistories(
                query.toCoreQuery(cursorCodec));
        return ResponseEnvelope.success(
                IssuanceHistoryPageResponse.from(result, cursorCodec));
    }
}
