package com.kafkick.api.admin.notification;

import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.caller.Caller;
import com.kafkick.api.admin.notification.dto.NotificationResendAcceptedResponse;
import com.kafkick.api.admin.notification.dto.NotificationFailurePageResponse;
import com.kafkick.api.admin.notification.dto.NotificationSummaryResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.notification.NotificationFailurePage;
import com.kafkick.core.notification.NotificationQueryService;
import com.kafkick.core.notification.NotificationSummary;
import com.kafkick.core.notification.NotificationResendResult;
import com.kafkick.core.notification.NotificationResendService;
import com.kafkick.core.notification.NotificationResendRejectedException;

import java.util.Objects;

/**
 * 운영자가 고객 알림의 상태를 조회하고 실패 알림의 재발송을 요청하는 API입니다.
 *
 * <p>수신자·연락처·메시지 본문은 요청과 응답에 노출하지 않습니다. 재발송 접수는 DB outbox에
 * 저장되며 완료가 아닌 {@code 202 Accepted}를 반환합니다.</p>
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminNotificationController {

    private final NotificationQueryService queryService;
    private final NotificationFailureCursorCodec cursorCodec;
    private final NotificationResendService resendService;

    public AdminNotificationController(NotificationQueryService queryService,
            NotificationFailureCursorCodec cursorCodec,
            NotificationResendService resendService) {
        this.queryService = Objects.requireNonNull(queryService, "queryService");
        this.cursorCodec = Objects.requireNonNull(cursorCodec, "cursorCodec");
        this.resendService = Objects.requireNonNull(resendService, "resendService");
    }

    /**
     * 지정한 알림의 재발송 요청을 접수합니다.
     *
     * <p>{@code notificationId}는 양수만 허용합니다. FAILED·DEAD 알림만 접수하며 상태·시도 횟수·
     * 재발송 횟수 CAS와 10분 멱등 윈도우로 중복 요청을 거부합니다.</p>
     *
     * @param notificationId 재발송할 알림 식별자
     * @param caller 헤더 검증을 통과한 재발송 요청 관리자
     * @return 재발송 접수 응답 봉투
     * @throws BusinessException 알림이 없거나 상태·멱등·횟수 정책으로 거부된 경우
     */
    @PostMapping("/notifications/{notificationId}/resend")
    public ResponseEntity<ResponseEnvelope<NotificationResendAcceptedResponse>> resend(
            @PathVariable @Positive(message = "notificationId는 양수여야 합니다.") Long notificationId,
            Caller caller) {
        try {
            NotificationResendResult result = resendService.resend(notificationId, caller.memberId());
            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(ResponseEnvelope.success(NotificationResendAcceptedResponse.from(result)));
        } catch (NotificationResendRejectedException rejection) {
            AdminApiErrorCode errorCode = switch (rejection.rejection()) {
                case NOT_FOUND -> AdminApiErrorCode.NOTIFICATION_NOT_FOUND;
                case CONFLICT -> AdminApiErrorCode.NOTIFICATION_RESEND_CONFLICT;
                case LIMIT_EXCEEDED -> AdminApiErrorCode.NOTIFICATION_RESEND_LIMIT_EXCEEDED;
            };
            throw new BusinessException(errorCode);
        }
    }

    /**
     * 회차별 발송 신청·성공·실패·잔여 건수를 독립 관측 상태와 함께 조회합니다.
     *
     * @param couponId 특정 회차만 조회할 선택 식별자
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 권위 DB에서 계산한 발송 요약
     */
    @GetMapping("/notifications/summary")
    public ResponseEnvelope<NotificationSummaryResponse> summary(
            @RequestParam(required = false) @Positive Long couponId,
            Caller caller) {
        NotificationSummary summary = queryService.getSummary(couponId);
        return ResponseEnvelope.success(NotificationSummaryResponse.from(summary));
    }

    /**
     * 재발송 대상을 찾기 위한 실패·DLQ 알림을 과거 방향으로 조회합니다.
     *
     * @param beforeCursor 직전 페이지보다 오래된 실패 항목을 가리키는 cursor
     * @param limit 반환할 최대 항목 수; 기본 50, 허용 범위 1~200
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 개인정보 원문을 제외한 실패 알림 목록
     * @throws BusinessException cursor 형식이 유효하지 않은 경우
     */
    @GetMapping("/notifications/failures")
    public ResponseEnvelope<NotificationFailurePageResponse> failures(
            @RequestParam(required = false) String beforeCursor,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) Integer limit,
            Caller caller) {
        Long beforeId = beforeCursor == null ? null : cursorCodec.decode(beforeCursor);
        NotificationFailurePage page = queryService.getFailures(beforeId, limit);
        return ResponseEnvelope.success(NotificationFailurePageResponse.from(page, cursorCodec));
    }
}
