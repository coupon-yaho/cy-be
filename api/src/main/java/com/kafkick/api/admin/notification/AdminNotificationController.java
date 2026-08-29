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

import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.caller.Caller;
import com.kafkick.api.admin.notification.dto.NotificationResendAcceptedResponse;
import com.kafkick.api.admin.notification.dto.NotificationFailurePageResponse;
import com.kafkick.api.admin.notification.dto.NotificationSummaryResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 운영자가 실패한 고객 알림의 재발송을 요청하는 명령 API의 HTTP 계약을 선구축합니다.
 *
 * <p>수신자·연락처·메시지 본문은 요청과 응답에 노출하지 않습니다. 실제 비동기 발송 큐 적재와 중복 요청 처리,
 * 성공 시 {@code 202 Accepted} 응답은 후속 알림 구현에서 연결합니다.</p>
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminNotificationController {

    /**
     * 지정한 알림의 재발송 요청을 접수합니다.
     *
     * <p>{@code notificationId}는 양수만 허용합니다. 현재는 발송 명령이 연결되지 않아 유효 요청에도
     * {@link AdminApiErrorCode#NOT_IMPLEMENTED}를 발생시키며, 후속 구현에서 비동기 발송 요청을 만든 뒤
     * 접수 시각과 접수 상태를 반환합니다.</p>
     *
     * @param notificationId 재발송할 알림 식별자
     * @param caller 헤더 검증을 통과한 재발송 요청 관리자
     * @return 후속 구현에서 사용할 재발송 접수 응답 봉투
     * @throws BusinessException 알림 재발송 구현이 아직 연결되지 않은 경우
     */
    @PostMapping("/notifications/{notificationId}/resend")
    public ResponseEnvelope<NotificationResendAcceptedResponse> resend(
            @PathVariable @Positive(message = "notificationId는 양수여야 합니다.") Long notificationId,
            Caller caller) {
        throw new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }

    /**
     * 회차별 발송 신청·성공·실패·잔여 건수를 독립 관측 상태와 함께 조회합니다.
     *
     * @param couponId 특정 회차만 조회할 선택 식별자
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 알림 집계 연결에서 사용할 발송 요약
     * @throws BusinessException 알림 집계가 아직 연결되지 않은 경우
     */
    @GetMapping("/notifications/summary")
    public ResponseEnvelope<NotificationSummaryResponse> summary(
            @RequestParam(required = false) @Positive Long couponId,
            Caller caller) {
        throw new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }

    /**
     * 재발송 대상을 찾기 위한 실패·DLQ 알림을 과거 방향으로 조회합니다.
     *
     * @param beforeCursor 직전 페이지보다 오래된 실패 항목을 가리키는 cursor
     * @param limit 반환할 최대 항목 수; 기본 50, 허용 범위 1~200
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 개인정보 원문을 제외한 실패 알림 목록
     * @throws BusinessException 실패 알림 원천이 아직 연결되지 않은 경우
     */
    @GetMapping("/notifications/failures")
    public ResponseEnvelope<NotificationFailurePageResponse> failures(
            @RequestParam(required = false) String beforeCursor,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) Integer limit,
            Caller caller) {
        throw new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }
}
