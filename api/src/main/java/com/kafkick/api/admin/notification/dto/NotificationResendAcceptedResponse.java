package com.kafkick.api.admin.notification.dto;

import java.time.Instant;
import com.kafkick.core.notification.NotificationResendResult;

/**
 * 알림 재발송 명령이 비동기 처리 대상으로 접수됐음을 나타내는 응답입니다.
 *
 * <p>{@code requestedAt}은 실제 발송 완료 시각이 아니라 운영자의 재발송 요청을 받아들인 시각입니다.
 * 수신자·연락처·메시지 본문은 포함하지 않으며 접수 상태는 명시적 enum으로 반환합니다.</p>
 *
 * @param notificationId 재발송 요청을 접수한 알림 식별자
 * @param requestStatus 비동기 요청 접수 상태
 * @param requestedAt 재발송 요청 접수 시각
 */
public record NotificationResendAcceptedResponse(Long notificationId, RequestStatus requestStatus, Instant requestedAt) {
    public static NotificationResendAcceptedResponse from(NotificationResendResult result) {
        return new NotificationResendAcceptedResponse(
                result.notificationId(), RequestStatus.ACCEPTED, result.requestedAt());
    }
    /**
     * JSON 필드 계약을 검증하기 위한 접수 응답 예시를 만듭니다.
     *
     * @param notificationId 예시에 사용할 알림 식별자
     * @return ACCEPTED 상태와 epoch 시각을 가진 재발송 접수 예시
     */
    public static NotificationResendAcceptedResponse draft(Long notificationId) { return new NotificationResendAcceptedResponse(notificationId, RequestStatus.ACCEPTED, Instant.EPOCH); }

    /** 비동기 재발송 명령의 접수 결과입니다. */
    public enum RequestStatus { ACCEPTED, REJECTED }
}
