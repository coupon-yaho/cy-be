package com.kafkick.api.admin.notification;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;
import com.kafkick.core.notification.NotificationFailurePage;
import com.kafkick.core.notification.NotificationQueryService;
import com.kafkick.core.notification.NotificationSummary;
import com.kafkick.core.notification.NotificationSummary.Metric;
import com.kafkick.core.notification.domain.NotificationFailure;
import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.core.observation.SourceStatus;

/** 알림 재발송 명령과 요약·실패 목록 조회의 관리자 계약을 검증합니다. */
class AdminNotificationControllerTest {

    private static final Instant AT = Instant.parse("2026-08-29T00:00:00Z");
    private final NotificationQueryService queryService = org.mockito.Mockito.mock(NotificationQueryService.class);
    private final NotificationFailureCursorCodec cursorCodec = new NotificationFailureCursorCodec();

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(
            new AdminNotificationController(queryService, cursorCodec));

    /** 유효한 재발송 요청이 가짜 성공하지 않고 ADMIN-001을 반환하는지 검증합니다. */
    @Test
    @DisplayName("알림 재발송은 POST 유효 요청에 ADMIN-001 선구축 오류를 반환한다")
    void resendReturnsNotImplementedEnvelope() throws Exception {
        mockMvc.perform(post("/api/v1/admin/notifications/{notificationId}/resend", 1L))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }

    /** 재발송 대상 알림 식별자가 0 이하이면 400으로 거부되는지 검증합니다. */
    @Test
    @DisplayName("알림 재발송은 음수 notificationId를 400 실패 봉투로 거부한다")
    void resendRejectsNonPositiveNotificationId() throws Exception {
        mockMvc.perform(post("/api/v1/admin/notifications/{notificationId}/resend", -1L))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** 알림 요약과 실패 목록이 재발송 명령과 분리된 조회 endpoint인지 검증합니다. */
    @Test
    void exposesSummaryAndFailureReadContracts() throws Exception {
        when(queryService.getSummary(null)).thenReturn(validSummary());
        when(queryService.getFailures(null, 50)).thenReturn(new NotificationFailurePage(List.of(
                new NotificationFailure(41L, 10L, 20L, NotifyFailureReason.SEND_TIMEOUT, 4, AT)),
                41L, true));

        mockMvc.perform(get("/api/v1/admin/notifications/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.totalRequests.value").value(10))
                .andExpect(jsonPath("$.data.sentRate.value").value(0.6));
        mockMvc.perform(get("/api/v1/admin/notifications/failures")
                        .param("limit", "50"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.items[0].notificationId").value(41))
                .andExpect(jsonPath("$.data.items[0].reason").value("SEND_TIMEOUT"))
                .andExpect(jsonPath("$.data.items[0].reasonCode").doesNotExist())
                .andExpect(jsonPath("$.data.items[0].recipientContact").doesNotExist())
                .andExpect(jsonPath("$.data.nextBeforeCursor").value(cursorCodec.encode(41L)));
    }

    @Test
    void rejectsInvalidFailureCursor() throws Exception {
        mockMvc.perform(get("/api/v1/admin/notifications/failures")
                        .param("beforeCursor", "not-base64*"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));
    }

    private static NotificationSummary validSummary() {
        return new NotificationSummary(null, AT,
                Metric.observed(10L, SourceStatus.VALID, AT),
                Metric.observed(6L, SourceStatus.VALID, AT),
                Metric.observed(3L, SourceStatus.VALID, AT),
                Metric.observed(1L, SourceStatus.VALID, AT),
                Metric.observed(0.6d, SourceStatus.VALID, AT));
    }
}
