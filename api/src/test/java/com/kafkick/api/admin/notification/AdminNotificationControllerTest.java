package com.kafkick.api.admin.notification;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;

/** 알림 재발송 명령과 요약·실패 목록 조회의 관리자 계약을 검증합니다. */
class AdminNotificationControllerTest {

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(new AdminNotificationController());

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
        mockMvc.perform(get("/api/v1/admin/notifications/summary"))
                .andExpect(status().isNotImplemented());
        mockMvc.perform(get("/api/v1/admin/notifications/failures")
                        .param("beforeCursor", "cursor")
                        .param("limit", "50"))
                .andExpect(status().isNotImplemented());
    }
}
