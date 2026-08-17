package com.kafkick.api.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.admin.benchmark.dto.BenchmarkListResponse;
import com.kafkick.api.admin.dashboard.dto.AdminAnalyticsResponse;
import com.kafkick.api.admin.dashboard.dto.AdminOverviewResponse;
import com.kafkick.api.admin.dashboard.dto.CouponMetricsResponse;
import com.kafkick.api.admin.issuance.dto.IssuanceHistoryPageResponse;
import com.kafkick.api.admin.issuance.dto.IssuanceInquiryPageResponse;
import com.kafkick.api.admin.notification.dto.NotificationResendAcceptedResponse;
import com.kafkick.api.admin.observability.dto.AdminEventItem;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.support.LiveEventPollResponse;
import com.kafkick.core.admin.MetricsWindow;

/** 관리자 API 공통 응답 초안이 선언한 JSON 필드 구조를 유지하는지 검증합니다. */
class AdminDtoJsonSerializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 각 도메인의 빈 응답 예시도 필수 필드와 nullable 구조를 빠뜨리지 않는지 확인합니다. */
    @Test
    void responseDraftsSerializeTheirDeclaredJsonFields() throws Exception {
        assertThat(objectMapper.writeValueAsString(AdminOverviewResponse.draft(Instant.parse("2026-08-16T00:00:00Z"))))
                .contains("snapshotAt", "overallStatus");
        assertThat(objectMapper.writeValueAsString(CouponMetricsResponse.draft(1L, MetricsWindow.FIVE_MINUTES)))
                .contains("couponId", "issuanceProgress");
        assertThat(objectMapper.writeValueAsString(AdminAnalyticsResponse.draft()))
                .contains("range", "hourlyHeatmap");
        assertThat(objectMapper.writeValueAsString(IssuanceInquiryPageResponse.draft()))
                .contains("items", "hasOlder");
        assertThat(objectMapper.writeValueAsString(IssuanceHistoryPageResponse.draft()))
                .contains("items", "hasOlder");
        assertThat(objectMapper.writeValueAsString(NotificationResendAcceptedResponse.draft(1L)))
                .contains("notificationId", "requestStatus");
        assertThat(objectMapper.writeValueAsString(AdminMetricsResponse.draft(MetricsWindow.ONE_MINUTE)))
                .contains("scope", "consistency");
        assertThat(objectMapper.writeValueAsString(new LiveEventPollResponse(
                List.of(AdminEventItem.draft(UUID.fromString("00000000-0000-0000-0000-000000000001"))),
                null, false, false, false)))
                .contains("nextAfterCursor", "eventsMayBeMissing");
        assertThat(objectMapper.writeValueAsString(BenchmarkListResponse.draft()))
                .contains("items", "hasOlder");
    }
}
