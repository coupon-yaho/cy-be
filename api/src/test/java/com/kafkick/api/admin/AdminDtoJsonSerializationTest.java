package com.kafkick.api.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

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
import com.kafkick.api.admin.support.AdminJsonTest;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.SourceStatus;

/** 관리자 API 공통 응답 초안이 선언한 JSON 필드 구조를 유지하는지 검증합니다. */
@AdminJsonTest
class AdminDtoJsonSerializationTest {

    @Autowired
    private ObjectMapper objectMapper;

    /** 각 도메인의 빈 응답 예시도 필수 필드와 nullable 구조를 빠뜨리지 않는지 확인합니다. */
    @Test
    void responseDraftsSerializeTheirDeclaredJsonFields() throws Exception {
        assertThat(objectMapper.writeValueAsString(unavailableOverview()))
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
                .contains("eventsMayBeMissing")
                .doesNotContain("nextAfterCursor");
        assertThat(objectMapper.writeValueAsString(BenchmarkListResponse.draft()))
                .contains("items", "hasOlder");
    }

    /** 모든 운영 현황 원천이 미관측인 직렬화 입력을 테스트 범위에서만 생성합니다. */
    private AdminOverviewResponse unavailableOverview() {
        return new AdminOverviewResponse(
                Instant.parse("2026-08-16T00:00:00Z"),
                AdminOverviewResponse.OverallStatus.UNAVAILABLE,
                unavailable(), unavailable(), unavailable(), unavailable(),
                unavailable(), unavailable(), unavailable(), unavailable(),
                unavailable(), unavailable(), unavailable());
    }

    private <T> ObservedValue<T> unavailable() {
        return new ObservedValue<>(null, SourceStatus.UNAVAILABLE, null);
    }

    /** 이벤트를 관리자용으로 투영할 때 등급과 두 queue 순번을 분리하고 원문 코드를 숨기는지 검증합니다. */
    @Test
    void eventJsonKeepsGradeQueuePositionAndSequenceWithoutRawInternalFields() throws Exception {
        AdminEventItem event = new AdminEventItem(
                UUID.fromString("00000000-0000-0000-0000-000000000001"),
                EventType.ENTRY_RESULT,
                1L,
                2L,
                null,
                "ABCD-****",
                Grade.GOLD,
                202,
                null,
                17L,
                103L,
                false,
                Instant.parse("2026-08-16T00:00:00Z"));

        assertThat(objectMapper.writeValueAsString(event))
                .contains("\"grade\":\"GOLD\"")
                .contains("\"queuePosition\":17")
                .contains("\"queueSequence\":103")
                .contains("\"issuanceCodeMasked\":\"ABCD-****\"")
                .doesNotContain("\"issuanceCode\":", "\"requestId\":", "\"producerInstanceId\":");
    }
}
