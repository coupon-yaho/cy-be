package com.kafkick.api.admin.dashboard;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.admin.dashboard.dto.CouponMetricsResponse;
import com.kafkick.api.admin.issuance.dto.IssuanceHistoryPageResponse;
import com.kafkick.api.admin.issuance.dto.IssuanceInquiryPageResponse;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.SourceStatus;

/** 대시보드 지표와 발급 조회 DTO의 중첩 관측값·마스킹·enum JSON 계약을 검증합니다. */
class DashboardIssuanceDtoJsonSerializationTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-16T00:00:00Z");

    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 쿠폰 지표가 원천별 상태와 관측 시각을 잃지 않고 확정 enum 이름으로 직렬화되는지 확인합니다. */
    @Test
    void couponMetricsSerializesNestedObservedValuesAndCampaignStatus() throws Exception {
        ObservedValue<Long> initialCount = new ObservedValue<>(100L, SourceStatus.VALID, OBSERVED_AT);
        ObservedValue<Long> remainingCount = new ObservedValue<>(40L, SourceStatus.VALID, OBSERVED_AT);
        CouponMetricsResponse response = new CouponMetricsResponse(
                7L,
                OBSERVED_AT,
                MetricsWindow.FIVE_MINUTES,
                new CouponMetricsResponse.StockSummary(initialCount, remainingCount),
                new ObservedValue<>(0.6, SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(new CouponMetricsResponse.RateSummary(12.5, 20.0), SourceStatus.VALID, OBSERVED_AT),
                new CouponMetricsResponse.QueueSummary(
                        new ObservedValue<>(null, SourceStatus.PENDING, null),
                        new ObservedValue<>(null, SourceStatus.PENDING, null)
                ),
                new CouponMetricsResponse.CampaignRuntimeSummary(CouponStatus.OPEN, null),
                new ObservedValue<>(0.2, SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(new CouponMetricsResponse.IssuanceStatusCounts(10, 5, 1, 2),
                        SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(null, SourceStatus.PENDING, null)
        );

        String json = objectMapper.writeValueAsString(response);

        assertThat(json)
                .contains("\"couponId\":7")
                .contains("\"window\":\"FIVE_MINUTES\"")
                .contains("\"initialCount\":{\"value\":100,\"state\":\"VALID\"")
                .contains("\"waitingCount\":{\"value\":null,\"state\":\"PENDING\"")
                .contains("\"status\":\"OPEN\"")
                .contains("\"transitionRate\":{\"value\":null,\"state\":\"PENDING\"");
    }

    /** 발급 문의·이력이 nullable 필드와 마스킹 코드를 보존하고 core enum을 재사용하는지 확인합니다. */
    @Test
    void issuanceDtosKeepNullableFieldsAndReuseCoreEnums() throws Exception {
        IssuanceInquiryPageResponse inquiries = new IssuanceInquiryPageResponse(
                List.of(new IssuanceInquiryPageResponse.IssuanceInquiryItem(
                        1L, null, 3L, null, ReasonCode.STOCK_EXHAUSTED, IssuanceStatus.ISSUED, OBSERVED_AT
                )),
                null,
                false
        );
        IssuanceHistoryPageResponse histories = new IssuanceHistoryPageResponse(
                List.of(new IssuanceHistoryPageResponse.IssuanceHistoryItem(
                        3L, "ABCD-****", 7L, null, IssuanceStatus.ISSUED,
                        IssuanceEventType.ISSUE, OBSERVED_AT
                )),
                null,
                false
        );

        assertThat(objectMapper.writeValueAsString(inquiries))
                .contains("\"httpStatus\":null", "\"reasonCode\":\"STOCK_EXHAUSTED\"", "\"currentStatus\":\"ISSUED\"");
        assertThat(objectMapper.writeValueAsString(histories))
                .contains("\"fromStatus\":null", "\"toStatus\":\"ISSUED\"", "\"eventType\":\"ISSUE\"");
    }
}
