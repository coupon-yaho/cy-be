package com.kafkick.api.admin.dashboard;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.admin.dashboard.dto.CouponMetricsResponse;
import com.kafkick.api.admin.issuance.IssuanceInquiryCursorCodec;
import com.kafkick.api.admin.issuance.dto.IssuanceHistoryPageResponse;
import com.kafkick.api.admin.issuance.dto.IssuanceInquiryPageResponse;
import com.kafkick.api.admin.issuance.dto.IssuanceInquiryQuery;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.api.admin.support.AdminJsonTest;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.InquiryPosition;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.SourceKind;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryResult;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.SourceStatus;

/** 대시보드 지표와 발급 조회 DTO의 중첩 관측값·마스킹·enum JSON 계약을 검증합니다. */
@AdminJsonTest
class DashboardIssuanceDtoJsonSerializationTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-16T00:00:00Z");
    private static final Instant CAMPAIGN_OPENS_AT = Instant.parse("2026-08-15T23:00:00Z");

    private final ObjectMapper objectMapper;

    @Autowired
    DashboardIssuanceDtoJsonSerializationTest(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

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
                new CouponMetricsResponse.CampaignRuntimeSummary(CouponStatus.OPEN, OBSERVED_AT),
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
                .contains("\"waitingCount\":{\"state\":\"PENDING\"")
                .contains("\"status\":\"OPEN\"")
                .contains("\"transitionRate\":{\"state\":\"PENDING\"");
    }

    @Test
    void couponMetricsSerializesCalculatedWindowedRates() throws Exception {
        CouponMetricsSnapshot.Observation<Long> count = snapshotObserved(10L);
        CouponMetricsSnapshot snapshot = new CouponMetricsSnapshot(
                101L,
                OBSERVED_AT,
                MetricsWindow.FIVE_MINUTES,
                new CouponMetricsSnapshot.StockSummary(snapshotObserved(100L), snapshotObserved(40L)),
                snapshotObserved(0.6),
                snapshotObserved(new CouponMetricsSnapshot.RateSummary(12.5, 20.0)),
                new CouponMetricsSnapshot.QueueSummary(count, snapshotObserved(Duration.ofMillis(1_250L))),
                new CouponMetricsSnapshot.CampaignRuntimeSummary(CouponStatus.OPEN, CAMPAIGN_OPENS_AT),
                snapshotObserved(0.2),
                snapshotObserved(new CouponMetricsSnapshot.IssuanceStatusCounts(8L, 2L, 1L, 1L)),
                snapshotObserved(new CouponMetricsSnapshot.TransitionRateSummary(2.5, 1.5, 0.5, 0.25)));

        String json = objectMapper.writeValueAsString(CouponMetricsResponse.from(snapshot));

        assertThat(json)
                .contains("\"window\":\"FIVE_MINUTES\"")
                .contains("\"snapshotAt\":\"2026-08-16T00:00:00Z\"")
                .contains("\"currentPerSecond\":12.5")
                .contains("\"peakPerSecond\":20.0")
                .contains("\"estimatedWaitMillis\":{\"value\":1250")
                .contains("\"opensAt\":\"2026-08-15T23:00:00Z\"")
                .contains("\"unusedCount\":8")
                .contains("\"usePerSecond\":2.5")
                .contains("\"cancelUsePerSecond\":1.5")
                .doesNotContain("\"openedAt\":", "\"issuedCount\":", "\"useCount\":");
    }

    /** 발급 문의·이력이 nullable 필드와 마스킹 코드를 보존하고 core enum을 재사용하는지 확인합니다. */
    @Test
    void issuanceDtosKeepNullableFieldsAndReuseCoreEnums() throws Exception {
        IssuanceInquiryPageResponse inquiries = new IssuanceInquiryPageResponse(
                List.of(new IssuanceInquiryPageResponse.IssuanceInquiryItem(
                        1L, 7L, null, 409, ReasonCode.STOCK_EXHAUSTED, null, OBSERVED_AT
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
                false,
                new IssuanceHistoryPageResponse.IssuanceHistorySummary(1L, 1L, 0L, 0L, 0L, 0L)
        );

        assertThat(objectMapper.writeValueAsString(inquiries))
                .contains("\"httpStatus\":409", "\"reasonCode\":\"STOCK_EXHAUSTED\"")
                .doesNotContain("\"issuanceId\":", "\"currentStatus\":");
        assertThat(objectMapper.writeValueAsString(histories))
                .contains(
                        "\"toStatus\":\"ISSUED\"",
                        "\"eventType\":\"ISSUE\"",
                        "\"summary\":{\"totalCount\":1,\"issueCount\":1")
                .doesNotContain("\"fromStatus\":");
    }

    /** 생략한 limit과 cursor를 Core의 기본 페이지 조건으로 변환하는지 확인합니다. */
    @Test
    void issuanceInquiryQueryUsesDefaultLimitAndNoPosition() {
        IssuanceInquiryQuery query = new IssuanceInquiryQuery(
                1_001L, 2_001L, 201, null, null, null);

        AdminIssuanceInquiryQuery coreQuery = query.toCoreQuery(new IssuanceInquiryCursorCodec());

        assertThat(coreQuery.memberId()).isEqualTo(1_001L);
        assertThat(coreQuery.couponId()).isEqualTo(2_001L);
        assertThat(coreQuery.httpStatus()).isEqualTo(201);
        assertThat(coreQuery.reasonCode()).isNull();
        assertThat(coreQuery.before()).isNull();
        assertThat(coreQuery.limit()).isEqualTo(AdminIssuanceInquiryQuery.DEFAULT_LIMIT);
    }

    /** HTTP cursor와 명시 limit을 Core 위치와 페이지 크기로 변환하는지 확인합니다. */
    @Test
    void issuanceInquiryQueryDecodesCursorAndExplicitLimit() {
        IssuanceInquiryCursorCodec cursorCodec = new IssuanceInquiryCursorCodec();
        InquiryPosition position = new InquiryPosition(OBSERVED_AT, SourceKind.ISSUANCE, 5_003L);
        IssuanceInquiryQuery query = new IssuanceInquiryQuery(
                1_001L, null, null, ReasonCode.INTERNAL_ERROR,
                cursorCodec.encode(position), 17);

        AdminIssuanceInquiryQuery coreQuery = query.toCoreQuery(cursorCodec);

        assertThat(coreQuery.before()).isEqualTo(position);
        assertThat(coreQuery.limit()).isEqualTo(17);
        assertThat(coreQuery.reasonCode()).isEqualTo(ReasonCode.INTERNAL_ERROR);
    }

    /** Core 위치는 cursor로만 변환되고 공개 JSON 항목에는 노출되지 않는지 확인합니다. */
    @Test
    void issuanceInquiryResponseConvertsCorePageWithoutExposingPosition() throws Exception {
        IssuanceInquiryCursorCodec cursorCodec = new IssuanceInquiryCursorCodec();
        InquiryPosition position = new InquiryPosition(OBSERVED_AT, SourceKind.ATTEMPT, 102L);
        AdminIssuanceInquiryResult result = new AdminIssuanceInquiryResult(
                List.of(new AdminIssuanceInquiryResult.InquiryItem(
                        1_001L, 2_001L, 5_001L, 201, null,
                        IssuanceStatus.USED, OBSERVED_AT, position)),
                position,
                true);

        IssuanceInquiryPageResponse response = IssuanceInquiryPageResponse.from(result, cursorCodec);
        String json = objectMapper.writeValueAsString(response);

        assertThat(response.nextBeforeCursor()).isEqualTo(cursorCodec.encode(position));
        assertThat(json).isEqualTo(
                "{\"items\":[{\"memberId\":1001,\"couponId\":2001,\"issuanceId\":5001,"
                        + "\"httpStatus\":201,\"currentStatus\":\"USED\","
                        + "\"occurredAt\":\"2026-08-16T00:00:00Z\"}],"
                        + "\"nextBeforeCursor\":\"" + cursorCodec.encode(position) + "\","
                        + "\"hasOlder\":true}");
    }

    /** 로그가 유실된 실제 발급은 nullable 로그 필드와 DB 현재 상태를 그대로 보존합니다. */
    @Test
    void issuanceInquiryResponsePreservesDbOnlyIssuanceNullLogFields() throws Exception {
        InquiryPosition position = new InquiryPosition(OBSERVED_AT, SourceKind.ISSUANCE, 5_003L);
        AdminIssuanceInquiryResult result = new AdminIssuanceInquiryResult(
                List.of(new AdminIssuanceInquiryResult.InquiryItem(
                        1_001L, 2_005L, 5_003L, null, null,
                        IssuanceStatus.EXPIRED, OBSERVED_AT, position)),
                null,
                false);

        String json = objectMapper.writeValueAsString(
                IssuanceInquiryPageResponse.from(result, new IssuanceInquiryCursorCodec()));

        assertThat(json).isEqualTo(
                "{\"items\":[{\"memberId\":1001,\"couponId\":2005,\"issuanceId\":5003,"
                        + "\"currentStatus\":\"EXPIRED\","
                        + "\"occurredAt\":\"2026-08-16T00:00:00Z\"}],\"hasOlder\":false}");
    }

    private static <T> CouponMetricsSnapshot.Observation<T> snapshotObserved(T value) {
        return new CouponMetricsSnapshot.Observation<>(value, SourceStatus.VALID, OBSERVED_AT);
    }
}
