package com.kafkick.core.admin.inquiry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.InquiryPosition;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.SourceKind;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryResult.InquiryItem;
import com.kafkick.core.coupon.IssuanceStatus;

class AdminIssuanceInquiryResultTest {

    private static final Instant OCCURRED_AT = Instant.parse("2026-08-23T00:00:00Z");
    private static final InquiryPosition POSITION = new InquiryPosition(
            OCCURRED_AT, SourceKind.ATTEMPT, 1L);

    @Test
    void protectsItemsAndEnforcesPaginationConsistency() {
        ArrayList<InquiryItem> mutable = new ArrayList<>(List.of(item()));
        AdminIssuanceInquiryResult result = new AdminIssuanceInquiryResult(
                mutable, POSITION, true);
        mutable.clear();

        assertThat(result.items()).hasSize(1);
        assertThatThrownBy(() -> result.items().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new AdminIssuanceInquiryResult(null, null, false))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AdminIssuanceInquiryResult(List.of(), POSITION, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceInquiryResult(List.of(item()), null, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceInquiryResult(List.of(item()), POSITION, false))
                .isInstanceOf(IllegalArgumentException.class);
        InquiryPosition wrongNextBefore = new InquiryPosition(
                OCCURRED_AT.minusSeconds(1), SourceKind.ATTEMPT, 2L);
        assertThatThrownBy(() -> new AdminIssuanceInquiryResult(
                List.of(item()), wrongNextBefore, true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsItemsWithoutRequiredIdentityTimeOrPosition() {
        assertThatThrownBy(() -> new InquiryItem(
                0L, 2L, null, 500, null, null, OCCURRED_AT, POSITION))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InquiryItem(
                1L, 0L, null, 500, null, null, OCCURRED_AT, POSITION))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InquiryItem(
                1L, 2L, 0L, 201, null, IssuanceStatus.ISSUED, OCCURRED_AT, POSITION))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InquiryItem(
                1L, 2L, null, 99, null, null, OCCURRED_AT, POSITION))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InquiryItem(
                1L, 2L, null, null, null, null, null, POSITION))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new InquiryItem(
                1L, 2L, null, null, null, null, OCCURRED_AT, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new InquiryItem(
                1L, 2L, null, null, null, IssuanceStatus.ISSUED, OCCURRED_AT, POSITION))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsFailureResultWithoutReasonCode() {
        assertThatThrownBy(() -> new InquiryItem(
                1L, 2L, null, 500, null, null, OCCURRED_AT, POSITION))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static InquiryItem item() {
        return new InquiryItem(
                1L, 2L, 3L, 201, null, IssuanceStatus.ISSUED, OCCURRED_AT, POSITION);
    }
}
