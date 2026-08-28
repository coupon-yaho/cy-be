package com.kafkick.core.admin.inquiry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.InquiryPosition;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.SourceKind;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

class AdminIssuanceInquiryQueryTest {

    @Test
    void rejectsInvalidExternalFiltersAsCommonInvalidInput() {
        assertInvalidInput(() -> query(0L, null, null, null, 50));
        assertInvalidInput(() -> query(1L, 0L, null, null, 50));
        assertInvalidInput(() -> query(1L, null, 99, null, 50));
        assertInvalidInput(() -> query(1L, null, 600, null, 50));
        assertInvalidInput(() -> query(1L, null, null, null, 0));
        assertInvalidInput(() -> query(1L, null, null, null, 201));
    }

    @Test
    void limitErrorMessageUsesThePublishedMaximum() {
        assertThatThrownBy(() -> query(
                1L, null, null, null, AdminIssuanceInquiryQuery.MAX_LIMIT + 1))
                .hasMessage("limit은 1~" + AdminIssuanceInquiryQuery.MAX_LIMIT + "이어야 합니다.");
    }

    @Test
    void acceptsEverySupportedFilterTogether() {
        InquiryPosition before = new InquiryPosition(
                Instant.parse("2026-08-23T00:00:00Z"), SourceKind.ATTEMPT, 10L);

        AdminIssuanceInquiryQuery query = new AdminIssuanceInquiryQuery(
                1L, 2L, 409, ReasonCode.ALREADY_ISSUED, before, 200);

        assertThat(query.memberId()).isEqualTo(1L);
        assertThat(query.couponId()).isEqualTo(2L);
        assertThat(query.httpStatus()).isEqualTo(409);
        assertThat(query.reasonCode()).isEqualTo(ReasonCode.ALREADY_ISSUED);
        assertThat(query.before()).isEqualTo(before);
        assertThat(query.limit()).isEqualTo(AdminIssuanceInquiryQuery.MAX_LIMIT);
    }

    @Test
    void rejectsInvalidCursorPositionComponents() {
        assertThatThrownBy(() -> new InquiryPosition(null, SourceKind.ATTEMPT, 1L))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new InquiryPosition(
                Instant.EPOCH, null, 1L))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new InquiryPosition(
                Instant.EPOCH, SourceKind.ISSUANCE, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static AdminIssuanceInquiryQuery query(
            long memberId,
            Long couponId,
            Integer httpStatus,
            ReasonCode reasonCode,
            int limit
    ) {
        return new AdminIssuanceInquiryQuery(
                memberId, couponId, httpStatus, reasonCode, null, limit);
    }

    private static void assertInvalidInput(Runnable construction) {
        assertThatThrownBy(construction::run)
                .isInstanceOfSatisfying(BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(CommonErrorCode.INVALID_INPUT));
    }
}
