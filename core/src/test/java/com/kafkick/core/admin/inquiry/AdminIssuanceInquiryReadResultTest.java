package com.kafkick.core.admin.inquiry;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

class AdminIssuanceInquiryReadResultTest {

    @Test
    void requiresSourceOnlyWhenTheRequestedEntitiesAreAvailable() {
        AdminIssuanceInquirySource source = new AdminIssuanceInquirySource(
                List.of(), List.of(), List.of());

        assertThatThrownBy(() -> new AdminIssuanceInquiryReadResult(
                AdminIssuanceInquiryReadResult.Availability.AVAILABLE, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceInquiryReadResult(
                AdminIssuanceInquiryReadResult.Availability.MEMBER_NOT_FOUND, source))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceInquiryReadResult(
                AdminIssuanceInquiryReadResult.Availability.COUPON_NOT_FOUND, source))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
