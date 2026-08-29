package com.kafkick.core.admin.inquiry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.Dependency;

class AdminIssuanceInquiryErrorCodeTest {

    @Test
    void exposesTheStableMemberCouponAndSourceFailureContracts() {
        assertThat(AdminIssuanceInquiryErrorCode.MEMBER_NOT_FOUND).satisfies(errorCode -> {
            assertThat(errorCode.getStatus()).isEqualTo(404);
            assertThat(errorCode.getCode()).isEqualTo("ADMIN-INQUIRY-001");
            assertThat(errorCode.getMessage()).isEqualTo("회원을 찾을 수 없습니다.");
            assertThat(errorCode.dependency()).isEqualTo(Dependency.NONE);
        });
        assertThat(AdminIssuanceInquiryErrorCode.COUPON_NOT_FOUND).satisfies(errorCode -> {
            assertThat(errorCode.getStatus()).isEqualTo(404);
            assertThat(errorCode.getCode()).isEqualTo("ADMIN-INQUIRY-002");
            assertThat(errorCode.getMessage()).isEqualTo("쿠폰을 찾을 수 없습니다.");
            assertThat(errorCode.dependency()).isEqualTo(Dependency.NONE);
        });
        assertThat(AdminIssuanceInquiryErrorCode.SOURCE_UNAVAILABLE).satisfies(errorCode -> {
            assertThat(errorCode.getStatus()).isEqualTo(503);
            assertThat(errorCode.getCode()).isEqualTo("ADMIN-INQUIRY-003");
            assertThat(errorCode.getMessage()).isEqualTo("발급 문의 데이터를 조회할 수 없습니다.");
            assertThat(errorCode.dependency()).isEqualTo(Dependency.MYSQL);
        });
    }
}
