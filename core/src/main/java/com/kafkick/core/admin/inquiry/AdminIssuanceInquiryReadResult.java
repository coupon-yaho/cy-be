package com.kafkick.core.admin.inquiry;

import java.util.Objects;

/** 원천 조회가 확인한 회원·쿠폰 존재 상태와 조회 후보를 함께 전달합니다. */
public record AdminIssuanceInquiryReadResult(
        Availability availability,
        AdminIssuanceInquirySource source
) {

    /** 존재 상태와 후보 원천의 조합을 계약에 맞게 제한합니다. */
    public AdminIssuanceInquiryReadResult {
        Objects.requireNonNull(availability, "availability");
        if ((availability == Availability.AVAILABLE) != (source != null)) {
            throw new IllegalArgumentException("AVAILABLE 상태에서만 원천 결과가 필요합니다.");
        }
    }

    /** 회원과 선택 쿠폰의 존재를 먼저 판정한 원천 조회 결과 상태입니다. */
    public enum Availability {
        AVAILABLE,
        MEMBER_NOT_FOUND,
        COUPON_NOT_FOUND
    }

    /** 조회 후보가 준비된 가용 결과를 만듭니다. */
    public static AdminIssuanceInquiryReadResult available(AdminIssuanceInquirySource source) {
        return new AdminIssuanceInquiryReadResult(Availability.AVAILABLE, source);
    }

    /** 회원 미존재 결과를 만듭니다. */
    public static AdminIssuanceInquiryReadResult memberNotFound() {
        return new AdminIssuanceInquiryReadResult(Availability.MEMBER_NOT_FOUND, null);
    }

    /** 선택 쿠폰 미존재 결과를 만듭니다. */
    public static AdminIssuanceInquiryReadResult couponNotFound() {
        return new AdminIssuanceInquiryReadResult(Availability.COUPON_NOT_FOUND, null);
    }
}
