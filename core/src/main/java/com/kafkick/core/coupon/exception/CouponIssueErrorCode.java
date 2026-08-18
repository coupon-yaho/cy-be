// 쿠폰 발급 과정의 비즈니스 실패를 공통 응답 코드로 정의합니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.ErrorCode;

public enum CouponIssueErrorCode implements ErrorCode {

    INVALID_COUPON_ISSUE_REQUEST(400, "COUPON-300", "쿠폰 발급 요청 값이 올바르지 않습니다."),
    COUPON_ROUND_NOT_FOUND(404, "COUPON-301", "쿠폰 회차를 찾을 수 없습니다."),
    NOT_OPENED(409, "COUPON-302", "아직 쿠폰 발급이 시작되지 않았습니다."),
    CAMPAIGN_CLOSED(409, "COUPON-303", "쿠폰 발급이 마감되었습니다."),
    GRADE_NOT_ELIGIBLE(403, "COUPON-304", "쿠폰 발급 대상 등급이 아닙니다."),
    ALREADY_ISSUED(409, "COUPON-305", "이미 발급받은 쿠폰입니다."),
    SOLD_OUT(409, "COUPON-306", "쿠폰 재고가 모두 소진되었습니다."),
    COUPON_ISSUE_SAVE_FAILED(500, "COUPON-307", "쿠폰 발급 저장 중 오류가 발생했습니다."),
    COUPON_STOCK_NOT_FOUND(500, "COUPON-308", "쿠폰 재고 정보를 찾을 수 없습니다."),
    MEMBER_NOT_FOUND(404, "COUPON-309", "회원을 찾을 수 없습니다."),
    INVALID_TRANSITION(409, "COUPON-310", "허용되지 않은 쿠폰 상태 전이입니다.");

    private final int status;
    private final String code;
    private final String message;

    CouponIssueErrorCode(int status, String code, String message) {
        this.status = status;
        this.code = code;
        this.message = message;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
