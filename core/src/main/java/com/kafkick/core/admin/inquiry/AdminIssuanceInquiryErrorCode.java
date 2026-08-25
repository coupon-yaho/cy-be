package com.kafkick.core.admin.inquiry;

import com.kafkick.core.observation.Dependency;
import com.kafkick.core.support.exception.ErrorCode;

/** 회원 발급 문의 원천 조회와 후보 계약의 안정적인 오류 응답입니다. */
public enum AdminIssuanceInquiryErrorCode implements ErrorCode {

    /** 요청한 회원이 확인된 원천에 존재하지 않습니다. */
    MEMBER_NOT_FOUND(
            404,
            "ADMIN-INQUIRY-001",
            "회원을 찾을 수 없습니다."
    ),

    /** 요청한 쿠폰이 확인된 원천에 존재하지 않습니다. */
    COUPON_NOT_FOUND(
            404,
            "ADMIN-INQUIRY-002",
            "쿠폰을 찾을 수 없습니다."
    ),

    /** MySQL 원천 조회를 완료할 수 없어 문의 결과를 제공할 수 없습니다. */
    SOURCE_UNAVAILABLE(
            503,
            "ADMIN-INQUIRY-003",
            "발급 문의 데이터를 조회할 수 없습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    /** HTTP 상태와 외부 오류 코드를 보존합니다. */
    AdminIssuanceInquiryErrorCode(int status, String code, String message) {
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

    /** MySQL 원천 읽기 실패만 관측 의존성으로 분류합니다. */
    @Override
    public Dependency dependency() {
        return this == SOURCE_UNAVAILABLE ? Dependency.MYSQL : ErrorCode.super.dependency();
    }
}
