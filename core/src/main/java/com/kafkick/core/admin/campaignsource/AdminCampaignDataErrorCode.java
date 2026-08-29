package com.kafkick.core.admin.campaignsource;

import com.kafkick.core.support.exception.ErrorCode;

/** 관리자 캠페인 DB 관측 경계의 외부 오류 응답 계약입니다. */
public enum AdminCampaignDataErrorCode implements ErrorCode {

    /** DB 관측 실패로 캠페인 데이터를 확인할 수 없습니다. */
    OBSERVATION_UNAVAILABLE(
            503,
            "ADMIN-CAMPAIGN-001",
            "캠페인 관측 데이터를 조회할 수 없습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    /** HTTP 상태와 클라이언트 오류 표현을 보존합니다. */
    AdminCampaignDataErrorCode(int status, String code, String message) {
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
