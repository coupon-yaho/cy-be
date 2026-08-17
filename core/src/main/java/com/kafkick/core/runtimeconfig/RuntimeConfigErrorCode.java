package com.kafkick.core.runtimeconfig;

import com.kafkick.core.support.exception.ErrorCode;

public enum RuntimeConfigErrorCode implements ErrorCode {

    INVALID_REVISION(400, "RUNTIME_CONFIG-001", "설정 revision은 0 이상이어야 합니다."),
    INVALID_UPDATED_BY(400, "RUNTIME_CONFIG-002", "설정 변경자를 입력해야 합니다.");

    private final int status;
    private final String code;
    private final String message;

    RuntimeConfigErrorCode(int status, String code, String message) {
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
