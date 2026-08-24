package com.kafkick.core.support.exception;

public class ForbiddenException extends BusinessException {

    public ForbiddenException(String detail) {
        super(CommonErrorCode.FORBIDDEN, detail);
    }
}
