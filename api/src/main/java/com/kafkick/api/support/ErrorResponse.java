package com.kafkick.api.support;

import java.time.Instant;

import com.kafkick.core.support.exception.ErrorCode;

public record ErrorResponse(
        int status,
        String code,
        String message,
        Long currentRevision,
        Object details,
        String requestId,
        Instant timestamp
) {
    public static ErrorResponse of(ErrorCode errorCode, String requestId, Instant timestamp) {
        return new ErrorResponse(
                errorCode.getStatus(),
                errorCode.getCode(),
                errorCode.getMessage(),
                null,
                null,
                requestId,
                timestamp
        );
    }
}
