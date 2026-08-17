package com.kafkick.core.support.exception;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorCodeTest {

    @Test
    void reasonCodeIsOptionalForExistingErrorContracts() {
        ErrorCode errorCode = new ErrorCode() {
            @Override
            public int getStatus() {
                return 500;
            }

            @Override
            public String getCode() {
                return "TEST-001";
            }

            @Override
            public String getMessage() {
                return "test";
            }
        };

        assertThat(errorCode.reasonCode()).isEmpty();
    }
}
