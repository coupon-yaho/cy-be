package com.kafkick.core.support.exception;

public interface ErrorCode {

    int getStatus();

    String getCode();

    String getMessage();
}
