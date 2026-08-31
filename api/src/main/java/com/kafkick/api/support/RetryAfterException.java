package com.kafkick.api.support;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.ErrorCode;

/**
 * 다시 보내면 풀리는 실패. {@code Retry-After} 헤더를 붙여 클라이언트가 <b>서버가 아니라
 * 자기 쪽에서</b> 기다리게 한다.
 *
 * <p>v2 의 {@code -7}(처리 중)과 {@code -9}(재구성 창)가 여기에 해당한다. 서버가 대신
 * 기다리면(v1 의 폴링) 스파이크에서 톰캣 워커가 그대로 갉아먹힌다.
 *
 * <p><b>{@code -11} 에는 붙이지 않는다.</b> 카운터를 못 읽는 상태는 기다려서 풀리지 않고
 * 사람이 봐야 한다 — 재시도를 권하면 그 요청이 같은 실패로 되돌아온다.
 */
public class RetryAfterException extends BusinessException {

    private final int retryAfterSeconds;

    public RetryAfterException(ErrorCode errorCode, int retryAfterSeconds) {
        super(errorCode);
        // 0 은 "즉시 다시 보내라" 라서 재시도 폭주와 같다. 이 클래스가 막으려는 것 자체다.
        if (retryAfterSeconds < 1) {
            throw new IllegalArgumentException("retryAfterSeconds는 1 이상이어야 합니다.");
        }
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public int retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
