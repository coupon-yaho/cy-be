package com.kafkick.api.admin.observability;

/**
 * Prometheus 질의가 실패했음을 나타냅니다.
 *
 * <p>이 예외는 HTTP 500 으로 번역되지 않습니다. 조립하는 쪽이 잡아서 해당 질의가 채우려던 값만
 * {@code UNAVAILABLE} 로 내려보냅니다 — 원천 하나가 죽었다고 화면 전체가 죽으면 안 됩니다.</p>
 */
public class PromQueryException extends RuntimeException {

    public PromQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public PromQueryException(String message) {
        super(message);
    }
}
