package com.kafkick.core.observation;

/**
 * HTTP 진입부터 발급 결과까지의 운영 관측 단계입니다.
 * 발급권 상태 전이를 나타내는 {@code IssuanceEventType}과는 별도 계약입니다.
 */
public enum EventType {

    ENTRY_RESULT,
    QUEUE_ADMITTED,
    ISSUE_RESULT
}
