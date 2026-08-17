package com.kafkick.core.admin;

/**
 * 이벤트·문의·관측 집계에 저장할 저카디널리티 사유 코드입니다.
 *
 * <p>자유 문자열이나 예외 메시지를 저장하지 않아 개인정보 노출과 무한한 지표 태그 증가를 막습니다.</p>
 */
public enum ReasonCode {
    CAMPAIGN_NOT_OPEN,
    CAMPAIGN_CLOSED,
    GRADE_NOT_ELIGIBLE,
    QUEUE_REQUIRED,
    NO_ENTRY_TOKEN,
    ENTRY_TOKEN_EXPIRED,
    ALREADY_ISSUED,
    STOCK_EXHAUSTED,
    TEMPORARILY_UNAVAILABLE,
    INTERNAL_ERROR,
    UNMAPPED
}
