package com.kafkick.core.admin;

/** EventRecorder가 발행하는 세 가지 논리 이벤트입니다. */
public enum EventType {
    ENTRY_RESULT,
    QUEUE_ADMITTED,
    ISSUE_RESULT
}
