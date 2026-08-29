package com.kafkick.core.observation;

/** HTTP 어댑터 사이에서 공유하는 요청 속성 키. 특정 필터나 예외 처리기 이름에 종속되지 않는다. */
public final class RequestAttributeKeys {

    public static final String DEPENDENCY = RequestAttributeKeys.class.getName() + ".dependency";

    private RequestAttributeKeys() {
    }
}
