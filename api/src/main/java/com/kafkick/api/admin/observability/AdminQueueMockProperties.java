package com.kafkick.api.admin.observability;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 관리자 대기열 Mock 원천을 명시적으로 켜는 API 전용 설정입니다. */
@ConfigurationProperties(prefix = "admin.queue")
public class AdminQueueMockProperties {

    private boolean mockEnabled = false;

    /** 대기열 Mock 원천을 사용할지 반환합니다. */
    public boolean isMockEnabled() {
        return mockEnabled;
    }

    /** 운영 기본값을 바꾸지 않고 외부 설정이 명시한 Mock 사용 여부를 반영합니다. */
    public void setMockEnabled(boolean mockEnabled) {
        this.mockEnabled = mockEnabled;
    }
}
