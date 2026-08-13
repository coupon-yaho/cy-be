package com.kafkick.core.support.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class TimeConfig {

    /** DB 서버가 default-time-zone='+00:00' 이므로 앱 시계도 UTC 로 맞춘다. KST 변환은 표현 계층에서. */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
