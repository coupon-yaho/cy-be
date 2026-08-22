package com.kafkick.core.support.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.support.TimeProvider;

// API와 Batch가 동일한 UTC 시각 기준을 사용하도록 공통 빈을 제공합니다.
@Configuration
public class TimeConfiguration {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public TimeProvider timeProvider(Clock clock) {
        return new TimeProvider(clock);
    }
}
