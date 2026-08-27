package com.kafkick.api.caller;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class CallerFilterConfiguration {

    @Bean
    public CallerFilter callerFilter(CallerResolver callerResolver) {
        return new CallerFilter(callerResolver);
    }
}
