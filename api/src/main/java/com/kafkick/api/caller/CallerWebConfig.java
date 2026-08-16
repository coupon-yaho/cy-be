package com.kafkick.api.caller;

import java.util.List;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class CallerWebConfig implements WebMvcConfigurer {

    private final CallerArgumentResolver callerArgumentResolver;

    public CallerWebConfig(CallerArgumentResolver callerArgumentResolver) {
        this.callerArgumentResolver = callerArgumentResolver;
    }

    @Override
    public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
        resolvers.add(callerArgumentResolver);
    }
}
