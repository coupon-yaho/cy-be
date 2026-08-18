package com.kafkick.api.admin.support;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.format.FormatterRegistry;

/** 관리자 URL에만 헤더 기반 권한 경계를 적용하는 MVC 설정입니다. */
@Configuration
public class AdminWebConfig implements WebMvcConfigurer {

    private final AdminAuthorizationInterceptor adminAuthorizationInterceptor;

    public AdminWebConfig(AdminAuthorizationInterceptor adminAuthorizationInterceptor) {
        this.adminAuthorizationInterceptor = adminAuthorizationInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(adminAuthorizationInterceptor)
                .addPathPatterns("/api/v1/admin/**");
    }

    @Override
    public void addFormatters(FormatterRegistry registry) {
        registry.addConverter(new MetricsWindowConverter());
    }
}
