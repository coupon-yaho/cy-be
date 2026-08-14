// 모든 관리자 API 경로에 역할 검증 인터셉터를 공통 적용합니다.
package com.kafkick.api.support;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration(proxyBeanMethods = false)
public class AdminApiWebMvcConfiguration implements WebMvcConfigurer {

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new AdminRoleInterceptor())
                .addPathPatterns("/api/v1/admin/**");
    }
}
