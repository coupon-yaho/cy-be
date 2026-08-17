package com.kafkick.api.admin.support;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.format.support.DefaultFormattingConversionService;

import com.kafkick.api.support.GlobalExceptionHandler;
import com.kafkick.api.support.RequestIdFilter;
import com.kafkick.api.caller.CallerArgumentResolver;
import com.kafkick.api.caller.CallerFilter;
import com.kafkick.api.caller.HeaderCallerResolver;
import com.kafkick.core.support.TimeProvider;

/**
 * 관리자 Controller 계약 테스트에 공통 MockMvc, Validation, 오류 봉투, 헤더 경계를 구성합니다.
 *
 * <p>일반 Controller 테스트에는 유효한 관리자 헤더를 기본 제공하고, 인증 실패 테스트만 명시적으로
 * 헤더 없는 구성을 사용합니다. 따라서 각 테스트는 검증하려는 API 계약에만 집중할 수 있습니다.</p>
 */
public final class AdminControllerContractTestSupport {

    private AdminControllerContractTestSupport() {
    }

    public static MockMvc mockMvc(Object controller) {
        return build(controller, true);
    }

    /** 인증 실패 HTTP 상태를 검증할 때 기본 관리자 헤더 없이 MockMvc를 구성합니다. */
    public static MockMvc mockMvcWithoutAdminHeaders(Object controller) {
        return build(controller, false);
    }

    private static MockMvc build(Object controller, boolean defaultAdminHeaders) {
        Clock fixedClock = Clock.fixed(Instant.parse("2026-08-16T00:00:00Z"), ZoneOffset.UTC);
        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();
        DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();
        conversionService.addConverter(new MetricsWindowConverter());
        var builder = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler(new TimeProvider(fixedClock)))
                .setValidator(validator)
                .setConversionService(conversionService)
                .addInterceptors(new AdminAuthorizationInterceptor())
                .setCustomArgumentResolvers(new CallerArgumentResolver())
                .addFilters(new CallerFilter(new HeaderCallerResolver()), new RequestIdFilter());
        if (defaultAdminHeaders) {
            // 정상 API 계약 테스트가 인증 실패에 가려지지 않도록 검증된 관리자 헤더를 공통 적용합니다.
            builder.defaultRequest(get("/")
                    .header(HeaderCallerResolver.USER_ID_HEADER, "812934")
                    .header(AdminAuthorizationInterceptor.USER_ROLE_HEADER, "ADMIN"));
        }
        return builder.build();
    }
}
