package com.kafkick.api.support;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;

import com.kafkick.api.admin.support.AdminAuthorizationInterceptor;
import com.kafkick.api.admin.support.BenchmarkCommandAuthorizationInterceptor;
import com.kafkick.api.coupon.http.CouponRequestHeaders;
import com.kafkick.api.support.auth.MemberRequestHeaders;

/**
 * 브라우저에서 이 API 를 부를 수 있게 오리진을 허용한다.
 *
 * <p>설정이 없어도 {@code OPTIONS} 는 {@code 200} 을 낸다. 서버 로그에는 아무 오류가 안 남고
 * 브라우저에서만 막히므로, 없을 때의 증상이 원인을 안 가리킨다. {@code curl} 과 k6 는 CORS 를
 * 안 보기 때문에 이 벽에 안 부딪힌다.
 *
 * <p>{@code WebMvcConfigurer.addCorsMappings} 대신 {@link CorsFilter} 를 쓴다. 전자는
 * {@code DispatcherServlet} 안쪽이라 필터가 체인을 끊고 직접 응답하면 헤더가 안 붙는다 —
 * 그러면 화면은 401 을 못 읽고 CORS 오류로만 본다.
 *
 * <p>CORS 는 방어가 아니다. 브라우저만 지키는 규칙이라 오리진을 늘려도 닿을 수 있는 사람은
 * 안 늘어난다. 그래도 {@code *} 를 기본값으로 두지 않는 것은, 나중에 바인딩을 넓히는 날
 * 같이 넓어진 것을 아무도 못 보기 때문이다.
 *
 * <p>경로는 {@code /api/**} 하나다. 이 모듈이 여는 것이 전부 그 아래이고, 경로를 하나씩
 * 적으면 새 컨트롤러가 생길 때마다 원인을 안 가리키는 그 증상으로 나타난다.
 */
@Configuration
public class ApiCorsConfig {

    private static final Logger log = LoggerFactory.getLogger(ApiCorsConfig.class);

    static final String PATTERN = "/api/**";

    static final String ORIGINS = "api.cors.allowed-origins";

    /** 설정 파일에만 두면 그 키가 없는 컨텍스트에서 기동이 막힌다. */
    static final String DEFAULT_ORIGINS = "http://localhost:3000,http://127.0.0.1:3000,"
            + "http://localhost:5173,http://127.0.0.1:5173";

    /**
     * 다른 필터보다 앞에 서야 거절 응답에도 헤더가 붙는다.
     *
     * <p>{@code HIGHEST_PRECEDENCE} 가 {@code Integer.MIN_VALUE} 라 거기서 빼면 언더플로로
     * 감싸 돈다. 여유를 두고 시작한다.
     */
    static final int ORDER = Ordered.HIGHEST_PRECEDENCE + 1000;

    @Bean
    public FilterRegistrationBean<CorsFilter> apiCorsFilter(
            @Value("${" + ORIGINS + ":" + DEFAULT_ORIGINS + "}") List<String> allowedOrigins) {

        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(List.copyOf(allowedOrigins));
        configuration.setAllowedMethods(List.of("GET", "POST"));

        // 요청 헤더가 하나라도 안 열려 있으면 브라우저가 실제 요청을 안 보낸다.
        //
        // ⚠️ LEGACY_MEMBER_GRADE 는 **서버가 안 읽는데도 여기 있다.** 프론트가 게이트웨이
        //    전환기 동안 등급 헤더를 두 이름으로 함께 보내는데, 허용 목록에서 빼면 그 요청이
        //    프리플라이트에서 통째로 막힌다 — 서버가 값을 무시하는 것과 브라우저가 보낼 수
        //    있는 것은 다른 층이다. 값은 MemberGradeHeaderResolver 가 안 본다.
        //    프론트가 옛 이름을 그만 보내면 이 줄을 지운다.
        configuration.setAllowedHeaders(List.of(
                HttpHeaders.CONTENT_TYPE,
                MemberRequestHeaders.MEMBER_ID,
                MemberRequestHeaders.MEMBER_GRADE,
                MemberRequestHeaders.LEGACY_MEMBER_GRADE,
                CouponRequestHeaders.IDEMPOTENCY_KEY,
                RequestIdFilter.REQUEST_ID_HEADER,
                AdminAuthorizationInterceptor.USER_ROLE_HEADER,
                BenchmarkCommandAuthorizationInterceptor.SECRET_HEADER));

        // 응답에 실려 오는 것과 스크립트가 읽을 수 있는 것은 다르다. Retry-After 는 v2 의
        // -7·-9 응답에 붙는데, 못 읽으면 클라이언트가 제 주기로 재시도한다.
        configuration.setExposedHeaders(List.of(
                HttpHeaders.RETRY_AFTER,
                RequestIdFilter.REQUEST_ID_HEADER));

        // 오리진을 고쳤을 때 브라우저가 옛 답을 오래 들고 있지 않도록 짧게 둔다.
        configuration.setMaxAge(600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration(PATTERN, configuration);

        log.info("API CORS 오리진 {}개를 허용합니다: {}", allowedOrigins.size(), allowedOrigins);

        FilterRegistrationBean<CorsFilter> registration =
                new FilterRegistrationBean<>(new CorsFilter(source));
        registration.addUrlPatterns("/api/*");
        registration.setOrder(ORDER);
        return registration;
    }
}
