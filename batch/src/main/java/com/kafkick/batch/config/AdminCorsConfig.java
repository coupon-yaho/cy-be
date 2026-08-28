// 관제 화면이 브라우저에서 관리자 API 를 부를 수 있게 오리진을 허용합니다.
package com.kafkick.batch.config;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;

/**
 * <b>관제가 검증을 손으로 돌릴 수 있게 한다.</b> 트리거·조회가 이미 HTTP 로 열려 있는데
 * ({@code /api/v1/admin/**}) 브라우저는 오리진이 다르면 <b>응답을 읽지 못한다</b> — 지금까지
 * 이 API 를 부른 것이 전부 {@code curl} 이라 그 벽에 안 부딪혔다.
 *
 * <p>⚠️ <b>{@code WebMvcConfigurer.addCorsMappings} 가 아니라 {@link CorsFilter} 다.</b>
 * 전자는 {@code DispatcherServlet} <b>안쪽</b>의 인터셉터로 구현돼서,
 * {@link AdminTokenFilter} 처럼 <b>필터가 체인을 끊고 직접 응답하면 헤더가 안 붙는다</b> —
 * 실측에서 토큰이 틀린 401 응답에 {@code Access-Control-Allow-Origin} 이 <b>없었다</b>.
 * 그러면 관제 화면은 401 을 못 읽고 <b>"CORS 오류"</b> 로만 보게 되어, 원인이 토큰이라는 것을
 * 아무도 못 찾는다. 필터로 두면 관문보다 앞서 헤더를 붙이므로 <b>거절 응답도 읽힌다</b>.
 *
 * <p>⚠️ <b>CORS 는 방어가 아니다.</b> 브라우저가 스스로 지키는 규칙이라 {@code curl} 이나
 * 서버 대 서버 요청에는 아무 효력이 없다. 이것을 <i>"관리자 API 를 보호했다"</i> 로 읽으면 안 된다 —
 * {@code docs/11} §11 이 정한 1차 방어선은 <b>포트 미노출</b>이고, 소지를 묻는 2차 방어선은
 * {@link AdminTokenFilter} 다. 오리진을 늘려도 닿을 수 있는 사람이 늘지 않는다.
 *
 * <p><b>그래서 {@code *} 를 기본값으로 두지 않는다.</b> 방어가 아니라도 기본값이 전면 허용이면
 * 나중에 바인딩을 넓히는 날 <b>같이 넓어진 것을 아무도 못 본다.</b> 기본은 로컬 개발 서버 넷이고,
 * 다른 호스트에 띄우면 {@code BATCH_ADMIN_CORS_ORIGINS} 로 그 오리진만 적는다.
 *
 * <p><b>인증 정보는 안 싣는다</b>({@code allowCredentials} 를 안 켠다). 이 API 에는 쿠키도
 * 세션도 없고 — 토큰은 {@link AdminTokenFilter#HEADER} 로 명시적으로 싣는다 — 켜면
 * 와일드카드 오리진이 <b>런타임 예외</b>가 되어 손잡이만 더 날카로워진다.
 *
 * <p><b>관리 포트(9092)는 이 설정과 무관하다.</b> {@code management.server.port} 가 다르면
 * 액추에이터가 <b>별도 서블릿 컨텍스트</b>에서 뜨고 이 필터는 주 컨텍스트에만 등록된다 —
 * {@code AdminCorsConfigTest} 가 두 포트에 같은 프리플라이트를 보내 그 차이를 단언한다.
 */
@Configuration
public class AdminCorsConfig {

    private static final Logger log = LoggerFactory.getLogger(AdminCorsConfig.class);

    /** 관리자 경로만 연다. 다른 경로가 생겨도 여기 안 적으면 브라우저에서 안 읽힌다. */
    static final String ADMIN_PATTERN = "/api/v1/admin/**";

    static final String ORIGINS = "batch.admin.cors.allowed-origins";

    /**
     * <b>기본값을 코드에도 둔다.</b> 설정 파일에만 두면 그 키가 없는 컨텍스트에서
     * {@code PlaceholderResolutionException} 으로 <b>기동이 통째로 막힌다</b> — 실제로 그렇게
     * 만들었다가 테스트 204개가 한 번에 죽었다. 형제 가드 넷이 전부 {@code @Value} 에
     * 기본값을 들고 있는 것도 같은 이유다.
     */
    static final String DEFAULT_ORIGINS = "http://localhost:3000,http://127.0.0.1:3000,"
            + "http://localhost:5173,http://127.0.0.1:5173";

    /**
     * <b>토큰 관문보다 앞</b>에 서야 거절 응답에도 헤더가 붙는다.
     *
     * <p><b>{@code HIGHEST_PRECEDENCE} 를 그대로 쓰지 않는다.</b> 그 값이
     * {@code Integer.MIN_VALUE} 라, 여기서 빼는 식으로 순서를 조정하면 <b>언더플로로
     * 감싸 돌아</b> 의도와 정반대가 된다 — 순서를 뒤집는 돌연변이를 넣었는데 테스트가
     * 안 죽어서 알았다. 여유를 두고 시작해 앞뒤로 움직일 자리를 남긴다.
     */
    static final int ORDER = Ordered.HIGHEST_PRECEDENCE + 1000;

    @Bean
    public FilterRegistrationBean<CorsFilter> adminCorsFilter(
            @Value("${" + ORIGINS + ":" + DEFAULT_ORIGINS + "}") List<String> allowedOrigins) {

        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(List.copyOf(allowedOrigins));
        // GET 은 조회, POST 는 트리거·중단·복구다. 나머지는 이 API 에 없다 — 없는 메서드를
        // 미리 열어 두면 나중에 생기는 경로가 검토 없이 통과한다.
        configuration.setAllowedMethods(List.of("GET", "POST"));
        // 관제는 토큰을 헤더로 싣는다. 그 이름을 안 열면 프리플라이트에서 막힌다.
        configuration.setAllowedHeaders(List.of(AdminTokenFilter.HEADER, "Content-Type"));
        // 프리플라이트를 매 요청마다 보내지 않게 한다. 짧게 둔 이유는 오리진을 고칠 때
        // 브라우저가 옛 답을 오래 들고 있으면 "고쳤는데 왜 안 되나" 로 시간을 버려서다.
        configuration.setMaxAge(600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration(ADMIN_PATTERN, configuration);

        log.info("관리자 API CORS 오리진 {}개를 허용합니다: {}", allowedOrigins.size(), allowedOrigins);

        FilterRegistrationBean<CorsFilter> registration =
                new FilterRegistrationBean<>(new CorsFilter(source));
        registration.addUrlPatterns("/api/v1/admin/*");
        registration.setOrder(ORDER);
        return registration;
    }
}
