package com.kafkick.api.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.filter.CorsFilter;

import com.kafkick.api.coupon.http.CouponRequestHeaders;
import com.kafkick.api.support.auth.MemberRequestHeaders;

/**
 * 설정이 없어도 {@code OPTIONS} 는 200 을 내고 서버 로그에 오류가 안 남는다. 브라우저에서만
 * 막히므로 검사로 고정한다.
 *
 * <p>필터를 직접 태운다 — 재는 것이 {@code CorsConfiguration} 의 값이라 컨텍스트를 띄울
 * 이유가 없다.
 */
class ApiCorsConfigTest {

    private final CorsFilter filter = corsFilter();

    private static CorsFilter corsFilter() {
        FilterRegistrationBean<CorsFilter> registration =
                new ApiCorsConfig().apiCorsFilter(List.of("http://localhost:5173"));
        return registration.getFilter();
    }

    @Test
    @DisplayName("허용한 오리진의 발급 프리플라이트가 통과한다")
    void allowsPreflightFromAllowedOrigin() throws Exception {
        MockHttpServletResponse response = preflight(
                "http://localhost:5173",
                "POST",
                MemberRequestHeaders.MEMBER_ID + ","
                        + MemberRequestHeaders.MEMBER_GRADE + ","
                        + CouponRequestHeaders.IDEMPOTENCY_KEY + ",Content-Type");

        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN))
                .as("없으면 브라우저가 실제 요청을 안 보낸다. 응답 코드는 200 이라 서버에서는 안 보인다")
                .isEqualTo("http://localhost:5173");
    }

    @Test
    @DisplayName("발급이 쓰는 헤더 셋이 전부 허용된다 — 하나만 빠져도 프리플라이트가 막힌다")
    void allowsEveryHeaderTheIssuePathRequires() throws Exception {
        for (String header : List.of(
                MemberRequestHeaders.MEMBER_ID,
                MemberRequestHeaders.MEMBER_GRADE,
                CouponRequestHeaders.IDEMPOTENCY_KEY)) {
            MockHttpServletResponse response =
                    preflight("http://localhost:5173", "POST", header);

            assertThat(response.getHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS))
                    .as("%s 가 허용 목록에 없다. 하나라도 빠지면 요청이 안 나간다", header)
                    .containsIgnoringCase(header);
        }
    }

    @Test
    @DisplayName("Retry-After 와 X-Request-Id 를 노출한다 — 안 하면 스크립트가 못 읽는다")
    void exposesRetryAfterAndRequestId() throws Exception {
        MockHttpServletResponse response =
                preflight("http://localhost:5173", "POST", "Content-Type");

        String exposed = response.getHeader(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS);
        assertThat(exposed)
                .as("노출 안 하면 스크립트가 못 읽는다. Retry-After 를 못 읽으면 "
                        + "v2 의 -7·-9 에서 제 주기로 재시도한다")
                .containsIgnoringCase(HttpHeaders.RETRY_AFTER)
                .containsIgnoringCase(RequestIdFilter.REQUEST_ID_HEADER);
    }

    @Test
    @DisplayName("안 적은 오리진은 안 열린다 — 기본값이 전면 허용이 아니다")
    void rejectsUnknownOrigin() throws Exception {
        MockHttpServletResponse response =
                preflight("http://evil.example.com", "POST", "Content-Type");

        assertThat(response.getHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN))
                .as("기본값이 * 이면 나중에 넓어진 것을 아무도 못 본다")
                .isNull();
    }

    @Test
    @DisplayName("이 모듈에 없는 메서드는 안 연다")
    void doesNotOpenMethodsThisModuleDoesNotHave() throws Exception {
        MockHttpServletResponse response =
                preflight("http://localhost:5173", "DELETE", "Content-Type");

        assertThat(response.getHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN))
                .as("없는 메서드를 열어 두면 나중에 생기는 경로가 검토 없이 통과한다")
                .isNull();
    }

    private MockHttpServletResponse preflight(
            String origin, String method, String requestHeaders) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("OPTIONS",
                "/api/v1/coupons/1/issue");
        request.addHeader(HttpHeaders.ORIGIN, origin);
        request.addHeader(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, method);
        request.addHeader(HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS, requestHeaders);

        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        return response;
    }
}
