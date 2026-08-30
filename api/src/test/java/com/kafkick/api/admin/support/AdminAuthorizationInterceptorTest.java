package com.kafkick.api.admin.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.core.support.exception.BusinessException;

/** 관리자 interceptor의 역할 검사와 CORS preflight 예외 동작을 단위 검증합니다. */
class AdminAuthorizationInterceptorTest {

    private final AdminAuthorizationInterceptor interceptor = new AdminAuthorizationInterceptor();

    /** 호출자 ID와 무관하게 정확한 ADMIN 역할이면 관리자 경계를 통과시키는지 검증합니다. */
    @Test
    void acceptsExactAdminRoleWithoutParsingCallerId() throws Exception {
        MockHttpServletRequest request = request(null, "ADMIN");

        assertThat(interceptor.preHandle(request, new MockHttpServletResponse(), new Object())).isTrue();
    }

    /** 브라우저 CORS preflight는 관리자 역할 헤더 없이도 interceptor가 차단하지 않는지 검증합니다. */
    @Test
    void allowsCorsPreflightWithoutAdminHeaders() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("OPTIONS", "/api/v1/admin/overview");
        request.addHeader("Origin", "https://admin.example.com");
        request.addHeader("Access-Control-Request-Method", "GET");

        assertThat(interceptor.preHandle(request, new MockHttpServletResponse(), new Object())).isTrue();
    }

    /** 역할이 없거나 정확한 ADMIN이 아니면 관리자 권한 오류를 발생시키는지 검증합니다. */
    @Test
    void rejectsMissingOrNonAdminRoleAsForbidden() {
        assertThatThrownBy(() -> interceptor.preHandle(
                request("812934", null), new MockHttpServletResponse(), new Object()))
                .isInstanceOfSatisfying(BusinessException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(AdminApiErrorCode.FORBIDDEN));

        assertThatThrownBy(() -> interceptor.preHandle(
                request("812934", "admin"), new MockHttpServletResponse(), new Object()))
                .isInstanceOfSatisfying(BusinessException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(AdminApiErrorCode.FORBIDDEN));
    }

    private MockHttpServletRequest request(String userId, String role) {
        MockHttpServletRequest request = new MockHttpServletRequest();
        if (userId != null) {
            request.addHeader(MemberRequestHeaders.MEMBER_ID, userId);
        }
        if (role != null) {
            request.addHeader("X-User-Role", role);
        }
        return request;
    }
}
