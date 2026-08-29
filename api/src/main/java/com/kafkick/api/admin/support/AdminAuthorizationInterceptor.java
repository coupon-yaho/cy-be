package com.kafkick.api.admin.support;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Component;
import org.springframework.web.cors.CorsUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import com.kafkick.core.support.exception.BusinessException;

/**
 * 관리자 HTTP 요청의 역할 헤더만 검증합니다.
 *
 * <p>{@code X-User-Id} 파싱과 {@code Caller} 생성은 기존 caller 필터·resolver가 전담합니다.
 * 이 interceptor는 관리자 경로에서 정확한 {@code X-User-Role: ADMIN}만 확인하며, 브라우저의 CORS
 * preflight는 실제 요청이 아니므로 역할 헤더 없이 통과시킵니다.</p>
 */
@Component
public final class AdminAuthorizationInterceptor implements HandlerInterceptor {

    public static final String USER_ROLE_HEADER = "X-User-Role";
    private static final String ADMIN_ROLE = "ADMIN";

    /**
     * 관리자 역할을 확인합니다. 호출자 ID는 앞서 실행되는 기존 CallerFilter가 처리합니다.
     *
     * @throws BusinessException 관리자 역할이 누락됐거나 정확한 ADMIN이 아닌 경우 403
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (CorsUtils.isPreFlightRequest(request)) {
            return true;
        }
        String rawRole = request.getHeader(USER_ROLE_HEADER);
        if (!ADMIN_ROLE.equals(rawRole)) {
            throw new BusinessException(AdminApiErrorCode.FORBIDDEN);
        }
        return true;
    }
}
