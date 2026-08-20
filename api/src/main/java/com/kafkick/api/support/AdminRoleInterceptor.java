package com.kafkick.api.support;

import com.kafkick.core.support.exception.ForbiddenException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.servlet.HandlerInterceptor;

public class AdminRoleInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(
            HttpServletRequest request,
            HttpServletResponse response,
            Object handler
    ) {
        String userRole = request.getHeader(AdminRequestHeaders.USER_ROLE);
        if (!AdminRequestHeaders.ADMIN_ROLE.equals(userRole)) {
            throw new ForbiddenException(
                    "관리자 역할 헤더가 없거나 올바르지 않습니다."
            );
        }
        return true;
    }
}
