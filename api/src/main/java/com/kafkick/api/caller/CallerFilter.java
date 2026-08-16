package com.kafkick.api.caller;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * 요청 속성에 {@link Caller} 를 심는다. 헤더가 없거나 형식이 어긋나면 심지 않는다.
 *
 * <p>여기서 요청을 막지 않는다. 이 값은 인증 결과가 아니라 사용자 구분 수단이라
 * <b>거부 판정은 각 유즈케이스가</b> 한다 — 발급 경로는 등급을 회차 조건과 대조하는 식이다.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class CallerFilter extends OncePerRequestFilter {

    static final String ATTRIBUTE = Caller.class.getName();

    private final CallerResolver callerResolver;

    public CallerFilter(CallerResolver callerResolver) {
        this.callerResolver = callerResolver;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        callerResolver.resolve(request).ifPresent(caller -> request.setAttribute(ATTRIBUTE, caller));
        filterChain.doFilter(request, response);
    }
}
