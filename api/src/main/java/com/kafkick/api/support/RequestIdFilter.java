package com.kafkick.api.support;

import java.io.IOException;
import java.util.UUID;

import org.slf4j.MDC;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** 요청마다 requestId 를 MDC 에 심어 로그와 에러 응답을 같은 키로 묶는다. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestIdFilter extends OncePerRequestFilter {

    private static final String REQUEST_ID = "requestId";
    private static final String HEADER = "X-Request-Id";

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        // 내부 상관 ID는 클라이언트 입력과 분리해 요청마다 새로 생성한다.
        String requestId = generate();
        MDC.put(REQUEST_ID, requestId);
        response.setHeader(HEADER, requestId);
        try {
            filterChain.doFilter(request, response);
        } finally {
            // 톰캣 스레드가 재사용되므로 지우지 않으면 다음 요청이 남의 requestId 를 물고 간다.
            MDC.remove(REQUEST_ID);
        }
    }

    private static String generate() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
