package com.kafkick.api.support;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

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

    /** 필터가 확정한 요청 ID를 하위 HTTP 어댑터가 읽는 공용 요청 속성 키입니다. */
    public static final String REQUEST_ID_ATTRIBUTE =
            RequestIdFilter.class.getName() + ".requestId";

    private static final String REQUEST_ID = "requestId";
    private static final String HEADER = "X-Request-Id";
    private static final Pattern SAFE_REQUEST_ID = Pattern.compile(
            "^[A-Za-z0-9][A-Za-z0-9._-]{0,35}$"
    );

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String requestId = request.getHeader(HEADER);
        if (!isSafeRequestId(requestId)) {
            requestId = newRequestId();
        }
        MDC.put(REQUEST_ID, requestId);
        request.setAttribute(REQUEST_ID_ATTRIBUTE, requestId);
        response.setHeader(HEADER, requestId);
        try {
            filterChain.doFilter(request, response);
        } finally {
            // 톰캣 스레드가 재사용되므로 지우지 않으면 다음 요청이 남의 requestId 를 물고 간다.
            MDC.remove(REQUEST_ID);
        }
    }

    private static boolean isSafeRequestId(String requestId) {
        return requestId != null
                && SAFE_REQUEST_ID.matcher(requestId).matches();
    }

    private static String newRequestId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
