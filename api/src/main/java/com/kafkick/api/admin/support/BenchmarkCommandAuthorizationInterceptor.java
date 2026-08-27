package com.kafkick.api.admin.support;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkick.core.support.exception.BusinessException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/** 공개 API 포트에서 실제 benchmark 상태를 바꾸는 명령에 별도 공유 secret을 요구한다. */
@Component
public final class BenchmarkCommandAuthorizationInterceptor implements HandlerInterceptor {

    private static final java.util.Set<String> READ_ONLY_METHODS =
        java.util.Set.of("GET", "HEAD", "OPTIONS");

    private static final Logger log =
        LoggerFactory.getLogger(BenchmarkCommandAuthorizationInterceptor.class);

    public static final String SECRET_HEADER = "X-Benchmark-Command-Secret";

    private final byte[] expectedSecret;

    public BenchmarkCommandAuthorizationInterceptor(
        @Value("${benchmark.admin.command-secret:}") String expectedSecret
    ) {
        this.expectedSecret = expectedSecret.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (!isProtectedCommand(request)) {
            return true;
        }
        String provided = request.getHeader(SECRET_HEADER);
        byte[] actual = provided == null ? new byte[0] : provided.getBytes(StandardCharsets.UTF_8);
        if (expectedSecret.length == 0) {
            log.warn("Benchmark command rejected: reason=secret_not_configured");
            throw new BusinessException(AdminApiErrorCode.FORBIDDEN);
        }
        if (expectedSecret.length < 32) {
            log.warn("Benchmark command rejected: reason=secret_too_short");
            throw new BusinessException(AdminApiErrorCode.FORBIDDEN);
        }
        if (!MessageDigest.isEqual(expectedSecret, actual)) {
            log.warn("Benchmark command rejected: reason=secret_mismatch");
            throw new BusinessException(AdminApiErrorCode.FORBIDDEN);
        }
        return true;
    }

    private static boolean isProtectedCommand(HttpServletRequest request) {
        return !READ_ONLY_METHODS.contains(request.getMethod());
    }
}
