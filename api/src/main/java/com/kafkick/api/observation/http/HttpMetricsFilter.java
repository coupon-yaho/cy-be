package com.kafkick.api.observation.http;

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerMapping;

import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.RequestAttributeKeys;

/**
 * 대시보드용 저카디널리티 HTTP 결과와 지연을 기록한다.
 *
 * <p>다섯 업무 URI 그룹에 속하지 않는 404·스캔 요청은 결과·지연 모집단에서 의도적으로
 * 제외한다. UNKNOWN을 추가하면 업무 트래픽과 인터넷 잡음이 한 시계열에 섞이기 때문이다.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 20)
public final class HttpMetricsFilter extends OncePerRequestFilter {

    private final ResultClassifier classifier;
    private final HttpMetrics metrics;
    private final InFlightRegistry inFlight;

    public HttpMetricsFilter(
            ResultClassifier classifier, HttpMetrics metrics, InFlightRegistry inFlight) {
        this.classifier = classifier;
        this.metrics = metrics;
        this.inFlight = inFlight;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        String contextPath = request.getContextPath();
        if (!contextPath.isEmpty() && path.startsWith(contextPath)) {
            path = path.substring(contextPath.length());
        }
        return isExcludedPath(path);
    }

    @Override
    protected boolean shouldNotFilterAsyncDispatch() {
        // 최초 REQUEST에 등록한 AsyncListener가 완료 시 한 번 기록한다.
        return true;
    }

    @Override
    protected boolean shouldNotFilterErrorDispatch() {
        // 최초 REQUEST 또는 예외 catch가 기록하므로 ERROR 재디스패치는 중복 계측하지 않는다.
        return true;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        InFlightRegistry.InFlightToken token = inFlight.enter();
        long startedAt = System.nanoTime();
        Completion completion = new Completion(request, response, token, startedAt);
        try {
            filterChain.doFilter(request, response);
        } catch (IOException | ServletException | RuntimeException | Error failure) {
            try {
                if (request.isAsyncStarted()) {
                    completion.listen(request);
                } else {
                    completion.finish(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                }
            } catch (Throwable metricsFailure) {
                failure.addSuppressed(metricsFailure);
            }
            throw failure;
        } finally {
            if (!completion.isHandled()) {
                if (request.isAsyncStarted()) {
                    completion.listen(request);
                } else {
                    completion.finish(response.getStatus());
                }
            }
        }
    }

    private final class Completion implements AsyncListener {

        private final HttpServletRequest request;
        private final HttpServletResponse response;
        private final InFlightRegistry.InFlightToken token;
        private final long startedAt;
        private final AtomicBoolean recorded = new AtomicBoolean();
        private final AtomicBoolean released = new AtomicBoolean();
        private final AtomicBoolean listenerAttached = new AtomicBoolean();

        private Completion(
                HttpServletRequest request,
                HttpServletResponse response,
                InFlightRegistry.InFlightToken token,
                long startedAt) {
            this.request = request;
            this.response = response;
            this.token = token;
            this.startedAt = startedAt;
        }

        @Override
        public void onComplete(AsyncEvent event) {
            try {
                recordOnce(response.getStatus());
            } finally {
                releaseOnce();
            }
        }

        @Override
        public void onTimeout(AsyncEvent event) {
            // 요청 제한시간 안에 완료하지 못한 것은 응답 status와 무관하게 서버 실패다.
            recordOnce(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }

        @Override
        public void onError(AsyncEvent event) {
            recordOnce(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }

        @Override
        public void onStartAsync(AsyncEvent event) {
            listenerAttached.set(false);
            listen(event.getAsyncContext());
        }

        private boolean isHandled() {
            return released.get() || listenerAttached.get();
        }

        private void listen(HttpServletRequest asyncRequest) {
            try {
                if (!asyncRequest.isAsyncStarted()) {
                    finish(response.getStatus());
                    return;
                }
                listen(asyncRequest.getAsyncContext());
            } catch (IllegalStateException asyncAlreadyCompleted) {
                finish(response.getStatus());
            }
        }

        private void listen(AsyncContext context) {
            if (released.get() || !listenerAttached.compareAndSet(false, true)) {
                return;
            }
            try {
                context.addListener(this);
            } catch (IllegalStateException asyncAlreadyCompleted) {
                listenerAttached.set(false);
                finish(response.getStatus());
            }
        }

        private void recordOnce(int status) {
            if (!recorded.compareAndSet(false, true)) {
                return;
            }
            UriGroup.of(request.getMethod(), matchingPattern(request)).ifPresent(uriGroup -> {
                ResultClassifier.ResultClass resultClass = classifier.classify(
                        status, dependency(request));
                metrics.record(uriGroup, resultClass, System.nanoTime() - startedAt);
            });
        }

        private void releaseOnce() {
            if (released.compareAndSet(false, true)) {
                token.close();
            }
        }

        private void finish(int status) {
            // URI 매핑이 없거나 계측 중 예외가 나도 in-flight는 먼저 반드시 원복한다.
            releaseOnce();
            recordOnce(status);
        }
    }

    private static String matchingPattern(HttpServletRequest request) {
        Object pattern = request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        return pattern instanceof String value ? value : "";
    }

    private static Dependency dependency(HttpServletRequest request) {
        Object value = request.getAttribute(RequestAttributeKeys.DEPENDENCY);
        return value instanceof Dependency dependency ? dependency : Dependency.NONE;
    }

    public enum UriGroup {
        ISSUE,
        ENTRY,
        QUEUE,
        READ,
        USE;

        public String tagValue() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static Optional<UriGroup> of(String method, String pattern) {
            if (method == null || pattern == null || pattern.isBlank() || isExcludedPath(pattern)) {
                return Optional.empty();
            }

            String normalizedMethod = method.toUpperCase(Locale.ROOT);
            if (!"GET".equals(normalizedMethod) && !"POST".equals(normalizedMethod)) {
                return Optional.empty();
            }

            String[] segments = pattern.split("/");
            if ("POST".equals(normalizedMethod)
                    && isItemAction(segments, "campaigns", "issue")) {
                return Optional.of(ISSUE);
            }
            if ("POST".equals(normalizedMethod)
                    && isItemAction(segments, "campaigns", "entry")) {
                return Optional.of(ENTRY);
            }
            if ("GET".equals(normalizedMethod)
                    && isItemAction(segments, "campaigns", "queue")) {
                return Optional.of(QUEUE);
            }
            if ("GET".equals(normalizedMethod)) {
                return Optional.of(READ);
            }
            if (isItemAction(segments, "coupons", "use")
                    || isItemAction(segments, "coupons", "cancel-use")
                    || isItemAction(segments, "coupons", "cancel")) {
                return Optional.of(USE);
            }
            return Optional.empty();
        }

        private static boolean isItemAction(String[] segments, String resource, String action) {
            int length = segments.length;
            return length >= 4
                    && resource.equals(segments[length - 3])
                    && segments[length - 2].startsWith("{")
                    && segments[length - 2].endsWith("}")
                    && action.equals(segments[length - 1]);
        }

    }

    private static boolean isExcludedPath(String path) {
        return path.equals("/actuator") || path.startsWith("/actuator/")
                || path.equals("/admin") || path.startsWith("/admin/")
                || path.equals("/api/v1/admin") || path.startsWith("/api/v1/admin/");
    }
}
