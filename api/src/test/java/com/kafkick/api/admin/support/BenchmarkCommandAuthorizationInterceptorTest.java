package com.kafkick.api.admin.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.util.AntPathMatcher;

import com.kafkick.core.support.exception.BusinessException;

class BenchmarkCommandAuthorizationInterceptorTest {

    private final BenchmarkCommandAuthorizationInterceptor interceptor =
        new BenchmarkCommandAuthorizationInterceptor("measurement-secret-at-least-32-bytes");

    @Test
    void rejectsBenchmarkMutationWithoutExactSecret() {
        MockHttpServletRequest request = new MockHttpServletRequest(
            "POST", "/api/v1/admin/benchmarks/start");

        assertThatThrownBy(() -> interceptor.preHandle(
            request, new MockHttpServletResponse(), new Object()))
            .isInstanceOfSatisfying(BusinessException.class, exception ->
                assertThat(exception.getErrorCode()).isEqualTo(AdminApiErrorCode.FORBIDDEN));
    }

    @Test
    void acceptsExactSecretAndDoesNotGuardReadOnlyBenchmarkRequests() throws Exception {
        MockHttpServletRequest command = new MockHttpServletRequest(
            "POST", "/api/v1/admin/benchmarks/7/finalize");
        command.addHeader("X-Benchmark-Command-Secret", "measurement-secret-at-least-32-bytes");
        assertThat(interceptor.preHandle(command, new MockHttpServletResponse(), new Object())).isTrue();

        MockHttpServletRequest read = new MockHttpServletRequest(
            "GET", "/api/v1/admin/benchmarks/7");
        assertThat(interceptor.preHandle(read, new MockHttpServletResponse(), new Object())).isTrue();
    }

    @Test
    void missingConfiguredSecretFailsClosed() {
        BenchmarkCommandAuthorizationInterceptor disabled =
            new BenchmarkCommandAuthorizationInterceptor("");
        MockHttpServletRequest request = new MockHttpServletRequest(
            "POST", "/api/v1/admin/benchmarks/7/archive/retry");

        assertThatThrownBy(() -> disabled.preHandle(
            request, new MockHttpServletResponse(), new Object()))
            .isInstanceOf(BusinessException.class);
    }

    @Test
    void shortConfiguredSecretFailsClosed() {
        BenchmarkCommandAuthorizationInterceptor shortSecret =
            new BenchmarkCommandAuthorizationInterceptor("short");
        MockHttpServletRequest request = new MockHttpServletRequest(
            "POST", "/api/v1/admin/benchmarks/start");
        request.addHeader("X-Benchmark-Command-Secret", "short");

        assertThatThrownBy(() -> shortSecret.preHandle(
            request, new MockHttpServletResponse(), new Object()))
            .isInstanceOf(BusinessException.class);
    }

    @Test
    void matrixParameterCannotBypassFinalizeSecret() {
        MockHttpServletRequest request = new MockHttpServletRequest(
            "POST", "/api/v1/admin/benchmarks/7/finalize;a=b");

        assertThatThrownBy(() -> interceptor.preHandle(
            request, new MockHttpServletResponse(), new Object()))
            .isInstanceOf(BusinessException.class);
    }

    @Test
    void benchmarkCommandPatternFailsClosedForFutureNestedCommands() {
        AntPathMatcher matcher = new AntPathMatcher();

        assertThat(matcher.match(
            AdminWebConfig.BENCHMARK_COMMAND_PATH,
            "/api/v1/admin/benchmarks/7/future-command")).isTrue();
        assertThat(matcher.match(
            AdminWebConfig.BENCHMARK_COMMAND_PATH,
            "/api/v1/admin/runtime-config")).isFalse();
    }

    @Test
    void allStateChangingHttpMethodsRequireTheSecret() {
        for (String method : java.util.List.of("POST", "PUT", "PATCH", "DELETE")) {
            MockHttpServletRequest request = new MockHttpServletRequest(
                method, "/api/v1/admin/benchmarks/7/future-command");

            assertThatThrownBy(() -> interceptor.preHandle(
                request, new MockHttpServletResponse(), new Object()))
                .as(method)
                .isInstanceOf(BusinessException.class);
        }
        for (String method : java.util.List.of("GET", "HEAD", "OPTIONS")) {
            MockHttpServletRequest request = new MockHttpServletRequest(
                method, "/api/v1/admin/benchmarks/7");
            assertThatCode(() -> interceptor.preHandle(
                request, new MockHttpServletResponse(), new Object())).doesNotThrowAnyException();
        }
    }
}
