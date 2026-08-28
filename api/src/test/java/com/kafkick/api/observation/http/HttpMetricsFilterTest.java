package com.kafkick.api.observation.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.ServletException;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.kafkick.api.observation.MeterNames;
import com.kafkick.api.coupon.controller.CouponCancelController;
import com.kafkick.api.coupon.controller.CouponCancelUseController;
import com.kafkick.api.coupon.controller.CouponIssueController;
import com.kafkick.api.coupon.controller.CouponUseController;
import com.kafkick.api.coupon.controller.MemberCouponController;
import com.kafkick.api.observation.http.HttpMetricsFilter.UriGroup;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.RequestAttributeKeys;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.ErrorCode;
import com.kafkick.api.support.GlobalExceptionHandler;

class HttpMetricsFilterTest {

    private static final Set<Class<?>> COUPON_CONTROLLERS = Set.of(
            CouponIssueController.class,
            CouponUseController.class,
            CouponCancelUseController.class,
            CouponCancelController.class,
            MemberCouponController.class);

    private static final Set<ControllerMapping> EXPECTED_COUPON_MAPPINGS = Set.of(
            mapping(RequestMethod.GET, "/api/v1/coupons", UriGroup.READ),
            mapping(RequestMethod.POST, "/api/v1/coupons/{couponRoundId}/issue", UriGroup.ISSUE),
            mapping(RequestMethod.POST, "/api/v1/coupons/{issuanceId}/use", UriGroup.USE),
            mapping(RequestMethod.POST, "/api/v1/coupons/{issuanceId}/cancel-use", UriGroup.USE),
            mapping(RequestMethod.POST, "/api/v1/coupons/{issuanceId}/cancel", UriGroup.USE));

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(HttpMetricsFilterConfiguration.class)
            .withBean(ResultClassifier.class)
            .withBean(SimpleMeterRegistry.class)
            .withBean(HttpMetrics.class)
            .withBean(InFlightRegistry.class);

    @Test
    void filterConfigurationRegistersHttpMetricsFilter() {
        contextRunner.run(context -> assertThat(context).hasSingleBean(HttpMetricsFilter.class));
    }

    @Test
    void mapsTemplatesWithoutUsingActualResourceIds() {
        assertThat(UriGroup.of("POST", "/api/v1/coupons/{couponRoundId}/issue"))
                .contains(UriGroup.ISSUE);
        assertThat(UriGroup.of("POST", "/api/v1/coupons/{couponRoundId}/entry")).isEmpty();
        assertThat(UriGroup.of("GET", "/api/v1/coupons/{couponRoundId}/queue")).isEmpty();
        assertThat(UriGroup.of("GET", "/api/v1/brands")).contains(UriGroup.READ);
        assertThat(UriGroup.of("POST", "/api/v1/coupons/{couponId}/cancel-use"))
                .contains(UriGroup.USE);
        assertThat(UriGroup.of("POST", "/api/v1/coupons/42/issue")).isEmpty();
        assertThat(UriGroup.of("GET", "/api/v1/admin/campaigns")).isEmpty();
        assertThat(UriGroup.of("GET", "/admin/campaigns")).isEmpty();
        assertThat(UriGroup.of("GET", "/actuator/prometheus")).isEmpty();
    }

    @Test
    void couponControllersExposeExactlyTheExpectedUriGroups() {
        Set<ControllerMapping> actual = COUPON_CONTROLLERS.stream()
                .flatMap(controller -> controllerMappings(controller).stream())
                .collect(Collectors.toSet());

        assertThat(COUPON_CONTROLLERS).hasSize(5);
        assertThat(actual).containsExactlyInAnyOrderElementsOf(EXPECTED_COUPON_MAPPINGS);
        assertThat(actual).hasSize(5);
    }

    private static Set<ControllerMapping> controllerMappings(Class<?> controller) {
        RequestMapping root = requiredMapping(controller);
        return Arrays.stream(controller.getDeclaredMethods())
                .map(method -> AnnotatedElementUtils.findMergedAnnotation(
                        method, RequestMapping.class))
                .filter(mapping -> mapping != null)
                .flatMap(endpoint -> Arrays.stream(paths(root))
                        .flatMap(rootPath -> Arrays.stream(paths(endpoint))
                                .flatMap(endpointPath -> Arrays.stream(endpoint.method())
                                        .map(method -> classifiedMapping(
                                                method, rootPath + endpointPath)))))
                .collect(Collectors.toSet());
    }

    private static RequestMapping requiredMapping(Class<?> controller) {
        RequestMapping mapping = AnnotatedElementUtils.findMergedAnnotation(
                controller, RequestMapping.class);
        if (mapping == null) {
            throw new IllegalStateException(
                    "Controller has no @RequestMapping: " + controller.getName());
        }
        return mapping;
    }

    private static String[] paths(RequestMapping mapping) {
        if (mapping.path().length == 0) {
            return new String[] {""};
        }
        return mapping.path();
    }

    private static ControllerMapping classifiedMapping(RequestMethod method, String path) {
        return new ControllerMapping(method, path, UriGroup.of(method.name(), path));
    }

    private static ControllerMapping mapping(
            RequestMethod method, String path, UriGroup group) {
        return new ControllerMapping(method, path, Optional.of(group));
    }

    private record ControllerMapping(
            RequestMethod method, String path, Optional<UriGroup> uriGroup) {
    }

    /**
     * [OBS-31] Timer 가 둘에서 넷이 됐습니다. 표본이 하나도 없는 축도 <b>등록은 되어 있어야</b>
     * 합니다 — 첫 요청이 와서야 시계열이 생기면 화면이 그 사이 '축이 없음' 과 '값이 없음' 을
     * 구분할 수 없습니다.
     */
    @Test
    void registersSixCountersAndFourTimersForEveryUriGroup() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new HttpMetrics(registry);

        for (UriGroup group : UriGroup.values()) {
            assertThat(registry.find(MeterNames.HTTP_RESULT)
                    .tag("uri_group", group.tagValue()).counters()).hasSize(6);
            assertThat(registry.find(MeterNames.HTTP_LATENCY)
                    .tag("uri_group", group.tagValue()).timers()).hasSize(4);
            for (HttpMetrics.LatencyOutcome outcome : HttpMetrics.LatencyOutcome.values()) {
                assertThat(registry.find(MeterNames.HTTP_LATENCY)
                        .tags("uri_group", group.tagValue(), "outcome", outcome.tagValue())
                        .timer())
                        .as("%s · %s 축이 등록되지 않았습니다", group, outcome)
                        .isNotNull();
            }
        }
    }

    @Test
    void keepsClientInvalidAndPolicyRejectOutOfSystemFailures() throws Exception {
        Fixture fixture = new Fixture();

        for (int i = 0; i < 100; i++) {
            fixture.exchange("POST", "/api/v1/coupons/{couponRoundId}/issue", 400, Dependency.NONE);
        }
        for (int i = 0; i < 100; i++) {
            fixture.exchange("POST", "/api/v1/coupons/{couponRoundId}/issue", 409, Dependency.NONE);
        }

        assertThat(fixture.counter("issue", "client_invalid").count()).isEqualTo(100);
        assertThat(fixture.counter("issue", "policy_reject").count()).isEqualTo(100);
        assertThat(fixture.counter("issue", "dependency_failure").count()).isZero();
        assertThat(fixture.counter("issue", "application_failure").count()).isZero();
    }

    @Test
    void recordsDependencyAndApplicationFailuresExclusively() throws Exception {
        Fixture fixture = new Fixture();

        fixture.exchange("POST", "/api/v1/coupons/{couponRoundId}/issue", 500, Dependency.REDIS);
        fixture.exchange("POST", "/api/v1/coupons/{couponRoundId}/issue", 503, Dependency.REDIS);
        fixture.exchange("POST", "/api/v1/coupons/{couponRoundId}/issue", 500, Dependency.NONE);

        assertThat(fixture.counter("issue", "dependency_failure").count()).isEqualTo(2);
        assertThat(fixture.counter("issue", "application_failure").count()).isEqualTo(1);
    }

    @Test
    void actualIssueMappingRecordsOneCounterAndOneTimerSample() throws Exception {
        Fixture fixture = new Fixture();

        fixture.exchange(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", 201, Dependency.NONE);

        assertThat(fixture.counter("issue", "success").count()).isEqualTo(1);
        assertThat(fixture.timer("issue", "success").count()).isEqualTo(1);
    }

    @Test
    void restoresInFlightWhenTheChainThrows() {
        Fixture fixture = new Fixture();
        MockHttpServletRequest request = fixture.request(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", Dependency.NONE);

        assertThatThrownBy(() -> fixture.filter.doFilter(
                request, new MockHttpServletResponse(), (req, res) -> {
                    throw new ServletException("forced");
                })).isInstanceOf(ServletException.class).hasMessage("forced");

        assertThat(fixture.inFlight().value()).isZero();
        assertThat(fixture.counter("issue", "application_failure").count()).isEqualTo(1);
        assertThat(fixture.counter("issue", "success").count()).isZero();
    }

    @Test
    void restoresInFlightWhenUriTemplateHasNoMetricsGroup() throws Exception {
        Fixture fixture = new Fixture();
        fixture.rawExchange("POST", "/not-grouped", "/not-grouped", 204);

        assertThat(fixture.inFlight().value()).isZero();
        assertThat(fixture.registry.find(MeterNames.HTTP_RESULT).counters())
                .allMatch(counter -> counter.count() == 0.0);
    }

    @Test
    void defersInFlightAndLatencyUntilAsyncCompletion() throws Exception {
        Fixture fixture = new Fixture();
        MockHttpServletRequest request = fixture.request(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", Dependency.NONE);
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.setAsyncSupported(true);

        fixture.filter.doFilter(request, response, (req, res) -> req.startAsync());

        assertThat(fixture.inFlight().value()).isEqualTo(1);
        assertThat(fixture.counter("issue", "success").count()).isZero();

        response.setStatus(201);
        request.getAsyncContext().complete();

        assertThat(fixture.inFlight().value()).isZero();
        assertThat(fixture.counter("issue", "success").count()).isEqualTo(1);
    }

    @Test
    void closesInFlightWhenAsyncContextDisappearsBeforeListenerRegistration() throws Exception {
        Fixture fixture = new Fixture();
        MockHttpServletRequest request = spy(fixture.request(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", Dependency.NONE));
        when(request.isAsyncStarted()).thenReturn(true);
        when(request.getAsyncContext()).thenThrow(new IllegalStateException("already complete"));

        fixture.filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> { });

        assertThat(fixture.inFlight().value()).isZero();
        assertThat(fixture.counter("issue", "success").count()).isEqualTo(1);
    }

    @Test
    void closesInFlightWhenCompletedContextRejectsListenerRegistration() throws Exception {
        Fixture fixture = new Fixture();
        AsyncContext context = mock(AsyncContext.class);
        doThrow(new IllegalStateException("already complete"))
                .when(context).addListener(any(AsyncListener.class));
        MockHttpServletRequest request = spy(fixture.request(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", Dependency.NONE));
        when(request.isAsyncStarted()).thenReturn(true);
        when(request.getAsyncContext()).thenReturn(context);

        fixture.filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> { });

        assertThat(fixture.inFlight().value()).isZero();
        assertThat(fixture.counter("issue", "success").count()).isEqualTo(1);
    }

    @Test
    void timeoutRecordsFailureButReleasesOnlyAtCompletion() throws Exception {
        Fixture fixture = new Fixture();
        AtomicReference<AsyncListener> listener = new AtomicReference<>();
        AsyncContext context = mock(AsyncContext.class);
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(0));
            return null;
        }).when(context).addListener(any(AsyncListener.class));
        MockHttpServletRequest request = spy(fixture.request(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", Dependency.NONE));
        when(request.isAsyncStarted()).thenReturn(true);
        when(request.getAsyncContext()).thenReturn(context);

        fixture.filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> { });
        listener.get().onTimeout(new AsyncEvent(context));

        assertThat(fixture.counter("issue", "application_failure").count()).isEqualTo(1);
        assertThat(fixture.inFlight().value()).isEqualTo(1);

        listener.get().onComplete(new AsyncEvent(context));

        assertThat(fixture.counter("issue", "application_failure").count()).isEqualTo(1);
        assertThat(fixture.inFlight().value()).isZero();
    }

    @Test
    void asyncRestartRegistrationFailureClosesInFlight() throws Exception {
        Fixture fixture = new Fixture();
        AtomicReference<AsyncListener> listener = new AtomicReference<>();
        AsyncContext initialContext = mock(AsyncContext.class);
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(0));
            return null;
        }).when(initialContext).addListener(any(AsyncListener.class));
        MockHttpServletRequest request = spy(fixture.request(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", Dependency.NONE));
        when(request.isAsyncStarted()).thenReturn(true);
        when(request.getAsyncContext()).thenReturn(initialContext);
        fixture.filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> { });

        AsyncContext restartedContext = mock(AsyncContext.class);
        doThrow(new IllegalStateException("already complete"))
                .when(restartedContext).addListener(any(AsyncListener.class));
        listener.get().onStartAsync(new AsyncEvent(restartedContext));

        assertThat(fixture.inFlight().value()).isZero();
        assertThat(fixture.counter("issue", "success").count()).isEqualTo(1);
    }

    @Test
    void asyncChainFailureWaitsForLifecycleCompletionBeforeRelease() throws Exception {
        Fixture fixture = new Fixture();
        AtomicReference<AsyncListener> listener = new AtomicReference<>();
        AsyncContext context = mock(AsyncContext.class);
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(0));
            return null;
        }).when(context).addListener(any(AsyncListener.class));
        MockHttpServletRequest request = spy(fixture.request(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", Dependency.NONE));
        when(request.isAsyncStarted()).thenReturn(true);
        when(request.getAsyncContext()).thenReturn(context);

        assertThatThrownBy(() -> fixture.filter.doFilter(
                request, new MockHttpServletResponse(), (req, res) -> {
                    throw new IOException("client disconnected");
                })).isInstanceOf(IOException.class).hasMessage("client disconnected");

        assertThat(fixture.inFlight().value()).isEqualTo(1);
        assertThat(fixture.counter("issue", "application_failure").count()).isZero();

        listener.get().onError(new AsyncEvent(context));
        assertThat(fixture.counter("issue", "application_failure").count()).isEqualTo(1);
        assertThat(fixture.inFlight().value()).isEqualTo(1);

        listener.get().onComplete(new AsyncEvent(context));
        assertThat(fixture.inFlight().value()).isZero();
        assertThat(fixture.counter("issue", "application_failure").count()).isEqualTo(1);
    }

    @Test
    void springMvcCallableIsMeasuredOnceAtAsyncCompletion() throws Exception {
        Fixture fixture = new Fixture();
        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new AsyncIssueController())
                .addFilters(fixture.filter)
                .build();

        var pending = mockMvc.perform(post("/api/v1/coupons/42/issue"))
                .andExpect(request().asyncStarted())
                .andReturn();

        assertThat(fixture.inFlight().value()).isEqualTo(1);
        assertThat(fixture.counter("issue", "success").count()).isZero();

        mockMvc.perform(asyncDispatch(pending)).andExpect(status().isCreated());

        assertThat(fixture.inFlight().value()).isZero();
        assertThat(fixture.counter("issue", "success").count()).isEqualTo(1);
    }

    @Test
    void redispatchPolicyKeepsAsyncAndErrorDispatchesOutOfTheFilterBody() {
        Fixture fixture = new Fixture();

        assertThat(fixture.filter.shouldNotFilterAsyncDispatch()).isTrue();
        assertThat(fixture.filter.shouldNotFilterErrorDispatch()).isTrue();
    }

    /**
     * <b>URI 그룹</b> 축을 봅니다. 같은 성공 경로라도 그룹이 다르면 다른 Timer 여야 합니다 —
     * 한 Timer 에 모으면 무거운 발급과 가벼운 조회가 한 분포에 섞입니다.
     *
     * <p>outcome 축을 가르는 것은 {@link #separatesPolicyRejectFromSystemFailureLatency} 와
     * {@link #queueAcceptedRidesTheSuccessAxis} 의 몫이라 여기서는 보지 않습니다.</p>
     */
    @Test
    void separatesLatencyByUriGroup() {
        Fixture fixture = new Fixture();
        for (int i = 0; i < 100; i++) {
            fixture.metrics.record(UriGroup.ISSUE, ResultClassifier.ResultClass.SUCCESS, 300_000_000);
            fixture.metrics.record(UriGroup.QUEUE, ResultClassifier.ResultClass.SUCCESS, 100_000);
        }

        Timer issueSuccess = fixture.timer("issue", "success");
        Timer queueSuccess = fixture.timer("queue", "success");

        assertThat(issueSuccess.max(TimeUnit.NANOSECONDS))
                .isGreaterThan(queueSuccess.max(TimeUnit.NANOSECONDS) * 1_000);
        // 그룹이 서로의 표본을 삼키지 않는다.
        assertThat(issueSuccess.count()).isEqualTo(100);
        assertThat(queueSuccess.count()).isEqualTo(100);
    }

    /**
     * [OBS-31] 이 티켓의 본론입니다. 정책 거절은 재고 소진 판정으로 끝나 1ms 미만이고 시스템
     * 실패는 타임아웃까지 끌립니다. 한 Timer 에 넣으면 <b>거절이 쏟아질수록 실패 지연이 좋아
     * 보입니다</b> — 화면이 정확히 반대로 읽습니다.
     */
    @Test
    void separatesPolicyRejectFromSystemFailureLatency() {
        Fixture fixture = new Fixture();
        for (int i = 0; i < 1000; i++) {
            fixture.metrics.record(UriGroup.ISSUE, ResultClassifier.ResultClass.POLICY_REJECT, 500_000);
        }
        fixture.metrics.record(UriGroup.ISSUE, ResultClassifier.ResultClass.DEPENDENCY_FAILURE, 3_000_000_000L);
        fixture.metrics.record(UriGroup.ISSUE, ResultClassifier.ResultClass.APPLICATION_FAILURE, 2_000_000_000L);

        Timer policyReject = fixture.timer("issue", "policy_reject");
        Timer systemFailure = fixture.timer("issue", "system_failure");

        assertThat(policyReject.count()).isEqualTo(1000);
        assertThat(systemFailure.count()).isEqualTo(2);
        // 거절 1000 건이 실패 지연을 끌어내리지 않는다.
        assertThat(systemFailure.max(TimeUnit.NANOSECONDS)).isEqualTo(3_000_000_000L);
        assertThat(policyReject.max(TimeUnit.NANOSECONDS)).isEqualTo(500_000L);
    }

    /**
     * 4xx 계약 위반은 <b>자기 축</b>을 가집니다. 정책 거절로 새면 인증 실패·라우팅 실패가
     * '정책상 거절' 로 읽히고, 그 축이 다시 못 믿을 값이 됩니다.
     *
     * <p><b>계측 계층이 스스로 증명해야 하는 성질입니다.</b> 조립기 쪽 계약 테스트가 같은 것을
     * 보지만 그쪽 목적은 "노출하지 않는다" 이고, 이 모듈만 따로 돌렸을 때도 축이 갈리는지가
     * 확인돼야 합니다.</p>
     */
    @Test
    void clientInvalidGetsItsOwnLatencyAxis() {
        Fixture fixture = new Fixture();
        fixture.metrics.record(UriGroup.ISSUE, ResultClassifier.ResultClass.CLIENT_INVALID, 7_000_000);
        fixture.metrics.record(UriGroup.ISSUE, ResultClassifier.ResultClass.POLICY_REJECT, 500_000);
        fixture.metrics.record(UriGroup.ISSUE, ResultClassifier.ResultClass.SUCCESS, 300_000_000);

        assertThat(fixture.timer("issue", "client_invalid").count()).isEqualTo(1);
        assertThat(fixture.timer("issue", "client_invalid").max(TimeUnit.NANOSECONDS))
                .isEqualTo(7_000_000L);
        // 서로의 표본을 삼키지 않는다.
        assertThat(fixture.timer("issue", "policy_reject").count()).isEqualTo(1);
        assertThat(fixture.timer("issue", "success").count()).isEqualTo(1);
        assertThat(fixture.timer("issue", "system_failure").count()).isZero();
    }

    /** 202 는 성공 축입니다. 대기열 진입이 실패로 세지면 큐 경로의 지연이 통째로 뒤집힙니다. */
    @Test
    void queueAcceptedRidesTheSuccessAxis() {
        Fixture fixture = new Fixture();
        fixture.metrics.record(UriGroup.QUEUE, ResultClassifier.ResultClass.QUEUE_ACCEPTED, 7_000_000);

        assertThat(fixture.timer("queue", "success").count()).isEqualTo(1);
        assertThat(fixture.timer("queue", "system_failure").count()).isZero();
    }

    @Test
    void exceptionHandlerDependencyContractFeedsTheFilter() throws Exception {
        Fixture fixture = new Fixture();
        GlobalExceptionHandler handler = new GlobalExceptionHandler(new TimeProvider(
                Clock.fixed(Instant.parse("2026-08-19T00:00:00Z"), ZoneOffset.UTC)));
        ErrorCode redisFailure = new ErrorCode() {
            public int getStatus() { return 500; }
            public String getCode() { return "TEST-REDIS"; }
            public String getMessage() { return "redis failed"; }
            public Dependency dependency() { return Dependency.REDIS; }
        };
        MockHttpServletRequest request = fixture.request(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", Dependency.NONE);

        fixture.filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> {
            var handled = handler.handleBusinessException(new BusinessException(redisFailure), request);
            ((MockHttpServletResponse) res).setStatus(handled.getStatusCode().value());
        });

        assertThat(fixture.counter("issue", "dependency_failure").count()).isEqualTo(1);
        assertThat(fixture.counter("issue", "application_failure").count()).isZero();
    }

    @Test
    void unexpectedExceptionExplicitlyFeedsApplicationFailure() throws Exception {
        Fixture fixture = new Fixture();
        GlobalExceptionHandler handler = new GlobalExceptionHandler(new TimeProvider(
                Clock.fixed(Instant.parse("2026-08-19T00:00:00Z"), ZoneOffset.UTC)));
        MockHttpServletRequest request = fixture.request(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", Dependency.REDIS);

        fixture.filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> {
            var handled = handler.handleUnexpected(new IllegalStateException("forced"), request);
            ((MockHttpServletResponse) res).setStatus(handled.getStatusCode().value());
        });

        assertThat(request.getAttribute(RequestAttributeKeys.DEPENDENCY)).isEqualTo(Dependency.NONE);
        assertThat(fixture.counter("issue", "application_failure").count()).isEqualTo(1);
        assertThat(fixture.counter("issue", "dependency_failure").count()).isZero();
    }

    @Test
    void metricsFailureDoesNotReplaceOriginalChainFailure() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HttpMetrics throwingMetrics = mock(HttpMetrics.class);
        RuntimeException metricsFailure = new RuntimeException("metrics failed");
        doThrow(metricsFailure).when(throwingMetrics).record(any(), any(), anyLong());
        HttpMetricsFilter filter = new HttpMetricsFilter(
                new ResultClassifier(), throwingMetrics, new InFlightRegistry(registry));
        MockHttpServletRequest request = new MockHttpServletRequest(
                "POST", "/api/v1/coupons/42/issue");
        request.setAttribute(
                HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE,
                "/api/v1/coupons/{couponRoundId}/issue");
        ServletException original = new ServletException("chain failed");

        assertThatThrownBy(() -> filter.doFilter(
                request, new MockHttpServletResponse(), (req, res) -> { throw original; }))
                .isSameAs(original)
                .satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(metricsFailure));
        assertThat(registry.get(MeterNames.IN_FLIGHT).gauge().value()).isZero();
    }

    @Test
    void standardMvcFiveHundredExplicitlyFeedsApplicationFailure() throws Exception {
        Fixture fixture = new Fixture();
        ExposedExceptionHandler handler = new ExposedExceptionHandler();
        MockHttpServletRequest request = fixture.request(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", Dependency.REDIS);

        fixture.filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> {
            ResponseEntity<Object> handled = handler.standardFiveHundred(request);
            ((MockHttpServletResponse) res).setStatus(handled.getStatusCode().value());
        });

        assertThat(request.getAttribute(RequestAttributeKeys.DEPENDENCY))
                .isEqualTo(Dependency.NONE);
        assertThat(fixture.counter("issue", "application_failure").count()).isEqualTo(1);
        assertThat(fixture.counter("issue", "dependency_failure").count()).isZero();
    }

    @Test
    void nonServletWebRequestDoesNotBlockStandardErrorResponse() {
        ExposedExceptionHandler handler = new ExposedExceptionHandler();

        ResponseEntity<Object> response = handler.standardFiveHundred(mock(WebRequest.class));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void constraintViolationExplicitlyClearsDependencyMapping() throws Exception {
        Fixture fixture = new Fixture();
        GlobalExceptionHandler handler = new GlobalExceptionHandler(new TimeProvider(
                Clock.fixed(Instant.parse("2026-08-19T00:00:00Z"), ZoneOffset.UTC)));
        MockHttpServletRequest request = fixture.request(
                "POST", "/api/v1/coupons/{couponRoundId}/issue", Dependency.REDIS);

        fixture.filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> {
            var handled = handler.handleConstraintViolation(
                    new ConstraintViolationException(Set.of()), request);
            ((MockHttpServletResponse) res).setStatus(handled.getStatusCode().value());
        });

        assertThat(request.getAttribute(RequestAttributeKeys.DEPENDENCY))
                .isEqualTo(Dependency.NONE);
        assertThat(fixture.counter("issue", "client_invalid").count()).isEqualTo(1);
        assertThat(fixture.counter("issue", "dependency_failure").count()).isZero();
    }

    @Test
    void requestBodyValidationUsesNeutralDependencyAttributeContract() throws Exception {
        Fixture fixture = new Fixture();
        MockMvc mockMvc = validationMockMvc(fixture);

        var result = mockMvc.perform(post("/api/v1/coupons/42/issue")
                        .requestAttr(RequestAttributeKeys.DEPENDENCY, Dependency.REDIS)
                        .contentType("application/json")
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andReturn();

        assertThat(result.getRequest().getAttribute(RequestAttributeKeys.DEPENDENCY))
                .isEqualTo(Dependency.NONE);
    }

    @Test
    void methodValidationUsesNeutralDependencyAttributeContract() throws Exception {
        Fixture fixture = new Fixture();
        MockMvc mockMvc = validationMockMvc(fixture);

        var result = mockMvc.perform(get("/api/v1/campaigns/0/validation")
                        .requestAttr(RequestAttributeKeys.DEPENDENCY, Dependency.REDIS))
                .andExpect(status().isBadRequest())
                .andReturn();

        assertThat(result.getRequest().getAttribute(RequestAttributeKeys.DEPENDENCY))
                .isEqualTo(Dependency.NONE);
    }

    @Test
    void resultTagsAreStableInTurkishLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr"));
            Fixture fixture = new Fixture();
            assertThat(fixture.counter("issue", "client_invalid")).isNotNull();
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void tokenCloseIsIdempotentAcrossTwoThousandConcurrentCallers() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        InFlightRegistry inFlight = new InFlightRegistry(registry);
        InFlightRegistry.InFlightToken token = inFlight.enter();
        CountDownLatch ready = new CountDownLatch(2_000);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 2_000; i++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    token.close();
                    return null;
                }));
            }
            ready.await();
            start.countDown();
            for (var future : futures) {
                future.get();
            }
        }

        assertThat(registry.get(MeterNames.IN_FLIGHT).gauge().value()).isZero();
    }

    @Test
    void adminAndActuatorRequestsDoNotCreateOrUpdateMeters() throws Exception {
        Fixture fixture = new Fixture();
        Set<Meter> before = Set.copyOf(fixture.registry.getMeters());

        fixture.rawExchange("GET", "/api/v1/admin/metrics", "/api/v1/admin/metrics", 200);
        fixture.rawExchange("GET", "/admin/metrics", "/admin/metrics", 200);
        fixture.rawExchange("GET", "/actuator/prometheus", "/actuator/prometheus", 200);

        assertThat(fixture.registry.getMeters()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(fixture.registry.find(MeterNames.HTTP_RESULT).counters())
                .allMatch(counter -> counter.count() == 0.0);
    }

    @Test
    void hotPathHasNoInfrastructureCollaborator() {
        assertThat(HttpMetricsFilter.class.getDeclaredFields())
                .extracting(field -> field.getType().getName())
                .noneMatch(type -> type.toLowerCase().contains("redis"));
        assertThat(InFlightRegistry.class.getDeclaredFields())
                .extracting(field -> field.getType().getName())
                .containsExactly("java.util.concurrent.atomic.AtomicInteger");
    }

    private static final class Fixture {
        private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        private final HttpMetrics metrics = new HttpMetrics(registry);
        private final HttpMetricsFilter filter = new HttpMetricsFilter(
                new ResultClassifier(), metrics, new InFlightRegistry(registry));

        private void exchange(String method, String pattern, int status, Dependency dependency)
                throws ServletException, IOException {
            rawExchange(method, pattern.replace("{id}", "42"), pattern, status, dependency);
        }

        private void rawExchange(String method, String uri, String pattern, int status)
                throws ServletException, IOException {
            rawExchange(method, uri, pattern, status, Dependency.NONE);
        }

        private void rawExchange(String method, String uri, String pattern, int status, Dependency dependency)
                throws ServletException, IOException {
            MockHttpServletRequest request = request(method, pattern, dependency);
            request.setRequestURI(uri);
            filter.doFilter(request, new MockHttpServletResponse(),
                    (req, res) -> ((MockHttpServletResponse) res).setStatus(status));
        }

        private MockHttpServletRequest request(String method, String pattern, Dependency dependency) {
            MockHttpServletRequest request = new MockHttpServletRequest(method, pattern);
            request.setAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE, pattern);
            request.setAttribute(RequestAttributeKeys.DEPENDENCY, dependency);
            return request;
        }

        private Counter counter(String uriGroup, String result) {
            return registry.get(MeterNames.HTTP_RESULT)
                    .tags("uri_group", uriGroup, "result", result).counter();
        }

        private Timer timer(String uriGroup, String outcome) {
            return registry.get(MeterNames.HTTP_LATENCY)
                    .tags("uri_group", uriGroup, "outcome", outcome).timer();
        }

        private Gauge inFlight() {
            return registry.get(MeterNames.IN_FLIGHT).gauge();
        }
    }

    private static MockMvc validationMockMvc(Fixture fixture) {
        return MockMvcBuilders.standaloneSetup(new ValidationController())
                .setControllerAdvice(new GlobalExceptionHandler(new TimeProvider(Clock.fixed(
                        Instant.parse("2026-08-19T00:00:00Z"), ZoneOffset.UTC))))
                .addFilters(fixture.filter)
                .build();
    }

    private static final class ExposedExceptionHandler extends GlobalExceptionHandler {

        private ExposedExceptionHandler() {
            super(new TimeProvider(Clock.fixed(
                    Instant.parse("2026-08-19T00:00:00Z"), ZoneOffset.UTC)));
        }

        private ResponseEntity<Object> standardFiveHundred(MockHttpServletRequest request) {
            return standardFiveHundred(new ServletWebRequest(request));
        }

        private ResponseEntity<Object> standardFiveHundred(WebRequest request) {
            return handleExceptionInternal(
                    new HttpMessageNotWritableException("forced"),
                    null,
                    HttpHeaders.EMPTY,
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    request);
        }
    }

    @RestController
    private static final class AsyncIssueController {

        @PostMapping("/api/v1/coupons/{couponRoundId}/issue")
        private Callable<ResponseEntity<Void>> issue(@PathVariable long couponRoundId) {
            return () -> ResponseEntity.status(HttpStatus.CREATED).build();
        }
    }

    @RestController
    private static final class ValidationController {

        @PostMapping("/api/v1/coupons/{couponRoundId}/issue")
        private ResponseEntity<Void> body(
                @PathVariable long couponRoundId, @Valid @RequestBody ValidationBody body) {
            return ResponseEntity.noContent().build();
        }

        @GetMapping("/api/v1/campaigns/{id}/validation")
        private ResponseEntity<Void> method(@PathVariable @Min(1) long id) {
            return ResponseEntity.noContent().build();
        }
    }

    private record ValidationBody(@NotBlank String value) {
    }
}
