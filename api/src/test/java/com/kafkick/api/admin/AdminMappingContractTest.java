package com.kafkick.api.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.web.bind.annotation.RequestMapping;

import com.kafkick.api.admin.benchmark.AdminBenchmarkController;
import com.kafkick.api.admin.campaign.AdminCampaignController;
import com.kafkick.api.admin.dashboard.AdminDashboardController;
import com.kafkick.api.admin.issuance.AdminIssuanceController;
import com.kafkick.api.admin.measurement.AdminMeasurementController;
import com.kafkick.api.admin.notification.AdminNotificationController;
import com.kafkick.api.admin.observability.AdminObservabilityController;
import com.kafkick.api.admin.runtimeconfig.AdminRuntimeConfigController;
import com.kafkick.api.admin.verification.AdminVerificationController;
import com.kafkick.api.caller.Caller;

/** 관리자 API 선구축 범위가 정확히 9개 Controller와 27개 mapping인지 회귀 검증합니다. */
class AdminMappingContractTest {

    private static final Set<Class<?>> CONTROLLERS = Set.of(
            AdminDashboardController.class,
            AdminIssuanceController.class,
            AdminNotificationController.class,
            AdminObservabilityController.class,
            AdminBenchmarkController.class,
            AdminCampaignController.class,
            AdminVerificationController.class,
            AdminMeasurementController.class,
            AdminRuntimeConfigController.class);

    private static final Set<String> EXPECTED_ROUTES = Set.of(
            "GET /api/v1/admin/overview",
            "GET /api/v1/admin/coupons/{couponId}/metrics",
            "GET /api/v1/admin/analytics",
            "GET /api/v1/admin/members/issuance-inquiries",
            "GET /api/v1/admin/issuance-histories",
            "POST /api/v1/admin/notifications/{notificationId}/resend",
            "GET /api/v1/admin/notifications/summary",
            "GET /api/v1/admin/notifications/failures",
            "GET /api/v1/admin/metrics",
            "GET /api/v1/admin/events",
            "GET /api/v1/admin/benchmarks",
            "GET /api/v1/admin/benchmarks/{benchmarkRunId}",
            "POST /api/v1/admin/benchmarks/start",
            "POST /api/v1/admin/benchmarks/{benchmarkRunId}/stop",
            "POST /api/v1/admin/benchmarks/{benchmarkRunId}/finalize",
            "POST /api/v1/admin/benchmarks/{benchmarkRunId}/k6-result",
            "GET /api/v1/admin/campaigns",
            "GET /api/v1/admin/brands",
            "GET /api/v1/admin/templates",
            "POST /api/v1/admin/campaigns/{campaignId}/status-transitions",
            "POST /api/v1/admin/verify",
            "GET /api/v1/admin/verification-runs",
            "GET /api/v1/admin/verification-runs/{runId}",
            "POST /api/v1/admin/measurements/start",
            "POST /api/v1/admin/measurements/stop",
            "GET /api/v1/admin/runtime-config",
            "PUT /api/v1/admin/runtime-config");

    /** HTTP 메서드와 전체 경로 집합을 비교해 endpoint 누락·추가·병합을 동시에 감지합니다. */
    @Test
    @DisplayName("API 선구축 완료 기준인 9개 Controller와 27개 HTTP mapping을 정확히 유지한다")
    void exposesExactlyTwentySevenPrebuiltAdminMappings() {
        Set<String> actual = CONTROLLERS.stream()
                .flatMap(controller -> routes(controller).stream())
                .collect(Collectors.toSet());

        assertThat(CONTROLLERS).hasSize(9);
        assertThat(actual).containsExactlyInAnyOrderElementsOf(EXPECTED_ROUTES);
        assertThat(actual).hasSize(27);
        assertThat(CONTROLLERS.stream()
                .flatMap(controller -> Arrays.stream(controller.getDeclaredMethods()))
                .filter(this::isMappedHandler)
                .filter(method -> Arrays.asList(method.getParameterTypes()).contains(Caller.class)))
                .as("27개 관리자 handler는 모두 기존 Caller를 필수 인자로 받아야 합니다.")
                .hasSize(27);
    }

    private Set<String> routes(Class<?> controller) {
        RequestMapping classMapping = AnnotatedElementUtils.findMergedAnnotation(controller, RequestMapping.class);
        String basePath = classMapping == null || classMapping.path().length == 0 ? "" : classMapping.path()[0];
        return Arrays.stream(controller.getDeclaredMethods())
                .map(method -> route(method, basePath))
                .filter(route -> route != null)
                .collect(Collectors.toSet());
    }

    private String route(Method method, String basePath) {
        RequestMapping mapping = AnnotatedElementUtils.findMergedAnnotation(method, RequestMapping.class);
        if (mapping == null || mapping.method().length != 1) {
            return null;
        }
        String methodPath = mapping.path().length == 0 ? "" : mapping.path()[0];
        return mapping.method()[0].name() + " " + basePath + methodPath;
    }

    private boolean isMappedHandler(Method method) {
        return AnnotatedElementUtils.findMergedAnnotation(method, RequestMapping.class) != null;
    }
}
