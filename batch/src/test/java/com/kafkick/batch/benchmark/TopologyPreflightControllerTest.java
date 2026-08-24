package com.kafkick.batch.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.kafkick.batch.benchmark.TopologyPreflightController.PreflightResponse;

class TopologyPreflightControllerTest {

    @Test
    @DisplayName("batch preflight는 요청 본문 없이 batch 소유 값만 검사한다")
    void httpContractHasNoCallerReportedApiValues() throws Exception {
        TopologyPreflightController controller = controller(false, 30_000, "10");

        MockMvcBuilders.standaloneSetup(controller).build()
            .perform(get("/internal/v1/benchmarks/preflight").param("couponId", "10"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.valid").value(true))
            .andExpect(jsonPath("$.violations").isEmpty());
    }

    @Test
    @DisplayName("batch 스케줄러와 gap 주기 위반을 실제 로컬값과 함께 반환한다")
    void localViolationsArePreserved() {
        ResponseEntity<PreflightResponse> response = controller(true, 5_000, "10").preflight(10L);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().violations())
            .extracting("key", "expected", "actual")
            .containsExactly(
                org.assertj.core.groups.Tuple.tuple("batch.scheduling.enabled", "false", "true"),
                org.assertj.core.groups.Tuple.tuple(
                    "observation.domain-gauge.aggregate-interval-ms", "30000", "5000")
            );
    }

    @Test
    @DisplayName("Gauge 쿠폰이 비었거나 시작 쿠폰과 다르면 같은 preflight에서 거부한다")
    void rejectsMissingAndDifferentGaugeCoupon() {
        assertThat(controller(false, 30_000, null).preflight(10L).getBody().violations())
            .extracting("key", "expected", "actual")
            .containsExactly(org.assertj.core.groups.Tuple.tuple(
                "observation.domain-gauge.coupon-id", "10", "null"));
        assertThat(controller(false, 30_000, "11").preflight(10L).getBody().violations())
            .extracting("key", "expected", "actual")
            .containsExactly(org.assertj.core.groups.Tuple.tuple(
                "observation.domain-gauge.coupon-id", "10", "11"));
    }

    @Test
    void missingGaugePropertiesFailClosedInsteadOfInventingRuntimeDefaults() {
        MockEnvironment environment = new MockEnvironment()
            .withProperty("batch.scheduling.enabled", "false")
            .withProperty("observation.domain-gauge.coupon-id", "10");

        ResponseEntity<PreflightResponse> response =
            new TopologyPreflightController(environment).preflight(10L);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().violations()).extracting("key", "actual")
            .containsExactly(
                org.assertj.core.groups.Tuple.tuple(
                    "observation.domain-gauge.enabled", "false"),
                org.assertj.core.groups.Tuple.tuple(
                    "observation.domain-gauge.aggregate-interval-ms", "-1"));
    }

    @Test
    void blankGaugeCouponBecomesViolationInsteadOfConversionFailure() {
        MockEnvironment environment = new MockEnvironment()
            .withProperty("batch.scheduling.enabled", "false")
            .withProperty("observation.domain-gauge.enabled", "true")
            .withProperty("observation.domain-gauge.aggregate-interval-ms", "30000")
            .withProperty("observation.domain-gauge.coupon-id", "");

        ResponseEntity<PreflightResponse> response =
            new TopologyPreflightController(environment).preflight(10L);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().violations()).extracting("key", "actual")
            .containsExactly(org.assertj.core.groups.Tuple.tuple(
                "observation.domain-gauge.coupon-id", "null"));
    }

    @Test
    void malformedGaugeCouponBecomesViolationInsteadOfServerError() {
        ResponseEntity<PreflightResponse> response =
            controller(false, 30_000, "not-a-number").preflight(10L);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().violations()).extracting("key", "actual")
            .containsExactly(org.assertj.core.groups.Tuple.tuple(
                "observation.domain-gauge.coupon-id", "null"));
    }

    @Test
    void malformedGaugeIntervalBecomesViolationInsteadOfServerError() {
        MockEnvironment environment = new MockEnvironment()
            .withProperty("batch.scheduling.enabled", "false")
            .withProperty("observation.domain-gauge.enabled", "true")
            .withProperty("observation.domain-gauge.aggregate-interval-ms", "not-a-number")
            .withProperty("observation.domain-gauge.coupon-id", "10");

        ResponseEntity<PreflightResponse> response =
            new TopologyPreflightController(environment).preflight(10L);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().violations()).extracting("key", "actual")
            .contains(org.assertj.core.groups.Tuple.tuple(
                "observation.domain-gauge.aggregate-interval-ms", "-1"));
    }

    @Test
    void disabledDomainGaugeClosesThePreflightGate() {
        MockEnvironment environment = new MockEnvironment()
            .withProperty("batch.scheduling.enabled", "false")
            .withProperty("observation.domain-gauge.enabled", "false")
            .withProperty("observation.domain-gauge.aggregate-interval-ms", "30000")
            .withProperty("observation.domain-gauge.coupon-id", "10");

        ResponseEntity<PreflightResponse> response =
            new TopologyPreflightController(new TopologyValidator(30_000L), environment)
                .preflight(10L);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().violations()).extracting("key", "actual")
            .containsExactly(org.assertj.core.groups.Tuple.tuple(
                "observation.domain-gauge.enabled", "false"));
    }

    private static TopologyPreflightController controller(
        boolean schedulingEnabled, long intervalMs, String couponId
    ) {
        MockEnvironment environment = new MockEnvironment()
            .withProperty("batch.scheduling.enabled", Boolean.toString(schedulingEnabled))
            .withProperty("observation.domain-gauge.enabled", "true")
            .withProperty("observation.domain-gauge.aggregate-interval-ms", Long.toString(intervalMs));
        if (couponId != null) {
            environment.withProperty("observation.domain-gauge.coupon-id", couponId);
        }
        return new TopologyPreflightController(new TopologyValidator(30_000L), environment);
    }
}
