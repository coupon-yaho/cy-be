package com.kafkick.api.admin.observability.mockserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.observability.PromMetricsAssembler;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.observability.dto.AdminMetricsResponse.QueueZone;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

class MockPromQueryContractTest {

    @Test
    @DisplayName("loaded 시나리오는 자원·in-flight·입장 대기열에 실제 조립 가능한 값을 제공한다")
    void loadedScenarioProvidesSaturationSamples() {
        AdminMetricsResponse response = assembleLoaded();

        assertThat(response.saturation().resources().subList(0, 4))
                .allSatisfy(row -> assertThat(row.utilization().state())
                        .isEqualTo(SourceStatus.VALID));
        assertThat(response.saturation().resources().subList(0, 4))
                .allSatisfy(row -> assertThat(row.utilization().value()).isPositive());
        assertThat(response.saturation().inFlight().globalSum().value()).isPositive();
        assertThat(response.saturation().inFlight().instanceMax().value()).isPositive();
        assertThat(response.saturation().inFlight().activeInstances()).isEqualTo(4);

        var admission = response.saturation().queues().stream()
                .filter(queue -> queue.zone() == QueueZone.ADMISSION)
                .findFirst()
                .orElseThrow();
        assertThat(admission.metrics().get(0).value().state()).isEqualTo(SourceStatus.VALID);
        assertThat(admission.metrics().get(0).value().value()).isPositive();
    }

    @Test
    @DisplayName("loaded 시나리오는 시스템 실패 사유 3종의 발생률을 제공한다")
    void loadedScenarioProvidesFailureReasonSamples() {
        var topReasons = assembleLoaded().errors().topReasons();

        assertThat(topReasons.state()).isEqualTo(SourceStatus.VALID);
        assertThat(topReasons.value())
                .extracting(AdminMetricsResponse.TopReason::reasonCode)
                .containsExactly(
                        ReasonCode.TEMPORARILY_UNAVAILABLE,
                        ReasonCode.INTERNAL_ERROR,
                        ReasonCode.UNMAPPED);
        assertThat(topReasons.value())
                .extracting(AdminMetricsResponse.TopReason::rps)
                .allMatch(rps -> rps > 0);
    }

    @Test
    @DisplayName("idle 시나리오는 처리 중 요청·입장 대기·실패 사유를 0으로 제공한다")
    void idleScenarioDoesNotRetainLoadedValues() {
        AdminMetricsResponse response = assemble("idle");

        assertThat(response.saturation().inFlight().globalSum().value()).isZero();
        assertThat(response.saturation().queues().get(0).metrics().get(0).value().value()).isZero();
        assertThat(response.errors().topReasons().value())
                .extracting(AdminMetricsResponse.TopReason::rps)
                .containsOnly(0.0);
    }

    private static AdminMetricsResponse assembleLoaded() {
        return assemble("loaded");
    }

    private static AdminMetricsResponse assemble(String scenario) {
        return new PromMetricsAssembler(
                new MockPromQuery(scenario, System.nanoTime()),
                new TimeProvider(Clock.systemUTC()),
                Duration.ofSeconds(120),
                Duration.ofMillis(500))
                .assemble(new MetricsQuery(MetricsWindow.ONE_MINUTE, null, null));
    }
}
