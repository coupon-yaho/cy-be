package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.kafkick.core.admin.MetricsWindow;

/** series 전용 예산이 1초 폴링 예산과 독립인지, 그리고 창·step·평가점 계약을 지키는지 검증합니다. */
class PrometheusSeriesPropertiesTest {

    /** 기본값이 1초 폴링 예산과 실제로 다른 값이어야 분리한 의미가 있다. */
    @Test
    @DisplayName("series 기본 예산은 /metrics 의 1초 폴링 예산과 다른 값이다")
    void defaultsAreIndependentOfPollBudget() {
        PrometheusSeriesProperties series = PrometheusSeriesProperties.defaults();
        PrometheusQueryProperties query =
                new PrometheusQueryProperties(null, null, null, null, null);

        assertThat(series.totalBudget()).isGreaterThan(query.totalBudget());
        assertThat(series.connectTimeout()).isGreaterThan(query.connectTimeout());
        assertThat(series.readTimeout()).isGreaterThan(query.readTimeout());
    }

    /** 예산은 "새 질의를 시작하지 않는 시점" 이라 최악은 budget + connect + read 다. */
    @Test
    @DisplayName("최악의 응답 시간이 series 폴링 간격을 넘으면 기동에서 죽는다")
    void rejectsWorstCaseBeyondPollInterval() {
        assertThatThrownBy(() -> new PrometheusSeriesProperties(
                Duration.ofSeconds(1), Duration.ofSeconds(3), Duration.ofSeconds(2), null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("폴링 간격");
    }

    /**
     * 계약이 두 곳에 걸린다 — 조회 구간은 {@link MetricsWindow} 가, 평가점 상한은 이 설정이 정한다.
     * 각각을 따로 보는 테스트 두 개로는 둘 사이가 어긋난 것을 못 잡는다.
     */
    @ParameterizedTest
    @EnumSource(MetricsWindow.class)
    @DisplayName("모든 MetricsWindow 가 step 으로 나누어지고 평가점 상한 안에 든다")
    void everyWindowFitsStepAndMaxPoints(MetricsWindow window) {
        PrometheusSeriesProperties properties = PrometheusSeriesProperties.defaults();
        long stepSeconds = properties.step().toSeconds();

        assertThat(window.duration().toSeconds() % stepSeconds).isZero();
        assertThat(window.duration().toSeconds() / stepSeconds + 1L)
                .isLessThanOrEqualTo(properties.maxPoints());
        assertThat(properties.maxRange()).isGreaterThanOrEqualTo(window.duration());
    }

    /** 상한을 낮추면 가장 긴 창이 먼저 걸려야 한다 — 조용히 통과하면 15분에서만 터진다. */
    @Test
    @DisplayName("가장 긴 창의 평가점이 max-points 를 넘으면 기동에서 죽는다")
    void rejectsMaxPointsThatCannotHoldLongestWindow() {
        assertThatThrownBy(() -> new PrometheusSeriesProperties(
                null, null, null, Duration.ofSeconds(5), 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-points");
    }

    /** step 이 창을 나누지 못하면 마지막 버킷이 잘려 값이 이웃과 다른 폭을 갖는다. */
    @Test
    @DisplayName("step 이 창을 나누지 못하면 기동에서 죽는다")
    void rejectsStepThatDoesNotDivideWindows() {
        assertThatThrownBy(() -> new PrometheusSeriesProperties(
                null, null, null, Duration.ofSeconds(7), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("나누어야");
    }

    /** PromQL 구간 표기에 정수 초로 실으므로 소수 초는 표현할 수 없다. */
    @Test
    @DisplayName("소수 초 step 은 기동에서 죽는다")
    void rejectsFractionalStep() {
        assertThatThrownBy(() -> new PrometheusSeriesProperties(
                null, null, null, Duration.ofMillis(1_500), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("정수 초");
    }
}
