package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 관제 질의 설정의 기본값과 세 값 사이의 관계를 검증합니다. */
class PrometheusQueryPropertiesTest {

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(1);

    /**
     * 예산은 "새 질의를 시작하지 않는 시점" 이라 예산 종료 직전에 시작한 질의가 자기 타임아웃만큼
     * 더 돕니다. 세 값을 따로 만지다 보면 이 관계가 조용히 깨집니다.
     */
    @Test
    @DisplayName("기본값의 최악 응답 시간이 폴링 간격 안에 든다")
    void defaultsFitInsideThePollInterval() {
        PrometheusQueryProperties defaults =
                new PrometheusQueryProperties(null, null, null, null, null);

        Duration worstCase = defaults.totalBudget()
                .plus(defaults.connectTimeout())
                .plus(defaults.readTimeout());

        assertThat(worstCase)
                .as("넘으면 다음 폴링이 앞 요청을 따라잡아 관제가 스스로 부하가 된다")
                .isLessThanOrEqualTo(POLL_INTERVAL);
    }

    /** 조용히 넘기지 않고 기동에서 죽인다. 부하 구간에서만 드러나면 그때는 이미 늦다. */
    @Test
    @DisplayName("최악 응답 시간이 폴링 간격을 넘는 설정은 기동에서 거부한다")
    void rejectsConfigurationThatOverrunsThePollInterval() {
        assertThatThrownBy(() -> new PrometheusQueryProperties(
                null, Duration.ofMillis(200), Duration.ofMillis(500), null, Duration.ofMillis(900)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("폴링 간격");
    }

    /** 경계에서 부호가 바뀌어도 알아채야 합니다. */
    @Test
    @DisplayName("합이 폴링 간격과 정확히 같으면 허용한다")
    void allowsConfigurationExactlyAtThePollInterval() {
        assertThatCode(() -> new PrometheusQueryProperties(
                null, Duration.ofMillis(100), Duration.ofMillis(300), null, Duration.ofMillis(600)))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> new PrometheusQueryProperties(
                null, Duration.ofMillis(100), Duration.ofMillis(300), null, Duration.ofMillis(601)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("base-url 의 끝 슬래시를 떼고 빈 값이면 기본값을 쓴다")
    void normalizesBaseUrl() {
        assertThat(new PrometheusQueryProperties("http://prom:9090/", null, null, null, null).baseUrl())
                .isEqualTo("http://prom:9090");
        assertThat(new PrometheusQueryProperties("  ", null, null, null, null).baseUrl())
                .isEqualTo("http://prometheus:9090");
    }
}
