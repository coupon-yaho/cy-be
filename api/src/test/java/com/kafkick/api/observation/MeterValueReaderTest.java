package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * "값 없음" 과 0 을 구분하는 규칙만 본다. 나머지는 Micrometer 가 하는 일이다.
 *
 * <p>이 구분이 이 클래스의 존재 이유다 — expiry 를 10초로 줄인 탓에 무부하 구간의 백분위가
 * 0 으로 읽히고, 그걸 그대로 화면에 넘기면 "트래픽 없음" 이 "지연 0ms" 로 둔갑한다.
 */
class MeterValueReaderTest {



    private final MockClock clock = new MockClock();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
    private final MeterValueReader reader = new MeterValueReader(registry);

    @Test
    @DisplayName("없는 미터는 0 이 아니라 빈 값이다")
    void missingMetersAreEmptyNotZero() {
        assertThat(reader.gauge("nope")).isEmpty();
        assertThat(reader.percentileNanos("nope", 0.99)).isEmpty();
        assertThat(reader.timerCount("nope")).isEmpty();
        assertThat(reader.exists("nope")).isFalse();
    }

    @Test
    @DisplayName("NaN 게이지는 빈 값이다 — 바인더가 아직 읽지 못한 상태를 0 으로 그리지 않는다")
    void nanGaugeIsEmpty() {
        AtomicReference<Double> source = new AtomicReference<>(Double.NaN);
        Gauge.builder("probe.gauge", source, AtomicReference::get).register(registry);

        assertThat(reader.gauge("probe.gauge")).isEmpty();

        source.set(3.0);
        assertThat(reader.gauge("probe.gauge")).hasValue(3.0);
    }

    @Test
    @DisplayName("설정되지 않은 백분위를 물으면 빈 값이다 — 0.999 를 물어도 0 이 나오지 않는다")
    void unconfiguredPercentileIsEmpty() {
        Timer timer = Timer.builder("probe.timer").publishPercentiles(0.99).register(registry);
        timer.record(Duration.ofMillis(10));

        assertThat(reader.percentileNanos("probe.timer", 0.99)).isPresent();
        assertThat(reader.percentileNanos("probe.timer", 0.999)).isEmpty();
    }

    @Test
    @DisplayName("expiry 가 지나 창이 비면 빈 값으로 바뀐다 — count 는 그대로인데도")
    void expiredWindowBecomesEmptyEvenThoughCountDoesNot() {
        Timer timer = Timer.builder("probe.timer")
                .publishPercentiles(0.99)
                .distributionStatisticExpiry(Duration.ofSeconds(10))
                .register(registry);
        timer.record(Duration.ofMillis(500));

        assertThat(reader.percentileNanos("probe.timer", 0.99)).isPresent();

        clock.add(Duration.ofSeconds(11));

        assertThat(reader.percentileNanos("probe.timer", 0.99))
                .as("창이 비었다")
                .isEmpty();
        assertThat(reader.timerCount("probe.timer"))
                .as("호출 수는 누적이라 줄지 않는다 — 이걸로는 빈 창을 판별할 수 없다")
                .hasValue(1.0);
    }
}
