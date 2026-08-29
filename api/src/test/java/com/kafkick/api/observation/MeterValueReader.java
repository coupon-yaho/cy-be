package com.kafkick.api.observation;

import java.util.OptionalDouble;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;

/**
 * 레지스트리에서 Gauge · Timer 값을 읽는 헬퍼.
 *
 * <p><b>테스트 소스셋에 있다 — 프로덕션 코드가 아니다.</b> Prometheus 가
 * {@code /actuator/prometheus} 를 직접 긁으면서 프로덕션 경로에서 빠졌다. main 에 두면
 * bootJar 에 실리고, 다음 사람이 "인스턴스 하나만 읽는다"는 제약을 운영 경로로 끌어들인다.
 * 다른 모듈에서 필요해지면 testFixtures 로 승격한다.
 *
 * <p>모든 반환이 {@link OptionalDouble} 인 이유는 <b>"값 없음"과 0 을 구분해야</b> 하기
 * 때문이다. expiry 를 10초로 줄인 탓에 무부하 구간의 p99 가 0 으로 읽히는데, 그대로 넘기면
 * 화면에 "지연 0ms" 가 그려진다 — "트래픽 없음" 보다 나쁜 거짓말이다.
 *
 * <p><b>이 클래스가 보장하지 않는 것</b>: 여러 인스턴스의 집계. 지금은 자기 JVM 의 레지스트리
 * 하나만 읽는다. 앱 N 대 집계는 이 클래스 뒤에서 바뀌어야 하며 후속 티켓(OBS-5 · P-1)이
 * 담당한다.
 *
 * <p>⚠️ <b>인스턴스별 백분위를 평균하거나 합산하지 말 것.</b> 통계적으로 무의미하다. 병합값은
 * 한 인스턴스만 느려진 상황을 희석해 숨기는데 그게 가장 흔한 장애 형태다. 최댓값을 쓰고 이름도
 * {@code maxInstanceP99} 처럼 실제 통계를 드러낼 것 — {@code globalP99} 라고 짓지 말 것.
 * Prometheus 를 써도 병합은 안 된다({@code quantile} 라벨이라 {@code _bucket} 이 없다).
 */
public final class MeterValueReader {

    private final MeterRegistry registry;

    public MeterValueReader(MeterRegistry registry) {
        this.registry = registry;
    }

    /** 미터가 없으면 빈 값. Gauge 값 자체가 NaN 이어도 빈 값이다(바인더가 아직 못 읽은 상태). */
    public OptionalDouble gauge(String name, String... tags) {
        Gauge gauge = registry.find(name).tags(Tags.of(tags)).gauge();
        if (gauge == null) {
            return OptionalDouble.empty();
        }
        double value = gauge.value();
        return Double.isNaN(value) ? OptionalDouble.empty() : OptionalDouble.of(value);
    }

    /**
     * Timer 의 백분위를 나노초로 읽는다. 단위는 실측 확인 — 스냅샷의 percentileValue 는
     * Timer 의 base unit(나노초)이다.
     *
     * <p>다음 경우 전부 빈 값이다. 호출자는 셋을 구분할 수 없고, 구분할 필요도 없다 —
     * 어느 쪽이든 "지금 이 창에 보여줄 지연 값이 없다"이다:
     * <ul>
     *   <li>Timer 가 아직 등록되지 않았다 (요청이 한 번도 없었다)
     *   <li>expiry 가 지나 현재 창이 비었다
     *   <li>해당 백분위가 설정돼 있지 않다 (observation.yml 의 percentiles 누락)
     * </ul>
     *
     * <p><b>빈 창 판별이 값 0 에 의존한다.</b> {@code count()} · {@code total()} 은 누적이라
     * 줄지 않고 {@code max()} 는 자체 창을 따로 굴린다 — 창이 비었다는 신호는 백분위가 0 으로
     * 내려앉는 것뿐이다(실측). 벽시계 지연이 정확히 0ns 인 요청은 없으므로 0 을 "표본 없음"
     * 으로 읽는다.
     *
     * <p>대가 — 나노초 미만으로 측정되는 초고속 Timer 에 쓰면 진짜 값도 없음으로 읽힌다.
     *
     * <p>같은 규칙이 프로덕션 경로의 {@code PromMetricsAssembler} 에도 있다. 저쪽은 Prometheus 를
     * 읽어 원천이 달라 코드를 공유할 수 없으니, 한쪽만 고치지 말 것 — 두 경로가 다른 값을 그린다.
     */
    public OptionalDouble percentileNanos(String name, double percentile, String... tags) {
        Timer timer = registry.find(name).tags(Tags.of(tags)).timer();
        if (timer == null) {
            return OptionalDouble.empty();
        }
        for (ValueAtPercentile value : timer.takeSnapshot().percentileValues()) {
            if (value.percentile() == percentile) {
                return value.value() == 0.0
                        ? OptionalDouble.empty()
                        : OptionalDouble.of(value.value());
            }
        }
        return OptionalDouble.empty();
    }

    /**
     * Timer 의 <b>누적</b> 호출 수. 창 단위가 아니라 expiry 가 지나도 줄지 않으므로, 초당
     * 처리량이 필요하면 호출자가 두 스냅샷의 차를 내야 한다. 미터가 없으면 빈 값이다.
     */
    public OptionalDouble timerCount(String name, String... tags) {
        Timer timer = registry.find(name).tags(Tags.of(tags)).timer();
        return timer == null ? OptionalDouble.empty() : OptionalDouble.of(timer.count());
    }

    /** 이름의 미터가 하나라도 등록돼 있는지. 자동 계측 등록 확인용이다. */
    public boolean exists(String name) {
        return !registry.find(name).meters().isEmpty();
    }
}
