package com.kafkick.infra.redis.coupon.v2;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import com.kafkick.core.coupon.v2.port.RestoreOutcome;
import com.kafkick.core.coupon.v2.port.V2RestorationMeters;

/**
 * 복원 결과 카운터. <b>회차 태그를 붙이지 않는다</b> — 회차는 계속 늘어나므로 시계열이
 * 무한히 갈라진다. 어느 회차인지는 경보를 받은 뒤 로그에서 찾는다.
 *
 * <p>결과별 카운터를 <b>기동 시점에 전부 등록</b>한다. 처음 발생할 때 만들면 값이 0 인
 * 시계열이 아예 없어서 "한 번도 안 났다" 와 "계측이 안 붙었다" 를 구별할 수 없다.
 */
public class MicrometerV2RestorationMeters implements V2RestorationMeters {

    static final String NAME = "coupon_v2_stock_restore_total";
    static final String FAILURE_OUTCOME = "CALL_FAILED";
    static final String HALT_WRITE_FAILURE_OUTCOME = "HALT_WRITE_FAILED";

    private final Map<RestoreOutcome, Counter> byOutcome = new EnumMap<>(RestoreOutcome.class);
    private final Counter callFailure;
    private final Counter haltWriteFailure;

    public MicrometerV2RestorationMeters(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry, "meterRegistry");
        for (RestoreOutcome outcome : RestoreOutcome.values()) {
            byOutcome.put(outcome, counter(meterRegistry, outcome.name()));
        }
        callFailure = counter(meterRegistry, FAILURE_OUTCOME);
        haltWriteFailure = counter(meterRegistry, HALT_WRITE_FAILURE_OUTCOME);
    }

    private static Counter counter(MeterRegistry meterRegistry, String outcome) {
        return Counter.builder(NAME)
                .description("v2 재고 복원 호출의 결과별 건수")
                .tag("outcome", outcome)
                .register(meterRegistry);
    }

    @Override
    public void recordOutcome(RestoreOutcome outcome) {
        byOutcome.get(outcome).increment();
    }

    @Override
    public void recordCallFailure() {
        callFailure.increment();
    }

    @Override
    public void recordHaltWriteFailure() {
        haltWriteFailure.increment();
    }
}
