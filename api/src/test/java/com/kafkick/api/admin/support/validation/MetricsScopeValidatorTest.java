package com.kafkick.api.admin.support.validation;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.core.admin.MetricsWindow;

/** 관측 지표의 GLOBAL·COUPON·BENCHMARK_RUN 범위 선택 규칙을 단위 검증합니다. */
class MetricsScopeValidatorTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    /** 쿠폰과 Benchmark 실행 식별자를 동시에 지정한 상호 배타 위반을 감지합니다. */
    @Test
    void rejectsCouponAndBenchmarkScopesTogether() {
        MetricsQuery query = new MetricsQuery(MetricsWindow.ONE_MINUTE, 1L, 2L);

        assertThat(validator.validate(query))
                .anyMatch(violation -> violation.getMessage()
                        .equals("couponId와 benchmarkRunId는 함께 지정할 수 없습니다."));
    }

    /** 두 식별자를 모두 생략한 GLOBAL 범위는 유효한 요청으로 허용합니다. */
    @Test
    void acceptsGlobalScope() {
        assertThat(validator.validate(new MetricsQuery(MetricsWindow.FIVE_MINUTES, null, null))).isEmpty();
    }
}
