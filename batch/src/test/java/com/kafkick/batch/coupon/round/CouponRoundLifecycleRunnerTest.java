package com.kafkick.batch.coupon.round;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.service.CouponRoundLifecycleService;
import com.kafkick.core.coupon.service.result.CouponRoundLifecycleResult;
import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponRoundLifecycleRunnerTest {

    @Mock
    private CouponRoundLifecycleService lifecycleService;

    @Test
    @DisplayName("한 번 고정한 마이크로초 기준 시각으로 회차 상태를 동기화한다")
    void synchronizeWithOneTruncatedAsOf() {
        Instant clockInstant =
                Instant.parse("2026-08-25T03:00:00.123456789Z");
        Instant expectedAsOf =
                Instant.parse("2026-08-25T03:00:00.123456Z");
        CouponRoundLifecycleRunner runner = new CouponRoundLifecycleRunner(
                lifecycleService,
                new TimeProvider(Clock.fixed(clockInstant, ZoneOffset.UTC))
        );
        CouponRoundLifecycleResult expected =
                new CouponRoundLifecycleResult(1, 0, 1);
        when(lifecycleService.synchronize(expectedAsOf)).thenReturn(expected);

        CouponRoundLifecycleResult result = runner.runOnce();

        assertThat(result).isEqualTo(expected);
        verify(lifecycleService).synchronize(expectedAsOf);
    }
}
