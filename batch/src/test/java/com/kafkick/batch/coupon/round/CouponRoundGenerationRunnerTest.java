package com.kafkick.batch.coupon.round;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.service.result.CouponRoundGenerationResult;
import com.kafkick.core.coupon.service.CouponRoundGenerationService;
import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponRoundGenerationRunnerTest {

    @Mock
    private CouponRoundGenerationService generationService;

    @Test
    @DisplayName("서울 날짜를 기준으로 오늘을 포함한 30일 범위의 회차를 생성한다")
    void generateRollingScheduleHorizonInConfiguredZone() {
        Instant asOf = Instant.parse("2026-08-20T16:00:00Z");
        TimeProvider timeProvider = new TimeProvider(
                Clock.fixed(asOf, ZoneOffset.UTC)
        );
        CouponRoundGenerationProperties properties =
                new CouponRoundGenerationProperties(30, "Asia/Seoul");
        CouponRoundGenerationRunner runner = new CouponRoundGenerationRunner(
                generationService,
                timeProvider,
                properties
        );
        CouponRoundGenerationResult expected =
                new CouponRoundGenerationResult(2, 1, 0, 1);
        when(generationService.generate(
                LocalDate.of(2026, 8, 21),
                LocalDate.of(2026, 9, 19),
                asOf
        )).thenReturn(expected);

        CouponRoundGenerationResult result = runner.runOnce();

        assertThat(result).isEqualTo(expected);
        verify(generationService).generate(
                LocalDate.of(2026, 8, 21),
                LocalDate.of(2026, 9, 19),
                asOf
        );
    }
}
