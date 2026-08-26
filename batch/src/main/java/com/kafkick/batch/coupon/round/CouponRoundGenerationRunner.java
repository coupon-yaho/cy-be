package com.kafkick.batch.coupon.round;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.service.CouponRoundGenerationService;
import com.kafkick.core.coupon.service.result.CouponRoundGenerationResult;
import com.kafkick.core.support.TimeProvider;

@Component
public class CouponRoundGenerationRunner {

    private final CouponRoundGenerationService generationService;
    private final TimeProvider timeProvider;
    private final CouponRoundGenerationProperties properties;

    public CouponRoundGenerationRunner(
            CouponRoundGenerationService generationService,
            TimeProvider timeProvider,
            CouponRoundGenerationProperties properties
    ) {
        this.generationService = generationService;
        this.timeProvider = timeProvider;
        this.properties = properties;
    }

    public CouponRoundGenerationResult runOnce() {
        Instant asOf = timeProvider.instant().truncatedTo(ChronoUnit.MICROS);
        LocalDate fromDate = LocalDate.ofInstant(
                asOf,
                properties.scheduleZoneId()
        );
        LocalDate toDate = fromDate.plusDays(properties.horizonDays() - 1L);
        return generationService.generate(fromDate, toDate, asOf);
    }
}
