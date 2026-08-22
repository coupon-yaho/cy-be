package com.kafkick.batch.coupon.round;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.service.result.CouponRoundGenerationResult;

@Component
@ConditionalOnProperty(
        prefix = "batch.scheduling",
        name = "enabled",
        havingValue = "true"
)
public class CouponRoundGenerationScheduler {

    private static final Logger log = LoggerFactory.getLogger(
            CouponRoundGenerationScheduler.class
    );

    private final CouponRoundGenerationRunner runner;

    public CouponRoundGenerationScheduler(
            CouponRoundGenerationRunner runner
    ) {
        this.runner = runner;
    }

    @Scheduled(
            cron = "${batch.schedule.coupon-create-cron}",
            zone = "${coupon.round-generation.schedule-zone}"
    )
    public void generateRounds() {
        CouponRoundGenerationResult result = runner.runOnce();
        log.info(
                "coupon round generation completed: targets={}, created={}, duplicate={}, conflict={}",
                result.creationTargets(),
                result.createdCount(),
                result.duplicateCount(),
                result.conflictCount()
        );
    }
}
