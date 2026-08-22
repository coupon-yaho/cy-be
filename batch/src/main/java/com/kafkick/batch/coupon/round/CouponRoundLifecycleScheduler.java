package com.kafkick.batch.coupon.round;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.service.result.CouponRoundLifecycleResult;

@Component
@ConditionalOnProperty(
        prefix = "batch.scheduling",
        name = "enabled",
        havingValue = "true"
)
public class CouponRoundLifecycleScheduler {

    private static final Logger log = LoggerFactory.getLogger(
            CouponRoundLifecycleScheduler.class
    );

    private final CouponRoundLifecycleRunner runner;

    public CouponRoundLifecycleScheduler(CouponRoundLifecycleRunner runner) {
        this.runner = runner;
    }

    @Scheduled(
            cron = "${batch.schedule.coupon-open-cron}",
            zone = "${coupon.round-generation.schedule-zone}"
    )
    public void synchronizeRounds() {
        CouponRoundLifecycleResult result = runner.runOnce();
        log.info(
                "coupon round lifecycle completed: closedOpen={}, closedMissed={}, opened={}",
                result.closedOpenCount(),
                result.closedMissedScheduledCount(),
                result.openedCount()
        );
    }
}
