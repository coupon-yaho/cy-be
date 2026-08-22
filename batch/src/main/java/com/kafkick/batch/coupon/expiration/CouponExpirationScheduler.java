package com.kafkick.batch.coupon.expiration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class CouponExpirationScheduler {

    private static final Logger log = LoggerFactory.getLogger(
            CouponExpirationScheduler.class
    );

    private final CouponExpirationRunner runner;

    public CouponExpirationScheduler(CouponExpirationRunner runner) {
        this.runner = runner;
    }

    @Scheduled(
            cron = "${coupon.expiration.cron}",
            zone = "${coupon.expiration.zone}"
    )
    public void expireCoupons() {
        CouponExpirationBatchResult result = runner.runOnce();
        log.info(
                "coupon expiration completed: asOf={}, scanned={}, expired={}",
                result.asOf(),
                result.scannedCount(),
                result.expiredCount()
        );
    }
}
