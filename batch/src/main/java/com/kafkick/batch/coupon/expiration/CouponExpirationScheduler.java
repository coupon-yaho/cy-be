package com.kafkick.batch.coupon.expiration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        prefix = "batch.scheduling",
        name = "enabled",
        havingValue = "true"
)
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
        if (!result.failedRoundIds().isEmpty()) {
            log.error("coupon expiration skipped rounds after failures: {}",
                    result.failedRoundIds());
        }
        if (!result.haltedRoundIds().isEmpty()) {
            // 회차를 중단했다는 사실은 info 줄에 묻히면 안 된다 — 재동기화 요청이다.
            log.error(
                    "coupon expiration halted rounds (v2 stock restore over cap): {}",
                    result.haltedRoundIds()
            );
        }
    }
}
