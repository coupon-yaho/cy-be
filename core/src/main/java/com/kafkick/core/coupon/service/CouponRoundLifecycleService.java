package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.port.CouponRoundLifecyclePort;
import com.kafkick.core.coupon.port.CouponRoundScheduleLockPort;
import com.kafkick.core.coupon.service.result.CouponRoundLifecycleResult;

@Service
public class CouponRoundLifecycleService {

    private final CouponRoundLifecyclePort lifecyclePort;
    private final CouponRoundScheduleLockPort scheduleLockPort;

    public CouponRoundLifecycleService(
            CouponRoundLifecyclePort lifecyclePort,
            CouponRoundScheduleLockPort scheduleLockPort
    ) {
        this.lifecyclePort = Objects.requireNonNull(lifecyclePort);
        this.scheduleLockPort = Objects.requireNonNull(scheduleLockPort);
    }

    @Transactional
    public CouponRoundLifecycleResult synchronize(Instant asOf) {
        if (asOf == null) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 상태 전환 기준 시각은 필수입니다."
            );
        }
        scheduleLockPort.lock();
        List<Long> closedOpenRoundIds = lifecyclePort.closeOpenRounds(asOf);
        int closedMissedScheduledCount =
                lifecyclePort.closeMissedScheduledRounds(asOf);
        int openedCount = lifecyclePort.openScheduledRounds(asOf);
        return new CouponRoundLifecycleResult(
                closedOpenRoundIds.size(),
                closedMissedScheduledCount,
                openedCount
        );
    }
}
