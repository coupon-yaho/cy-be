package com.kafkick.api.observation;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import com.kafkick.core.observation.CouponRoundLifecycleRecorder;
import com.kafkick.core.observation.ClosedCouponRound;
import com.kafkick.core.observation.ClosedCouponRoundRecoverySource;
import com.kafkick.core.support.TimeProvider;

public final class CouponRoundLifecycleStartupRecovery
        implements ApplicationRunner {

    static final Duration LOOKBACK = Duration.ofDays(1);
    static final int LIMIT = 1_000;

    private static final Logger log = LoggerFactory.getLogger(
            CouponRoundLifecycleStartupRecovery.class
    );

    private static final Comparator<ClosedCouponRound> OLDEST_FIRST =
            Comparator.comparing(ClosedCouponRound::closedAt)
                    .thenComparingLong(ClosedCouponRound::couponId);

    private final ClosedCouponRoundRecoverySource source;
    private final CouponRoundLifecycleRecorder recorder;
    private final TimeProvider timeProvider;

    public CouponRoundLifecycleStartupRecovery(
            ClosedCouponRoundRecoverySource source,
            CouponRoundLifecycleRecorder recorder,
            TimeProvider timeProvider
    ) {
        this.source = Objects.requireNonNull(source);
        this.recorder = Objects.requireNonNull(recorder);
        this.timeProvider = Objects.requireNonNull(timeProvider);
    }

    @Override
    public void run(ApplicationArguments arguments) {
        Instant now = timeProvider.instant();
        try {
            List<ClosedCouponRound> couponRounds = source.findRecentlyClosed(
                    now.minus(LOOKBACK),
                    now,
                    LIMIT
            );
            couponRounds.stream()
                    .sorted(OLDEST_FIRST)
                    .forEach(couponRound -> recorder.retireCouponRound(
                            couponRound.couponId(),
                            couponRound.closedAt()
                    ));
            log.info(
                    "최근 종료 쿠폰 회차의 미터 회수를 요청했습니다. count={}",
                    couponRounds.size()
            );
            if (couponRounds.size() == LIMIT) {
                log.warn(
                        "최근 종료 쿠폰 회차 조회가 상한에 도달했습니다. "
                                + "일부 쿠폰 회차가 미터 회수 대상에서 제외되었을 수 있습니다. "
                                + "count={}, limit={}",
                        couponRounds.size(),
                        LIMIT
                );
            }
        } catch (Exception exception) {
            log.warn(
                    "쿠폰 회차 종료 기동 보정에 실패했습니다. API 기동은 계속합니다. cause={}",
                    exception.toString()
            );
        }
    }
}
