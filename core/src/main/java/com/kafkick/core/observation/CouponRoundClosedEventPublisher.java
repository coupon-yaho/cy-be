package com.kafkick.core.observation;

public interface CouponRoundClosedEventPublisher {

    void publishAfterCommit(CouponRoundClosedEvent event);
}
