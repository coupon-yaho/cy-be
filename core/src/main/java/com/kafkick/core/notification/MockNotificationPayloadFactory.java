package com.kafkick.core.notification;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.domain.Issuance;

@Component
public class MockNotificationPayloadFactory implements NotificationPayloadFactory {
    @Override
    public NotificationPayload create(Issuance issuance) {
        return new NotificationPayload(
                "member:" + issuance.memberId(),
                "coupon-issued:" + issuance.id());
    }
}
