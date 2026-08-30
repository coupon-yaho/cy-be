package com.kafkick.core.notification;

import com.kafkick.core.coupon.domain.Issuance;

@FunctionalInterface
public interface NotificationPayloadFactory {
    NotificationPayload create(Issuance issuance);
}
