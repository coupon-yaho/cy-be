package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.notification.MockNotificationPayloadFactory;

class MockNotificationPayloadFactoryTest {
    @Test
    void createsDeterministicNonPiiPayload() {
        Instant at = Instant.parse("2026-08-29T00:00:00Z");
        Issuance issuance = Issuance.restore(100L, 10L, 20L, "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD, IssuanceStatus.ISSUED, at, at.plusSeconds(86_400), at);

        var payload = new MockNotificationPayloadFactory().create(issuance);

        assertThat(payload.recipientContact()).isEqualTo("member:20");
        assertThat(payload.messageBody()).isEqualTo("coupon-issued:100");
        assertThat(payload.toString()).doesNotContain(
                "ABCDEFGHJKLM2345", "member:20", "coupon-issued:100");
    }
}
