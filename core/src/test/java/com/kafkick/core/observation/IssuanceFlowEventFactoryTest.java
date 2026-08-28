package com.kafkick.core.observation;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class IssuanceFlowEventFactoryTest {

    @Test
    void generatesANewEventIdForEachEvent() {
        UUID firstId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        UUID secondId = UUID.fromString("22222222-2222-2222-2222-222222222222");
        Iterator<UUID> eventIds = List.of(firstId, secondId).iterator();
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(eventIds::next);
        IssuanceFlowEvent.Ctx context = IssuanceFlowEventTest.context("request-1", false);

        IssuanceFlowEvent first = factory.entry(
                context, 200, null, Dependency.NONE, null, null
        );
        IssuanceFlowEvent second = factory.issueRejected(
                context, 409, ReasonCode.STOCK_EXHAUSTED, Dependency.NONE
        );

        assertThat(first.eventId()).isEqualTo(firstId);
        assertThat(second.eventId()).isEqualTo(secondId);
        assertThat(first.requestId()).isEqualTo(second.requestId());
    }
}
