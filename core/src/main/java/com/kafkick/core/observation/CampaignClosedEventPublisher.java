package com.kafkick.core.observation;

public interface CampaignClosedEventPublisher {

    void publishAfterCommit(CampaignClosedEvent event);
}
