package com.kafkick.core.observation;

import java.util.Objects;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
public class SpringAfterCommitCampaignClosedEventPublisher
        implements CampaignClosedEventPublisher {

    private final ApplicationEventPublisher applicationEventPublisher;

    public SpringAfterCommitCampaignClosedEventPublisher(
            ApplicationEventPublisher applicationEventPublisher
    ) {
        this.applicationEventPublisher = Objects.requireNonNull(
                applicationEventPublisher
        );
    }

    @Override
    public void publishAfterCommit(CampaignClosedEvent event) {
        Objects.requireNonNull(event, "event");
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager
                .isSynchronizationActive()) {
            throw new IllegalStateException(
                    "캠페인 종료 이벤트는 활성 트랜잭션에서만 등록할 수 있습니다."
            );
        }
        TransactionSynchronizationManager.registerSynchronization(
                new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        applicationEventPublisher.publishEvent(event);
                    }
                }
        );
    }
}
