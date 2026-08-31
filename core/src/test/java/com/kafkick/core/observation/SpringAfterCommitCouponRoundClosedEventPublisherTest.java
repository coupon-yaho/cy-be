package com.kafkick.core.observation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpringAfterCommitCouponRoundClosedEventPublisherTest {

    private static final CouponRoundClosedEvent EVENT = new CouponRoundClosedEvent(
            201L,
            Instant.parse("2026-08-26T05:04:00Z")
    );

    private final List<Object> publishedEvents = new ArrayList<>();
    private TransactionTemplate transactionTemplate;
    private SpringAfterCommitCouponRoundClosedEventPublisher publisher;

    @BeforeEach
    void setUp() {
        ApplicationEventPublisher applicationEventPublisher =
                publishedEvents::add;
        publisher = new SpringAfterCommitCouponRoundClosedEventPublisher(
                applicationEventPublisher
        );
        transactionTemplate = new TransactionTemplate(
                new TestTransactionManager()
        );
    }

    @Test
    @DisplayName("트랜잭션 커밋 전에는 종료 이벤트를 발행하지 않는다")
    void publishOnlyAfterCommit() {
        transactionTemplate.executeWithoutResult(status -> {
            publisher.publishAfterCommit(EVENT);
            assertThat(publishedEvents).isEmpty();
        });

        assertThat(publishedEvents).containsExactly(EVENT);
    }

    @Test
    @DisplayName("트랜잭션을 롤백하면 종료 이벤트를 발행하지 않는다")
    void doNotPublishAfterRollback() {
        transactionTemplate.executeWithoutResult(status -> {
            publisher.publishAfterCommit(EVENT);
            status.setRollbackOnly();
        });

        assertThat(publishedEvents).isEmpty();
    }

    @Test
    @DisplayName("활성 트랜잭션 밖의 등록을 거부한다")
    void rejectRegistrationWithoutActiveTransaction() {
        assertThatThrownBy(() -> publisher.publishAfterCommit(EVENT))
                .isInstanceOf(IllegalStateException.class);
    }

    static final class TestTransactionManager
            extends AbstractPlatformTransactionManager {

        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(
                Object transaction,
                TransactionDefinition definition
        ) {
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }
    }
}
