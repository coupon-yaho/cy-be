package com.kafkick.core.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.SmartTransactionObject;

import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationFailure;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

class NotificationQueryServiceTransactionTest {

    private static final Instant AT = Instant.parse("2026-08-29T00:00:00Z");

    @Test
    void repositoryRollbackStillReturnsUnavailableSummary() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(TestConfig.class)) {
            NotificationQueryService service = context.getBean(NotificationQueryService.class);

            assertThat(AopUtils.isAopProxy(service)).isTrue();
            NotificationSummary summary = service.getSummary(null);

            assertThat(summary.totalRequests().state()).isEqualTo(SourceStatus.UNAVAILABLE);
            assertThat(summary.snapshotAt()).isNull();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement
    static class TestConfig {

        @Bean
        PlatformTransactionManager transactionManager() {
            return new RollbackAwareTransactionManager();
        }

        @Bean
        NotificationRepository notificationRepository() {
            return new FailingNotificationRepository();
        }

        @Bean
        CouponRoundRepository couponRoundRepository() {
            return mock(CouponRoundRepository.class);
        }

        @Bean
        TimeProvider timeProvider() {
            return new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC));
        }

        @Bean
        NotificationQueryService notificationQueryService(NotificationRepository notifications,
                CouponRoundRepository couponRounds, TimeProvider timeProvider) {
            return new NotificationQueryService(notifications, couponRounds, timeProvider);
        }
    }

    static final class FailingNotificationRepository implements NotificationRepository {

        @Override
        @Transactional(readOnly = true)
        public long countAll() {
            throw new IllegalStateException("db down");
        }

        @Override
        public Notification save(Notification notification) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Notification> findById(Long notificationId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long countByCouponId(Long couponId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long countByCouponIdAndStatusIn(Long couponId, List<NotificationStatus> statuses) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long countByStatusIn(List<NotificationStatus> statuses) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<NotificationFailure> findFailuresBeforeId(Long beforeId, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean saveIfStatus(Notification notification, NotificationStatus expectedStatus,
                int expectedAttemptCount) {
            throw new UnsupportedOperationException();
        }
    }

    static final class RollbackAwareTransactionManager
            extends AbstractPlatformTransactionManager {

        private final ThreadLocal<TestTransaction> current = new ThreadLocal<>();

        @Override
        protected Object doGetTransaction() {
            TestTransaction transaction = current.get();
            return transaction == null ? new TestTransaction() : transaction;
        }

        @Override
        protected boolean isExistingTransaction(Object transaction) {
            return ((TestTransaction) transaction).active;
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
            TestTransaction testTransaction = (TestTransaction) transaction;
            testTransaction.active = true;
            current.set(testTransaction);
        }

        @Override
        protected void doSetRollbackOnly(DefaultTransactionStatus status) {
            ((TestTransaction) status.getTransaction()).rollbackOnly = true;
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }

        @Override
        protected void doCleanupAfterCompletion(Object transaction) {
            ((TestTransaction) transaction).active = false;
            current.remove();
        }
    }

    static final class TestTransaction implements SmartTransactionObject {

        private boolean active;
        private boolean rollbackOnly;

        @Override
        public boolean isRollbackOnly() {
            return rollbackOnly;
        }

        @Override
        public void flush() {
        }
    }
}
