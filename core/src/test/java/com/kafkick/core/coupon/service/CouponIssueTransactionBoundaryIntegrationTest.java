package com.kafkick.core.coupon.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.query.CouponIssuePolicySnapshot;
import com.kafkick.core.coupon.service.idempotency.IdempotencyClaimService;
import com.kafkick.core.coupon.service.idempotency.IdempotencyExecutionService;
import com.kafkick.core.coupon.service.idempotency.IdempotencyPolicy;
import com.kafkick.core.coupon.service.idempotency.IdempotentOperationService;
import com.kafkick.core.coupon.service.result.CouponIssueExecutionResult;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CouponIssueTransactionBoundaryIntegrationTest {

    private static final String KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final Instant AT = Instant.parse("2026-08-24T05:00:00Z");

    @Test
    void crossesReadOnlyCallbackAndWriteBoundariesThroughSpringProxies() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(TestConfig.class)) {
            CouponOperationExecutionService service = context.getBean(
                    CouponOperationExecutionService.class
            );
            CouponIssuePreflightService preflight = context.getBean(
                    CouponIssuePreflightService.class
            );
            IdempotentOperationService operation = context.getBean(
                    IdempotentOperationService.class
            );
            BoundaryTrace trace = context.getBean(BoundaryTrace.class);

            assertThat(AopUtils.isAopProxy(service)).isFalse();
            assertThat(AopUtils.isAopProxy(preflight)).isTrue();
            assertThat(AopUtils.isAopProxy(operation)).isTrue();

            CouponIssueExecutionResult result = service.issueWithMetadata(
                    10L,
                    20L,
                    MembershipGrade.GOLD,
                    KEY,
                    () -> {
                        trace.add("callback");
                        assertThat(TransactionSynchronizationManager
                                .isActualTransactionActive()).isFalse();
                    }
            );

            assertThat(result.replayed()).isFalse();
            assertThat(result.result().issuanceId()).isEqualTo(100L);
            assertThat(trace.events()).containsExactly(
                    "preflight-idempotency",
                    "policy-snapshot",
                    "callback",
                    "authoritative-operation",
                    "idempotency-insert"
            );
            assertThat(TransactionSynchronizationManager
                    .isActualTransactionActive()).isFalse();
        }
    }

    @Configuration
    @EnableTransactionManagement
    static class TestConfig {

        @Bean
        TestTransactionManager transactionManager() {
            return new TestTransactionManager();
        }

        @Bean
        BoundaryTrace boundaryTrace() {
            return new BoundaryTrace();
        }

        @Bean
        TimeProvider timeProvider() {
            return new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC));
        }

        @Bean
        CouponIssuePreflightService couponIssuePreflightService(
                IdempotencyRepository repository,
                CouponIssuePolicyValidator policyValidator
        ) {
            return new CouponIssuePreflightService(
                    repository,
                    policyValidator
            );
        }

        @Bean
        CouponRoundRepository couponRoundRepository(BoundaryTrace trace) {
            CouponRoundRepository repository = mock(
                    CouponRoundRepository.class
            );
            when(repository.findIssuePolicySnapshot(10L, 20L))
                    .thenAnswer(invocation -> {
                        trace.add("policy-snapshot");
                        assertReadOnlyTransaction();
                        return Optional.of(new CouponIssuePolicySnapshot(
                                couponRound(),
                                false
                        ));
                    });
            return repository;
        }

        @Bean
        IssuanceRepository issuanceRepository() {
            return mock(IssuanceRepository.class);
        }

        @Bean
        CouponIssuePolicyValidator couponIssuePolicyValidator(
                CouponRoundRepository couponRoundRepository
        ) {
            return new CouponIssuePolicyValidator(couponRoundRepository);
        }

        @Bean
        IdempotencyRepository idempotencyRepository(BoundaryTrace trace) {
            IdempotencyRepository repository = mock(
                    IdempotencyRepository.class
            );
            when(repository.findByKey(any())).thenAnswer(invocation -> {
                trace.add("preflight-idempotency");
                assertReadOnlyTransaction();
                return java.util.Optional.empty();
            });
            when(repository.insertCompleted(
                    any(), any(), any(), any(), any(), any()
            )).thenAnswer(invocation -> {
                trace.add("idempotency-insert");
                assertWriteTransaction();
                return true;
            });
            return repository;
        }

        @Bean
        IdempotentOperationService idempotentOperationService(
                IdempotencyRepository repository
        ) {
            return new IdempotentOperationService(repository);
        }

        @Bean
        CouponIssueService couponIssueService(BoundaryTrace trace) {
            CouponIssueService service = mock(CouponIssueService.class);
            when(service.issue(any())).thenAnswer(invocation -> {
                trace.add("authoritative-operation");
                assertWriteTransaction();
                return issuance();
            });
            return service;
        }

        @Bean
        IdempotencyResultCodec<CouponIssueResult> issueCodec() {
            @SuppressWarnings("unchecked")
            IdempotencyResultCodec<CouponIssueResult> codec = mock(
                    IdempotencyResultCodec.class
            );
            when(codec.write(any())).thenReturn("stored-result");
            return codec;
        }

        @Bean
        CouponOperationExecutionService couponOperationExecutionService(
                IdempotentOperationService operationService,
                CouponIssueService couponIssueService,
                CouponIssuePreflightService preflightService,
                TimeProvider timeProvider,
                IdempotencyResultCodec<CouponIssueResult> issueCodec
        ) {
            return new CouponOperationExecutionService(
                    mock(IdempotencyExecutionService.class),
                    operationService,
                    couponIssueService,
                    preflightService,
                    timeProvider,
                    mock(CouponUseService.class),
                    mock(CouponCancelUseService.class),
                    mock(CouponCancelService.class),
                    issueCodec,
                    codec(),
                    codec(),
                    codec()
            );
        }

        private static <T> IdempotencyResultCodec<T> codec() {
            @SuppressWarnings("unchecked")
            IdempotencyResultCodec<T> codec = mock(
                    IdempotencyResultCodec.class
            );
            return codec;
        }
    }

    static final class BoundaryTrace {

        private final List<String> events = new ArrayList<>();

        void add(String event) {
            events.add(event);
        }

        List<String> events() {
            return List.copyOf(events);
        }
    }

    static final class TestTransactionManager
            extends AbstractPlatformTransactionManager {

        private final ThreadLocal<TestTransaction> current =
                new ThreadLocal<>();

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
        protected void doBegin(
                Object transaction,
                TransactionDefinition definition
        ) {
            TestTransaction testTransaction = (TestTransaction) transaction;
            testTransaction.active = true;
            current.set(testTransaction);
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

    static final class TestTransaction {

        private boolean active;
    }

    private static void assertReadOnlyTransaction() {
        assertThat(TransactionSynchronizationManager
                .isActualTransactionActive()).isTrue();
        assertThat(TransactionSynchronizationManager
                .isCurrentTransactionReadOnly()).isTrue();
    }

    private static void assertWriteTransaction() {
        assertThat(TransactionSynchronizationManager
                .isActualTransactionActive()).isTrue();
        assertThat(TransactionSynchronizationManager
                .isCurrentTransactionReadOnly()).isFalse();
    }

    private static CouponRound couponRound() {
        return CouponRound.restore(
                10L,
                1L,
                1L,
                "골드 VIP 5천원 할인",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                java.util.Set.of(
                        MembershipGrade.GOLD,
                        MembershipGrade.VIP
                ),
                Instant.parse("2026-08-24T04:00:00Z"),
                Instant.parse("2026-08-24T06:00:00Z"),
                CouponRoundStatus.OPEN,
                Instant.parse("2026-08-23T00:00:00Z")
        );
    }

    private static Issuance issuance() {
        return Issuance.restore(
                100L,
                10L,
                20L,
                "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD,
                IssuanceStatus.ISSUED,
                AT,
                AT.plusSeconds(604_800),
                AT
        );
    }
}
