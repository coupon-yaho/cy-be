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
            CouponIssuePolicyValidator validator = context.getBean(
                    CouponIssuePolicyValidator.class
            );
            IdempotencyExecutionService idempotency = context.getBean(
                    IdempotencyExecutionService.class
            );
            IdempotentOperationService operation = context.getBean(
                    IdempotentOperationService.class
            );
            BoundaryTrace trace = context.getBean(BoundaryTrace.class);

            assertThat(AopUtils.isAopProxy(service)).isFalse();
            assertThat(AopUtils.isAopProxy(validator)).isTrue();
            assertThat(AopUtils.isAopProxy(idempotency)).isTrue();
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
                    "claim",
                    "policy-round",
                    "policy-existing",
                    "callback",
                    "authoritative-operation",
                    "idempotency-complete"
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
        IdempotencyClaimService idempotencyClaimService(
                BoundaryTrace trace
        ) {
            IdempotencyClaimService service = mock(
                    IdempotencyClaimService.class
            );
            when(service.tryStart(any(), any(), any())).thenAnswer(
                    invocation -> {
                        trace.add("claim");
                        assertWriteTransaction();
                        return true;
                    }
            );
            return service;
        }

        @Bean
        IdempotencyExecutionService idempotencyExecutionService(
                IdempotencyClaimService claimService,
                TimeProvider timeProvider
        ) {
            return new IdempotencyExecutionService(
                    claimService,
                    timeProvider,
                    new IdempotencyPolicy(
                            Duration.ofSeconds(1),
                            Duration.ofMillis(10),
                            Duration.ofSeconds(30)
                    )
            );
        }

        @Bean
        CouponRoundRepository couponRoundRepository(BoundaryTrace trace) {
            CouponRoundRepository repository = mock(
                    CouponRoundRepository.class
            );
            when(repository.findById(10L)).thenAnswer(invocation -> {
                trace.add("policy-round");
                assertReadOnlyTransaction();
                return Optional.of(couponRound());
            });
            return repository;
        }

        @Bean
        IssuanceRepository issuanceRepository(BoundaryTrace trace) {
            IssuanceRepository repository = mock(IssuanceRepository.class);
            when(repository.existsForCouponRoundAndMember(10L, 20L))
                    .thenAnswer(invocation -> {
                        trace.add("policy-existing");
                        assertReadOnlyTransaction();
                        return false;
                    });
            return repository;
        }

        @Bean
        CouponIssuePolicyValidator couponIssuePolicyValidator(
                CouponRoundRepository couponRoundRepository,
                IssuanceRepository issuanceRepository
        ) {
            return new CouponIssuePolicyValidator(
                    couponRoundRepository,
                    issuanceRepository
            );
        }

        @Bean
        IdempotencyRepository idempotencyRepository(BoundaryTrace trace) {
            IdempotencyRepository repository = mock(
                    IdempotencyRepository.class
            );
            org.mockito.Mockito.doAnswer(invocation -> {
                trace.add("idempotency-complete");
                assertWriteTransaction();
                return null;
            }).when(repository).complete(
                    any(), any(), any(), any(), any()
            );
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
                IdempotencyExecutionService idempotencyExecutionService,
                IdempotentOperationService operationService,
                CouponIssueService couponIssueService,
                CouponIssuePolicyValidator policyValidator,
                IdempotencyResultCodec<CouponIssueResult> issueCodec
        ) {
            return new CouponOperationExecutionService(
                    idempotencyExecutionService,
                    operationService,
                    couponIssueService,
                    policyValidator,
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
