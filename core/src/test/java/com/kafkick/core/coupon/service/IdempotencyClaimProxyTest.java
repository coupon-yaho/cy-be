package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.SimpleTransactionStatus;

import com.kafkick.core.coupon.port.IdempotencyRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IdempotencyClaimProxyTest {

    @Test
    void appliesRequiresNewThroughActualSpringProxy() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(TestConfig.class)) {
            IdempotencyClaimService claimService = context.getBean(
                    IdempotencyClaimService.class
            );
            OuterOperation outer = context.getBean(OuterOperation.class);
            IdempotencyRepository repository = context.getBean(
                    IdempotencyRepository.class
            );
            RecordingTransactionManager transactionManager = context.getBean(
                    RecordingTransactionManager.class
            );

            assertThat(AopUtils.isAopProxy(claimService)).isTrue();
            assertThat(AopUtils.isAopProxy(outer)).isTrue();
            assertThatThrownBy(outer::execute)
                    .isInstanceOf(IllegalStateException.class);

            verify(repository).tryStart(any(), any(), any());
            assertThat(transactionManager.propagations()).containsExactly(
                    TransactionDefinition.PROPAGATION_REQUIRED,
                    TransactionDefinition.PROPAGATION_REQUIRES_NEW
            );
        }
    }

    @Configuration
    @EnableTransactionManagement
    static class TestConfig {

        @Bean
        RecordingTransactionManager transactionManager() {
            return new RecordingTransactionManager();
        }

        @Bean
        IdempotencyRepository idempotencyRepository() {
            IdempotencyRepository repository = mock(
                    IdempotencyRepository.class
            );
            when(repository.tryStart(any(), any(), any())).thenReturn(true);
            return repository;
        }

        @Bean
        IdempotencyClaimService idempotencyClaimService(
                IdempotencyRepository repository
        ) {
            return new IdempotencyClaimService(repository);
        }

        @Bean
        OuterOperation outerOperation(IdempotencyClaimService claimService) {
            return new OuterOperation(claimService);
        }
    }

    static class OuterOperation {

        private final IdempotencyClaimService claimService;

        OuterOperation(IdempotencyClaimService claimService) {
            this.claimService = claimService;
        }

        @Transactional
        public void execute() {
            claimService.tryStart(
                    "550e8400-e29b-41d4-a716-446655440000",
                    "hash",
                    Instant.parse("2026-08-20T05:00:00Z")
            );
            throw new IllegalStateException("outer rollback");
        }
    }

    static class RecordingTransactionManager
            implements PlatformTransactionManager {

        private final List<Integer> propagations = new ArrayList<>();

        @Override
        public TransactionStatus getTransaction(
                TransactionDefinition definition
        ) {
            propagations.add(definition.getPropagationBehavior());
            return new SimpleTransactionStatus();
        }

        @Override
        public void commit(TransactionStatus status) {
        }

        @Override
        public void rollback(TransactionStatus status) {
        }

        List<Integer> propagations() {
            return List.copyOf(propagations);
        }
    }
}
