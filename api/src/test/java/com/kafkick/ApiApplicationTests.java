package com.kafkick;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.service.idempotency.IdempotencyExecutionService;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.storage.db.MySqlContainerConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@Import(MySqlContainerConfig.class)
class ApiApplicationTests {

    private final ApplicationContext applicationContext;
    private final PlatformTransactionManager transactionManager;
    private final IdempotencyExecutionService idempotencyExecutionService;

    @Autowired
    ApiApplicationTests(
            ApplicationContext applicationContext,
            PlatformTransactionManager transactionManager,
            IdempotencyExecutionService idempotencyExecutionService
    ) {
        this.applicationContext = applicationContext;
        this.transactionManager = transactionManager;
        this.idempotencyExecutionService = idempotencyExecutionService;
    }

    @Test
    void contextLoads() {
        assertThat(applicationContext.getBean(TimeProvider.class)).isNotNull();
    }

    @Test
    void transactionalCoreServicesUseProxiesInProductionContext() {
        List<Class<?>> transactionalServiceTypes = applicationContext
                .getBeansWithAnnotation(Service.class)
                .values()
                .stream()
                .map(AopUtils::getTargetClass)
                .filter(type -> type.getPackageName().startsWith(
                        "com.kafkick.core"
                ))
                .filter(ApiApplicationTests::hasTransactionalBoundary)
                .distinct()
                .toList();

        assertThat(transactionalServiceTypes).isNotEmpty();
        for (Class<?> serviceType : transactionalServiceTypes) {
            Object bean = applicationContext.getBean(serviceType);
            assertThat(AopUtils.isAopProxy(bean))
                    .as("%s must be a transactional proxy", serviceType)
                    .isTrue();
        }
    }

    @Test
    void idempotencyPollingRejectsActiveOuterTransaction() {
        TransactionTemplate transaction = new TransactionTemplate(
                transactionManager
        );

        assertThatThrownBy(() -> transaction.executeWithoutResult(status ->
                idempotencyExecutionService.execute(
                        "550e8400-e29b-41d4-a716-446655440000",
                        () -> "request",
                        CouponUseErrorCode.INVALID_COUPON_USE_REQUEST,
                        claimedAt -> "completed",
                        responseBody -> responseBody
                )
        )).isInstanceOf(IllegalTransactionStateException.class);
    }

    private static boolean hasTransactionalBoundary(Class<?> type) {
        if (AnnotatedElementUtils.hasAnnotation(
                type,
                Transactional.class
        )) {
            return true;
        }
        return Arrays.stream(type.getDeclaredMethods())
                .filter(method -> !method.isSynthetic())
                .anyMatch(method -> AnnotatedElementUtils.hasAnnotation(
                        method,
                        Transactional.class
                ));
    }

}
