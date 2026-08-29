package com.kafkick;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.test.util.AopTestUtils;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.api.admin.benchmark.BenchmarkFinalizeOrchestrator;
import com.kafkick.api.admin.benchmark.BenchmarkRunConfiguration;
import com.kafkick.api.admin.benchmark.BenchmarkStartOrchestrator;
import com.kafkick.api.admin.observability.AdminObservabilityConfig;
import com.kafkick.api.admin.observability.PromQueryClient;
import com.kafkick.core.benchmark.BenchmarkRunRepository;
import com.kafkick.core.benchmark.BenchmarkRunService;
import com.kafkick.core.benchmark.RunTimeseriesArchiver;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.ArchiveStore;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.service.idempotency.IdempotencyExecutionService;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.benchmark.JdbcRunTimeseriesArchiveStore;

@SpringBootTest(properties = "observation.datasource.enabled=true")
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
    void contextLoads(@Autowired RunTimeseriesArchiver archiver, @Autowired ArchiveStore store,
                      @Autowired JdbcTemplate main, @Qualifier("obs") JdbcTemplate observation,
                      @Qualifier("promQueryClient") PromQueryClient pollingClient,
                      @Qualifier("archivePromQueryClient") PromQueryClient archiveClient) {
        assertThat(applicationContext.getBean(TimeProvider.class)).isNotNull();
        assertThat(archiver).isNotNull();
        assertThat(store).isNotNull();
        assertThat(main).isNotSameAs(observation);
        assertThat(pollingClient).isNotSameAs(archiveClient);
        assertThat(store).isInstanceOf(JdbcRunTimeseriesArchiveStore.class);
        Object target = AopTestUtils.getUltimateTargetObject(store);
        assertThat(new DirectFieldAccessor(target).getPropertyValue("writeJdbcTemplate"))
                .isSameAs(main).isNotSameAs(observation);
    }

    @Test
    void archiveBeanDoesNotDependOnConditionalEvaluationOrder() throws Exception {
        assertThat(AdminObservabilityConfig.class.getDeclaredMethod(
                "runTimeseriesArchiver", BenchmarkRunRepository.class,
                PromQueryClient.class, ArchiveStore.class, java.time.Duration.class,
                int.class, int.class)
                .getAnnotation(ConditionalOnBean.class)).isNull();
        assertThat(BenchmarkRunConfiguration.class.getDeclaredMethod(
                "benchmarkRunService", BenchmarkRunRepository.class,
                TimeProvider.class)).isNotNull();
        assertThat(BenchmarkRunConfiguration.class.getAnnotation(ConditionalOnProperty.class))
                .extracting(ConditionalOnProperty::havingValue).isEqualTo("true");
        assertThat(List.of(
                BenchmarkStartOrchestrator.class,
                BenchmarkFinalizeOrchestrator.class,
                com.kafkick.storage.db.benchmark.JdbcBenchmarkRunRepository.class,
                com.kafkick.storage.db.benchmark.JdbcRunTimeseriesArchiveStore.class,
                com.kafkick.storage.db.config.ObservationDataSourceConfig.class,
                com.kafkick.storage.db.config.ObservationHealthConfig.class))
                .allSatisfy(type -> assertThat(type.getAnnotation(ConditionalOnProperty.class))
                        .extracting(ConditionalOnProperty::havingValue).isEqualTo("true"));
    }

    @Test
    void numericOneDoesNotEnableBenchmarkOrArchiveBeans() {
        new ApplicationContextRunner()
                .withUserConfiguration(BenchmarkRunConfiguration.class, AdminObservabilityConfig.class)
                .withBean(TimeProvider.class,
                        () -> org.mockito.Mockito.mock(TimeProvider.class))
                .withBean(com.kafkick.core.runtimeconfig.RuntimeConfigStore.class,
                        () -> org.mockito.Mockito.mock(
                                com.kafkick.core.runtimeconfig.RuntimeConfigStore.class))
                .withBean(com.kafkick.core.admin.couponmetrics.CouponMetricsCalculator.class)
                .withBean(com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.class)
                .withBean(com.kafkick.core.admin.overview.calculator.IssuanceActionCalculator.class)
                .withBean(com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator.class)
                .withBean(com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.class)
                .withBean(com.kafkick.core.admin.overview.calculator.StockRiskCalculator.class)
                .withBean(com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator.class)
                .withBean(com.kafkick.core.admin.overview.calculator.CampaignPreparationCalculator.class)
                .withBean(com.kafkick.core.admin.overview.calculator.OperationActionCalculator.class)
                .withBean(com.kafkick.core.admin.overview.calculator.ConsistencyActionCalculator.class)
                .withBean(com.kafkick.core.admin.overview.calculator.OverviewStatusCalculator.class)
                .withPropertyValues("observation.datasource.enabled=1")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(BenchmarkRunService.class);
                    assertThat(context).doesNotHaveBean(RunTimeseriesArchiver.class);
                });
    }

    @Test
    void transactionalCoreServicesUseProxiesInProductionContext() {
        List<Class<?>> transactionalServiceTypes = applicationContext
                .getBeansWithAnnotation(Service.class)
                .values()
                .stream()
                .map(AopUtils::getTargetClass)
                .filter(type -> type.getPackageName().startsWith("com.kafkick.core"))
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
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);

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
        if (AnnotatedElementUtils.hasAnnotation(type, Transactional.class)) {
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
