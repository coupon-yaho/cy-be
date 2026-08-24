package com.kafkick;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.core.benchmark.RunTimeseriesArchiver;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.ArchiveStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.test.util.AopTestUtils;
import com.kafkick.storage.db.benchmark.JdbcRunTimeseriesArchiveStore;
import com.kafkick.api.admin.observability.PromQueryClient;
import static org.assertj.core.api.Assertions.assertThat;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import com.kafkick.api.admin.observability.AdminObservabilityConfig;
import com.kafkick.api.admin.benchmark.BenchmarkRunConfiguration;
import com.kafkick.api.admin.benchmark.BenchmarkStartOrchestrator;
import com.kafkick.api.admin.benchmark.BenchmarkFinalizeOrchestrator;
import com.kafkick.core.benchmark.BenchmarkRunRepository;
import com.kafkick.core.benchmark.BenchmarkRunService;

@SpringBootTest
@Import(MySqlContainerConfig.class)
class ApiApplicationTests {

    @Test
    void contextLoads(@Autowired RunTimeseriesArchiver archiver, @Autowired ArchiveStore store,
                      @Autowired JdbcTemplate main, @Qualifier("obs") JdbcTemplate observation,
                      @Qualifier("promQueryClient") PromQueryClient pollingClient,
                      @Qualifier("archivePromQueryClient") PromQueryClient archiveClient) {
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
            PromQueryClient.class, ArchiveStore.class, java.time.Duration.class, int.class)
            .getAnnotation(ConditionalOnBean.class)).isNull();
        assertThat(BenchmarkRunConfiguration.class.getDeclaredMethod(
            "benchmarkRunService", BenchmarkRunRepository.class,
            com.kafkick.core.support.TimeProvider.class)).isNotNull();
        assertThat(BenchmarkRunConfiguration.class.getAnnotation(ConditionalOnProperty.class))
            .extracting(ConditionalOnProperty::havingValue).isEqualTo("true");
        assertThat(java.util.List.of(
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
            .withBean(com.kafkick.core.support.TimeProvider.class,
                () -> org.mockito.Mockito.mock(com.kafkick.core.support.TimeProvider.class))
            .withBean(com.kafkick.core.admin.overview.mock.AdminOverviewMockDataFactory.class,
                () -> org.mockito.Mockito.mock(
                    com.kafkick.core.admin.overview.mock.AdminOverviewMockDataFactory.class))
            .withBean(com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.class)
            .withBean(com.kafkick.core.admin.overview.calculator.IssuanceActionCalculator.class)
            .withBean(com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator.class)
            .withBean(com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.class)
            .withBean(com.kafkick.core.admin.overview.calculator.StockRiskCalculator.class)
            .withBean(com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator.class)
            .withBean(com.kafkick.core.admin.overview.calculator.ConsistencyActionCalculator.class)
            .withBean(com.kafkick.core.admin.overview.calculator.OperationActionCalculator.class)
            .withBean(com.kafkick.core.admin.overview.calculator.OverviewStatusCalculator.class)
            .withPropertyValues("observation.datasource.enabled=1")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).doesNotHaveBean(BenchmarkRunService.class);
                assertThat(context).doesNotHaveBean(RunTimeseriesArchiver.class);
            });
    }

}
