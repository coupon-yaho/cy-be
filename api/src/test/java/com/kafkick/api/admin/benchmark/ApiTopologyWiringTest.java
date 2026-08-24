package com.kafkick.api.admin.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.tomcat.autoconfigure.TomcatServerProperties;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.util.ReflectionTestUtils;

import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.core.benchmark.BenchmarkRunService;

@SpringBootTest(
    classes = com.kafkick.ApiApplication.class,
    properties = "management.endpoint.health.validate-group-membership=false")
@Import(MySqlContainerConfig.class)
class ApiTopologyWiringTest {

    @Autowired
    ApiTopologyValidator validator;

    @Autowired
    BenchmarkRunService runService;

    @Autowired
    BenchmarkStartOrchestrator startOrchestrator;

    @Autowired
    BenchmarkFinalizeOrchestrator finalizeOrchestrator;

    @Autowired
    TomcatServerProperties tomcat;

    @Autowired
    HikariDataSource operationalDataSource;

    @Autowired
    @Qualifier("obs")
    JdbcTemplate observationJdbcTemplate;

    @MockitoBean
    BatchTopologyPreflight batch;

    @Test
    @DisplayName("실제 API 컨텍스트에서 @Qualifier(obs)가 운영 풀과 다른 빈을 잡는다")
    void actualObservationQualifierIsResolved() {
        given(batch.validate(10L)).willReturn(new BatchTopologyPreflight.Result(true, List.of()));

        assertThat(validator.validate(10L, 1, 20_000, null, null, null).violations())
            .extracting("key")
            .doesNotContain("datasource.separation");
        assertThat(ReflectionTestUtils.getField(validator, "jdbcTemplate"))
            .isSameAs(observationJdbcTemplate);
        assertThat(runService).isNotNull();
        assertThat(startOrchestrator).isNotNull();
        assertThat(finalizeOrchestrator).isNotNull();
    }

    @Test
    void repositoryDefaultsMatchFourReplicaProtocolTotals() {
        assertThat(tomcat.getThreads().getMax()).isEqualTo(15);
        assertThat(tomcat.getMaxConnections()).isEqualTo(4_000);
        assertThat(tomcat.getAcceptCount()).isEqualTo(1_000);
        assertThat(operationalDataSource.getMaximumPoolSize()).isEqualTo(3);
    }

    @Test
    void benchmarkRunServiceDoesNotDependOnConditionalEvaluationOrder() throws Exception {
        assertThat(BenchmarkRunConfiguration.class
            .getDeclaredMethod("benchmarkRunService", com.kafkick.core.benchmark.BenchmarkRunRepository.class,
                com.kafkick.core.support.TimeProvider.class)
            .getAnnotation(ConditionalOnBean.class)).isNull();
        assertThat(BenchmarkRunConfiguration.class.getAnnotation(ConditionalOnProperty.class))
            .isNotNull();
    }
}
