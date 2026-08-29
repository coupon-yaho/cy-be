package com.kafkick.api.admin.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.boot.tomcat.autoconfigure.TomcatServerProperties;
import com.kafkick.core.benchmark.BenchmarkTopologyObservation;
import com.kafkick.core.benchmark.BenchmarkTopologyObservation.CouponStock;
import java.util.Optional;
import org.springframework.dao.DataAccessResourceFailureException;

import com.zaxxer.hikari.HikariDataSource;

import com.kafkick.api.admin.benchmark.ApiTopologyValidator.MeasuredTopology;
import com.kafkick.api.admin.benchmark.BatchTopologyPreflight.Violation;

class ApiTopologyValidatorTest {

    private final HikariDataSource operational = new HikariDataSource();
    private final DataSource observation = mock(DataSource.class);
    private final BenchmarkTopologyObservation databaseObservation = mock(BenchmarkTopologyObservation.class);
    private final BatchTopologyPreflight batch = mock(BatchTopologyPreflight.class);

    @AfterEach
    void closePool() {
        operational.close();
    }

    @Test
    @DisplayName("API 실제 Tomcat·Hikari·풀과 batch 로컬 결과로 토폴로지를 만든다")
    void measuresOwnedValues() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(databaseObservation.couponStock(10L))
            .willReturn(Optional.of(new CouponStock(10_000, 0)));
        given(batch.validate(10L)).willReturn(new BatchTopologyPreflight.Result(true, List.of()));

        MeasuredTopology measured = validator(tomcat(15, 4_000, 1_000), 4)
            .validate(10L, 4, 20_000, 10_000, null, null);

        assertThat(measured.valid()).isTrue();
        assertThat(measured.topology().tomcatWorkersTotal()).isEqualTo(60);
        assertThat(measured.topology().tomcatMaxConnections()).isEqualTo(16_000);
        assertThat(measured.topology().tomcatAcceptCount()).isEqualTo(4_000);
        assertThat(measured.topology().hikariPoolTotal()).isEqualTo(52);
        assertThat(measured.topology().mysqlMaxConnections()).isEqualTo(151);
        verify(databaseObservation).couponStock(10L);
    }

    @Test
    @DisplayName("실제 Tomcat 수용량과 Hikari 총량이 목표에서 벗어나면 호출자 입력 없이 실패한다")
    void rejectsActualSocketAndPoolMismatch() {
        operational.setMaximumPoolSize(10);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(batch.validate(10L)).willReturn(new BatchTopologyPreflight.Result(true, List.of()));

        MeasuredTopology measured = validator(tomcat(60, 1, 1), 1)
            .validate(10L, 1, 20_000, null, null, null);

        assertThat(measured.valid()).isFalse();
        assertThat(measured.violations())
            .extracting("key", "actual")
            .contains(
                org.assertj.core.groups.Tuple.tuple("server.tomcat.connection-capacity.total", "2"),
                org.assertj.core.groups.Tuple.tuple("hikari.pool.total", "10")
            );
    }

    @Test
    @DisplayName("API 로컬 위반이 있으면 batch preflight를 호출하지 않는다")
    void localViolationShortCircuitsBatchPreflight() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        ApiTopologyValidator validator = new ApiTopologyValidator(
            tomcat(60, 19_900, 100), operational, operational, databaseObservation, batch, 1, 60, 52, 151);
        MeasuredTopology measured = validator.validate(10L, 1, 20_000, null, null, null);

        assertThat(measured.violations()).extracting("key")
            .contains("datasource.separation")
            .doesNotContain("batch.scheduling.enabled");
        verifyNoInteractions(batch);
    }

    @Test
    @DisplayName("요청 재고가 실제 쿠폰 재고와 다르면 회차를 거부한다")
    void rejectsReportedStockMismatch() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(databaseObservation.couponStock(10L))
            .willReturn(Optional.of(new CouponStock(10_000, 0)));
        given(batch.validate(10L)).willReturn(new BatchTopologyPreflight.Result(true, List.of()));

        MeasuredTopology measured = validator(tomcat(60, 19_900, 100), 1)
            .validate(10L, 1, 20_000, 20_000, null, null);

        assertThat(measured.violations()).extracting("key", "expected", "actual")
            .contains(org.assertj.core.groups.Tuple.tuple(
                "coupon-stock.total-quantity", "20000", "10000"));
    }

    @Test
    @DisplayName("쿠폰이 없으면 같은 행 부재를 active-count 위반으로 중복하지 않는다")
    void missingCouponProducesOneRowMissingViolation() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(databaseObservation.couponStock(404L))
            .willReturn(Optional.empty());
        given(batch.validate(404L)).willReturn(new BatchTopologyPreflight.Result(true, List.of()));

        MeasuredTopology measured = validator(tomcat(60, 19_900, 100), 1)
            .validate(404L, 1, 20_000, 10_000, null, null);

        assertThat(measured.violations()).extracting("key", "actual")
            .containsExactly(org.assertj.core.groups.Tuple.tuple(
                "coupon-stock.total-quantity", "missing"));
    }

    @Test
    @DisplayName("batch가 invalid라고 응답하면 위반 목록이 비어도 게이트를 닫는다")
    void rejectsInvalidBatchResultWithoutViolationDetails() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(databaseObservation.couponStock(10L))
            .willReturn(Optional.of(new CouponStock(10_000, 0)));
        given(batch.validate(10L)).willReturn(new BatchTopologyPreflight.Result(false, List.of()));

        MeasuredTopology measured = validator(tomcat(60, 19_900, 100), 1)
            .validate(10L, 1, 20_000, 10_000, null, null);

        assertThat(measured.violations()).extracting("key")
            .contains("batch.preflight");
    }

    @Test
    void nullBatchResultBecomesAnActionableViolation() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(databaseObservation.couponStock(10L))
            .willReturn(Optional.of(new CouponStock(10_000, 0)));
        given(batch.validate(10L)).willReturn(null);

        MeasuredTopology measured = validator(tomcat(60, 19_900, 100), 1)
            .validate(10L, 1, 20_000, 10_000, null, null);

        assertThat(measured.violations()).extracting("key", "actual")
            .containsExactly(org.assertj.core.groups.Tuple.tuple("batch.preflight", "null"));
    }

    @Test
    @DisplayName("리허설 발급이 남은 쿠폰은 새 회차로 열지 않는다")
    void rejectsCouponWithActiveIssuances() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(databaseObservation.couponStock(10L))
            .willReturn(Optional.of(new CouponStock(10_000, 3_000)));
        given(batch.validate(10L)).willReturn(new BatchTopologyPreflight.Result(true, List.of()));

        MeasuredTopology measured = validator(tomcat(60, 19_900, 100), 1)
            .validate(10L, 1, 20_000, 10_000, null, null);

        assertThat(measured.violations()).extracting("key", "expected", "actual")
            .contains(org.assertj.core.groups.Tuple.tuple(
                "coupon-stock.active-count", "0", "3000"));
    }

    @Test
    void rejectsCallerReplicaCountDifferentFromDeployment() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(batch.validate(10L)).willReturn(new BatchTopologyPreflight.Result(true, List.of()));

        MeasuredTopology measured = new ApiTopologyValidator(
            tomcat(15, 4_000, 1_000), operational, observation, databaseObservation, batch,
            4, 60, 52, 151).validate(10L, 3, 20_000, null, null, null);

        assertThat(measured.violations()).extracting("key", "expected", "actual")
            .contains(org.assertj.core.groups.Tuple.tuple("api.replicas", "4", "3"));
        assertThat(measured.topology()).isNull();
    }

    @Test
    void missingPoolsReturnAllViolationsWithoutConstructingInvalidTopology() {
        given(batch.validate(10L)).willReturn(new BatchTopologyPreflight.Result(true, List.of()));
        ApiTopologyValidator validator = new ApiTopologyValidator(
            tomcat(15, 4_000, 1_000), (HikariDataSource) null, (DataSource) null,
            (BenchmarkTopologyObservation) null, batch, 4, 60, 52, 151);

        MeasuredTopology measured = validator.validate(10L, 4, 20_000, 10_000, null, null);

        assertThat(measured.topology()).isNull();
        assertThat(measured.violations()).extracting("key")
            .contains("hikari.pool.total", "mysql.max-connections", "datasource.separation");
    }

    @Test
    void mysqlRuntimeQueryFailureClosesTheGateWithAViolation() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit())
            .willThrow(new DataAccessResourceFailureException("db unavailable"));

        MeasuredTopology measured = validator(tomcat(60, 19_900, 100), 1)
            .validate(10L, 1, 20_000, 10_000, null, null);

        assertThat(measured.topology()).isNull();
        assertThat(measured.violations()).extracting("key", "actual")
            .contains(org.assertj.core.groups.Tuple.tuple(
                "mysql.max-connections", "unavailable"));
        verifyNoInteractions(batch);
    }

    @Test
    void stockQueryFailureClosesTheGateWithAViolation() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(databaseObservation.couponStock(10L))
            .willThrow(new DataAccessResourceFailureException("db unavailable"));

        MeasuredTopology measured = validator(tomcat(60, 19_900, 100), 1)
            .validate(10L, 1, 20_000, 10_000, null, null);

        assertThat(measured.topology()).isNull();
        assertThat(measured.violations()).extracting("key", "actual")
            .contains(org.assertj.core.groups.Tuple.tuple("coupon-stock", "unavailable"));
        verifyNoInteractions(batch);
    }

    @Test
    void protocolWindowsComeFromDeploymentConfiguration() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(databaseObservation.couponStock(10L))
            .willReturn(Optional.of(new CouponStock(10_000, 0)));
        given(batch.validate(10L)).willReturn(new BatchTopologyPreflight.Result(true, List.of()));
        ApiTopologyValidator configured = new ApiTopologyValidator(
            tomcat(15, 4_000, 1_000), operational, observation, databaseObservation, batch,
            4, 60, 52, 151, 10, 90);

        MeasuredTopology measured = configured.validate(
            10L, 4, 20_000, 5, 60, 10_000, null, null);

        assertThat(measured.violations()).extracting("key", "expected", "actual")
            .contains(
                org.assertj.core.groups.Tuple.tuple("load.hold-seconds", "10", "5"),
                org.assertj.core.groups.Tuple.tuple("observation.hold-seconds", "90", "60"));
    }

    @Test
    void overflowingDeploymentTotalsBecomeViolationsInsteadOfServerErrors() {
        operational.setMaximumPoolSize(Integer.MAX_VALUE);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(batch.validate(10L)).willReturn(new BatchTopologyPreflight.Result(true, List.of()));
        ApiTopologyValidator configured = new ApiTopologyValidator(
            tomcat(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE), operational,
            observation, databaseObservation, batch, 2, 60, 52, 151, 5, 60);

        MeasuredTopology measured = configured.validate(10L, 2, 20_000, 5, 60, null, null, null);

        assertThat(measured.violations()).extracting("key")
            .contains(
                "server.tomcat.threads.max.total",
                "server.tomcat.accept-count.total",
                "hikari.pool.total");
    }

    @Test
    void overflowingConnectionCapacityBecomesAViolation() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        ApiTopologyValidator configured = new ApiTopologyValidator(
            tomcat(60, 1_500_000_000, 1_500_000_000), operational,
            observation, databaseObservation, batch, 1, 60, 52, 151);

        MeasuredTopology measured = configured.validate(10L, 1, 20_000, null, null, null);

        assertThat(measured.violations())
            .filteredOn(violation -> violation.key().equals(
                "server.tomcat.connection-capacity.total"))
            .extracting("reason")
            .contains("연결 수용량 계산이 정수 범위를 넘었다");
    }

    @Test
    void existingCouponStillRejectsMissingReportedStock() {
        operational.setMaximumPoolSize(13);
        given(databaseObservation.connectionLimit()).willReturn(151);
        given(databaseObservation.couponStock(10L))
            .willReturn(Optional.of(new CouponStock(10_000, 0)));

        MeasuredTopology measured = validator(tomcat(60, 19_900, 100), 1)
            .validate(10L, 1, 20_000, null, null, null);

        assertThat(measured.violations()).extracting("key", "expected", "actual")
            .contains(org.assertj.core.groups.Tuple.tuple(
                "coupon-stock.total-quantity", "reported stockTotal", "null"));
    }

    private ApiTopologyValidator validator(TomcatServerProperties tomcat, int expectedAppReplicas) {
        return new ApiTopologyValidator(
            tomcat, operational, observation, databaseObservation, batch,
            expectedAppReplicas, 60, expectedAppReplicas * 13, 151);
    }

    private static TomcatServerProperties tomcat(int workers, int maxConnections, int acceptCount) {
        TomcatServerProperties properties = new TomcatServerProperties();
        properties.getThreads().setMax(workers);
        properties.setMaxConnections(maxConnections);
        properties.setAcceptCount(acceptCount);
        return properties;
    }
}
