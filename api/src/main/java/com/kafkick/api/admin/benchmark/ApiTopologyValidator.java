package com.kafkick.api.admin.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.tomcat.autoconfigure.TomcatServerProperties;
import org.springframework.stereotype.Component;
import org.springframework.dao.DataAccessException;
import org.springframework.beans.factory.annotation.Value;

import com.zaxxer.hikari.HikariDataSource;

import com.kafkick.api.admin.benchmark.BatchTopologyPreflight.Violation;
import com.kafkick.core.benchmark.BenchmarkTopology;
import com.kafkick.core.benchmark.BenchmarkTopologyObservation;

/** API 인스턴스 값은 런타임 빈에서 읽고, 총량은 배포가 선언한 replica 수로 계산한다. */
@Component
public class ApiTopologyValidator {

    private final TomcatServerProperties tomcat;
    private final HikariDataSource operationalDataSource;
    private final DataSource observationDataSource;
    private final BenchmarkTopologyObservation databaseObservation;
    private final BatchTopologyPreflight batch;
    private final int expectedAppReplicas;
    private final int expectedTomcatWorkersTotal;
    private final int expectedHikariPoolTotal;
    private final int expectedMysqlMaxConnections;
    private final int expectedLoadHoldSeconds;
    private final int expectedObservationHoldSeconds;

    @Autowired
    public ApiTopologyValidator(
        TomcatServerProperties tomcat,
        ObjectProvider<HikariDataSource> operationalDataSource,
        @Qualifier("obs") ObjectProvider<DataSource> observationDataSource,
        ObjectProvider<BenchmarkTopologyObservation> databaseObservation,
        BatchTopologyPreflight batch,
        @Value("${benchmark.topology.app-replicas:4}") int expectedAppReplicas,
        @Value("${benchmark.topology.tomcat-workers-total:60}") int expectedTomcatWorkersTotal,
        @Value("${benchmark.topology.hikari-pool-total:52}") int expectedHikariPoolTotal,
        @Value("${benchmark.topology.mysql-max-connections:151}") int expectedMysqlMaxConnections,
        @Value("${benchmark.protocol.load-hold-seconds:5}") int expectedLoadHoldSeconds,
        @Value("${benchmark.protocol.observation-hold-seconds:60}") int expectedObservationHoldSeconds
    ) {
        this(tomcat, operationalDataSource.getIfAvailable(), observationDataSource.getIfAvailable(),
            databaseObservation.getIfAvailable(), batch, expectedAppReplicas, expectedTomcatWorkersTotal,
            expectedHikariPoolTotal, expectedMysqlMaxConnections, expectedLoadHoldSeconds,
            expectedObservationHoldSeconds);
    }

    ApiTopologyValidator(
        TomcatServerProperties tomcat,
        HikariDataSource operationalDataSource,
        DataSource observationDataSource,
        BenchmarkTopologyObservation databaseObservation,
        BatchTopologyPreflight batch,
        int expectedAppReplicas,
        int expectedTomcatWorkersTotal,
        int expectedHikariPoolTotal,
        int expectedMysqlMaxConnections
    ) {
        this(tomcat, operationalDataSource, observationDataSource, databaseObservation, batch,
            expectedAppReplicas, expectedTomcatWorkersTotal, expectedHikariPoolTotal,
            expectedMysqlMaxConnections, 5, 60);
    }

    ApiTopologyValidator(
        TomcatServerProperties tomcat,
        HikariDataSource operationalDataSource,
        DataSource observationDataSource,
        BenchmarkTopologyObservation databaseObservation,
        BatchTopologyPreflight batch,
        int expectedAppReplicas,
        int expectedTomcatWorkersTotal,
        int expectedHikariPoolTotal,
        int expectedMysqlMaxConnections,
        int expectedLoadHoldSeconds,
        int expectedObservationHoldSeconds
    ) {
        this.tomcat = tomcat;
        this.operationalDataSource = operationalDataSource;
        this.observationDataSource = observationDataSource;
        this.databaseObservation = databaseObservation;
        this.batch = batch;
        this.expectedAppReplicas = expectedAppReplicas;
        this.expectedTomcatWorkersTotal = expectedTomcatWorkersTotal;
        this.expectedHikariPoolTotal = expectedHikariPoolTotal;
        this.expectedMysqlMaxConnections = expectedMysqlMaxConnections;
        this.expectedLoadHoldSeconds = expectedLoadHoldSeconds;
        this.expectedObservationHoldSeconds = expectedObservationHoldSeconds;
    }

    public MeasuredTopology validate(
        long couponId,
        int appReplicas,
        int offeredRps,
        Integer stockTotal,
        Integer cpuMillicoresTotal,
        Integer memoryMbTotal
    ) {
        return validate(couponId, appReplicas, offeredRps, expectedLoadHoldSeconds,
            expectedObservationHoldSeconds, stockTotal, cpuMillicoresTotal, memoryMbTotal);
    }

    public MeasuredTopology validate(
        long couponId,
        int appReplicas,
        int offeredRps,
        int loadHoldSeconds,
        int observationHoldSeconds,
        Integer stockTotal,
        Integer cpuMillicoresTotal,
        Integer memoryMbTotal
    ) {
        List<Violation> violations = new ArrayList<>();
        int workersTotal = multiplyTotal(violations, "server.tomcat.threads.max.total",
            tomcat.getThreads().getMax(), expectedAppReplicas);
        int maxConnectionsTotal = multiplyTotal(violations,
            "server.tomcat.max-connections.total", tomcat.getMaxConnections(), expectedAppReplicas);
        int acceptCountTotal = multiplyTotal(violations, "server.tomcat.accept-count.total",
            tomcat.getAcceptCount(), expectedAppReplicas);
        HikariDataSource operational = operationalDataSource;
        DataSource observation = observationDataSource;
        BenchmarkTopologyObservation database = databaseObservation;
        int hikariPoolTotal = operational == null ? 0 : multiplyTotal(
            violations, "hikari.pool.total", operational.getMaximumPoolSize(), expectedAppReplicas);
        Integer mysqlMaxConnections = null;
        boolean mysqlQueryFailed = false;
        // 조회 결과의 **없음**은 여기서 Optional 로 들고 있다가, 응답 필드를 만들 때만
        // null 이 된다 — 그 필드는 nullable 이 정상이라 그대로 둔다(CY-909 갈래 ③).
        Optional<BenchmarkTopologyObservation.CouponStock> stock = Optional.empty();
        boolean stockQueryFailed = false;
        if (database != null) {
            try {
                mysqlMaxConnections = database.connectionLimit();
            } catch (DataAccessException failure) {
                mysqlQueryFailed = true;
                violations.add(new Violation("mysql.max-connections", "readable runtime value",
                    "unavailable", failure.getClass().getSimpleName()));
            }
            try {
                stock = database.couponStock(couponId);
            } catch (DataAccessException failure) {
                stockQueryFailed = true;
                violations.add(new Violation("coupon-stock", "readable stock row",
                    "unavailable", failure.getClass().getSimpleName()));
            }
        }
        Integer actualStockTotal = stock
                .map(BenchmarkTopologyObservation.CouponStock::totalQuantity).orElse(null);
        Integer activeStockCount = stock
                .map(BenchmarkTopologyObservation.CouponStock::activeCount).orElse(null);

        mismatch(violations, "api.replicas", expectedAppReplicas, appReplicas,
            "시작 요청의 replica 수가 배포 토폴로지와 다르다");
        mismatch(violations, "load.hold-seconds", expectedLoadHoldSeconds, loadHoldSeconds,
            "부하 유지 시간이 측정 프로토콜과 다르다");
        mismatch(violations, "observation.hold-seconds", expectedObservationHoldSeconds,
            observationHoldSeconds, "회복 관측 시간이 측정 프로토콜과 다르다");
        mismatch(violations, "server.tomcat.threads.max.total",
            expectedTomcatWorkersTotal, workersTotal,
            "Tomcat worker 총량이 AB-G3와 다르다");
        int connectionCapacity = addTotal(violations, "server.tomcat.connection-capacity.total",
            maxConnectionsTotal, acceptCountTotal);
        if (connectionCapacity < offeredRps) {
            violations.add(new Violation(
                "server.tomcat.connection-capacity.total",
                ">= offeredRps(" + offeredRps + ")",
                Integer.toString(connectionCapacity),
                "스파이크 연결이 worker에 닿기 전에 소켓 관문에서 잘릴 수 있다"));
        }
        mismatch(violations, "hikari.pool.total", expectedHikariPoolTotal, hikariPoolTotal,
            "API Hikari 풀 총량이 OBS-14b 목표와 다르다");
        if (!mysqlQueryFailed) {
            mismatch(violations, "mysql.max-connections", expectedMysqlMaxConnections,
                mysqlMaxConnections, "MySQL 연결 상한이 AB-G3와 다르다");
        }
        if (!stockQueryFailed && actualStockTotal == null) {
            violations.add(new Violation(
                "coupon-stock.total-quantity", "existing coupon", "missing",
                "시작 요청의 쿠폰 재고를 확인할 수 없다"));
        }
        if (stockTotal == null) {
            violations.add(new Violation(
                "coupon-stock.total-quantity", "reported stockTotal", "null",
                "회차 재고 입력이 없다"));
        } else if (actualStockTotal != null && !stockTotal.equals(actualStockTotal)) {
            violations.add(new Violation(
                "coupon-stock.total-quantity", stockTotal.toString(), String.valueOf(actualStockTotal),
                "시작 요청의 회차 재고가 실제 쿠폰 재고와 다르다"));
        }
        // **isPresent() 다.** Optional 로 바꾸면서 `stock != null` 을 그대로 두면 항상 참이
        // 되어, 재고 행이 없을 때도 active-count 위반이 하나 더 붙는다 — 같은 부재를 두 번
        // 보고하는 셈이다. 기존 테스트가 그 자리에서 잡았다.
        if (stock.isPresent()) {
            mismatch(violations, "coupon-stock.active-count", 0, activeStockCount,
                "리허설 발급이 남은 쿠폰은 새 측정 회차로 사용할 수 없다");
        }
        if (operational == null || observation == null) {
            violations.add(new Violation(
                "datasource.separation", "both actual pools",
                "operational=" + presence(operational) + ", observation=" + presence(observation),
                "API의 실제 두 풀을 모두 확인할 수 없다"));
        } else if (operational == observation) {
            violations.add(new Violation(
                "datasource.separation", "distinct instances", "same instance",
                "API 관측 쿼리가 운영 풀을 사용하면 v1의 Hikari 병목 측정을 흐린다"));
        }
        if (!violations.isEmpty()) {
            return new MeasuredTopology(null, violations);
        }
        BatchTopologyPreflight.Result batchResult = batch.validate(couponId);
        if (batchResult == null) {
            violations.add(new Violation(
                "batch.preflight", "response", "null", "batch 조건을 확인할 수 없다"));
        } else if (!batchResult.valid() && batchResult.violations().isEmpty()) {
            violations.add(new Violation(
                "batch.preflight", "valid response with details", "invalid without violations",
                "batch가 실패 이유 없이 검증 실패를 응답했다"));
        } else {
            violations.addAll(batchResult.violations());
        }

        if (!violations.isEmpty()) {
            return new MeasuredTopology(null, violations);
        }
        BenchmarkTopology topology = new BenchmarkTopology(
            expectedAppReplicas,
            Runtime.getRuntime().availableProcessors(),
            cpuMillicoresTotal,
            memoryMbTotal,
            workersTotal,
            maxConnectionsTotal,
            acceptCountTotal,
            hikariPoolTotal,
            mysqlMaxConnections != null ? mysqlMaxConnections : 1);
        return new MeasuredTopology(topology, violations);
    }

    private static String presence(Object value) {
        return value == null ? "null" : "present";
    }

    private static int multiplyTotal(
        List<Violation> violations, String key, int perInstance, int replicas
    ) {
        try {
            return Math.multiplyExact(perInstance, replicas);
        } catch (ArithmeticException overflow) {
            violations.add(new Violation(key, "32-bit integer total",
                perInstance + " x " + replicas, "토폴로지 총량 계산이 정수 범위를 넘었다"));
            return 0;
        }
    }

    private static int addTotal(List<Violation> violations, String key, int left, int right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            violations.add(new Violation(key, "32-bit integer total",
                left + " + " + right, "연결 수용량 계산이 정수 범위를 넘었다"));
            return 0;
        }
    }

    private static void mismatch(
        List<Violation> violations, String key, int expected, Integer actual, String reason
    ) {
        if (actual == null || actual != expected) {
            violations.add(new Violation(
                key, Integer.toString(expected), String.valueOf(actual), reason));
        }
    }

    public record MeasuredTopology(BenchmarkTopology topology, List<Violation> violations) {
        public MeasuredTopology {
            violations = List.copyOf(violations);
        }

        public boolean valid() {
            return violations.isEmpty();
        }
    }
}
