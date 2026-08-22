package com.kafkick.api.observation.resource;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import javax.sql.DataSource;

import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
class ResourceProviderTest {

    private static final Clock CLOCK = Clock.fixed(
            Instant.parse("2026-08-22T00:00:00Z"), ZoneOffset.UTC);

    @Test
    @DisplayName("enum 여섯 자리와 디스크 PENDING · 네트워크 N_A 계약을 함께 고정한다")
    void preservesAllSixSlotsAndDistinguishesTheTwoUnmeasuredSources() {
        ResourceSnapshot snapshot = new ResourceProvider(
                unsupportedDataSource(), new SimpleMeterRegistry(), timeProvider()).snapshot();

        assertThat(snapshot.metrics()).containsOnlyKeys(ResourceMetric.values());
        // 원천이 없으면 0이 아니라 UNAVAILABLE이다. 0을 실으면 "정상인데 0"과 구분되지 않는다.
        assertThat(snapshot.dbPool().active().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.tomcatThreads().used().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.disk().used().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(snapshot.disk().max().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(snapshot.disk().utilization().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(snapshot.network().used().state()).isEqualTo(SourceStatus.N_A);
        assertThat(snapshot.network().max().state()).isEqualTo(SourceStatus.N_A);
        assertThat(snapshot.network().utilization().state()).isEqualTo(SourceStatus.N_A);
    }

    @Test
    @DisplayName("실제 JVM의 availableProcessors를 스냅샷 실행 조건에 싣는다")
    void recordsTheRuntimeProcessorCount() {
        ResourceSnapshot snapshot = new ResourceProvider(
                unsupportedDataSource(), new SimpleMeterRegistry(), timeProvider()).snapshot();

        assertThat(snapshot.availableProcessors())
                .isEqualTo(Runtime.getRuntime().availableProcessors());
    }

    @Test
    @DisplayName("DB active · total · awaiting을 따로 읽고 사용률만 active/total로 계산한다")
    void keepsAwaitingAsAnIndependentCompanionValue() {
        ResourceSnapshot snapshot = new ResourceProvider(
                new StubHikariDataSource(new StubPool(12, 12, 19)),
                new SimpleMeterRegistry(), timeProvider()).snapshot();

        assertThat(snapshot.dbPool().active().value()).isEqualTo(12.0);
        assertThat(snapshot.dbPool().total().value()).isEqualTo(12.0);
        assertThat(snapshot.dbPool().awaiting().value()).isEqualTo(19.0);
        assertThat(snapshot.dbPool().utilization().value()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("awaiting 조회만 실패해도 DB 사용률은 유지한다")
    void anAwaitingFailureDoesNotEraseDbPoolUtilization() {
        ResourceSnapshot snapshot = new ResourceProvider(
                new StubHikariDataSource(new StubPool(6, 12, null)),
                new SimpleMeterRegistry(), timeProvider()).snapshot();

        assertThat(snapshot.dbPool().active().state()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.dbPool().total().state()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.dbPool().awaiting().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.dbPool().utilization().value()).isEqualTo(0.5);
    }

    @Test
    @DisplayName("DB total이 0이면 0으로 나누지 않고 사용률을 PENDING으로 둔다")
    void zeroDbPoolDenominatorIsPending() {
        ResourceSnapshot snapshot = new ResourceProvider(
                new StubHikariDataSource(new StubPool(0, 0, 0)),
                new SimpleMeterRegistry(), timeProvider()).snapshot();

        assertThat(snapshot.dbPool().utilization().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(snapshot.dbPool().utilization().value()).isNull();
        assertThat(snapshot.dbPool().utilization().observedAt()).isNull();
    }

    @Test
    @DisplayName("힙 max가 없으면 used는 VALID로 남기고 max와 사용률만 N_A로 낸다")
    void heapWithoutMaxKeepsUsedAndMarksOnlyMaxNotApplicable() {
        ResourceSnapshot snapshot = new ResourceProvider(
                unsupportedDataSource(), new SimpleMeterRegistry(), timeProvider(),
                heapMxBean(512L * 1024 * 1024, -1L), processCpuLoad(0.25)).snapshot();

        // 바이트 → MiB 변환이 Provider 한 곳에서만 일어나는지 함께 고정한다.
        assertThat(snapshot.heapMemory().used().state()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.heapMemory().used().value()).isEqualTo(512.0);
        assertThat(snapshot.heapMemory().max().state()).isEqualTo(SourceStatus.N_A);
        assertThat(snapshot.heapMemory().max().value()).isNull();
        assertThat(snapshot.heapMemory().utilization().state()).isEqualTo(SourceStatus.N_A);
        assertThat(snapshot.heapMemory().utilization().value()).isNull();
    }

    @Test
    @DisplayName("힙 max가 있으면 used·max를 MiB로 통일하고 사용률을 계산한다")
    void heapWithMaxConvertsBothToMebibytes() {
        ResourceSnapshot snapshot = new ResourceProvider(
                unsupportedDataSource(), new SimpleMeterRegistry(), timeProvider(),
                heapMxBean(512L * 1024 * 1024, 2048L * 1024 * 1024), processCpuLoad(0.25))
                .snapshot();

        assertThat(snapshot.heapMemory().used().value()).isEqualTo(512.0);
        assertThat(snapshot.heapMemory().max().value()).isEqualTo(2048.0);
        assertThat(snapshot.heapMemory().utilization().value()).isEqualTo(0.25);
    }

    @Test
    @DisplayName("CPU 값이 아직 없으면 0이 아니라 UNAVAILABLE로 낸다")
    void cpuLoadNotYetAvailableIsUnavailableRatherThanZero() {
        // getProcessCpuLoad()는 표본이 모이기 전 음수를 돌려준다. 0으로 실으면 "정상인데 0"과
        // 구분되지 않는다.
        ResourceSnapshot snapshot = new ResourceProvider(
                unsupportedDataSource(), new SimpleMeterRegistry(), timeProvider(),
                heapMxBean(64L * 1024 * 1024, 128L * 1024 * 1024), processCpuLoad(-1.0))
                .snapshot();

        assertThat(snapshot.processCpu().used().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.processCpu().used().value()).isNull();
        assertThat(snapshot.processCpu().max().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.processCpu().utilization().state())
                .isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    @DisplayName("CPU 값이 있으면 max 1.0 대비 비율로 낸다")
    void cpuLoadIsReportedAgainstAFullCore() {
        ResourceSnapshot snapshot = new ResourceProvider(
                unsupportedDataSource(), new SimpleMeterRegistry(), timeProvider(),
                heapMxBean(64L * 1024 * 1024, 128L * 1024 * 1024), processCpuLoad(0.75))
                .snapshot();

        assertThat(snapshot.processCpu().used().value()).isEqualTo(0.75);
        assertThat(snapshot.processCpu().max().value()).isEqualTo(1.0);
        assertThat(snapshot.processCpu().utilization().value()).isEqualTo(0.75);
    }

    @Test
    @DisplayName("힙 원천이 예외를 던져도 나머지 원천은 스냅샷에 남는다")
    void aHeapFailureDoesNotDiscardTheOtherSources() {
        ResourceSnapshot snapshot = new ResourceProvider(
                new StubHikariDataSource(new StubPool(6, 12, 3)),
                new SimpleMeterRegistry(), timeProvider(),
                throwingMemoryMxBean(), processCpuLoad(0.25)).snapshot();

        assertThat(snapshot.heapMemory().used().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.heapMemory().max().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.heapMemory().utilization().state())
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.dbPool().active().value()).isEqualTo(6.0);
        assertThat(snapshot.dbPool().awaiting().value()).isEqualTo(3.0);
        assertThat(snapshot.processCpu().used().value()).isEqualTo(0.25);
    }

    @Test
    @DisplayName("CPU 원천이 예외를 던져도 나머지 원천은 스냅샷에 남는다")
    void aCpuFailureDoesNotDiscardTheOtherSources() {
        ResourceSnapshot snapshot = new ResourceProvider(
                new StubHikariDataSource(new StubPool(6, 12, 3)),
                new SimpleMeterRegistry(), timeProvider(),
                heapMxBean(64L * 1024 * 1024, 128L * 1024 * 1024), throwingCpuMxBean()).snapshot();

        assertThat(snapshot.processCpu().used().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.processCpu().utilization().state())
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.dbPool().active().value()).isEqualTo(6.0);
        assertThat(snapshot.heapMemory().used().value()).isEqualTo(64.0);
    }

    @Test
    @DisplayName("게이지가 NaN을 내면 값이 아니라 UNAVAILABLE로 낸다")
    void aNaNGaugeIsUnavailableRatherThanAValue() {
        // Micrometer는 게이지 supplier 예외를 삼키고 NaN을 돌려준다. 여기서 걸러야 NaN이
        // 값으로 실리지 않는다.
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Gauge.builder("tomcat.threads.busy",
                        () -> { throw new IllegalStateException("jmx broken"); })
                .strongReference(true).register(registry);
        Gauge.builder("tomcat.threads.config.max", () -> 100.0).register(registry);

        ResourceSnapshot snapshot = new ResourceProvider(
                unsupportedDataSource(), registry, timeProvider()).snapshot();

        assertThat(snapshot.tomcatThreads().used().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.tomcatThreads().used().value()).isNull();
        assertThat(snapshot.tomcatThreads().max().value()).isEqualTo(100.0);
        assertThat(snapshot.tomcatThreads().utilization().state())
                .isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    @DisplayName("Tomcat busy와 config.max를 같은 단위로 읽어 사용률을 계산한다")
    void readsTomcatGauges() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Gauge.builder("tomcat.threads.busy", () -> 25.0).register(registry);
        Gauge.builder("tomcat.threads.config.max", () -> 100.0).register(registry);

        ResourceSnapshot snapshot = new ResourceProvider(
                unsupportedDataSource(), registry, timeProvider()).snapshot();

        assertThat(snapshot.tomcatThreads().used().value()).isEqualTo(25.0);
        assertThat(snapshot.tomcatThreads().max().value()).isEqualTo(100.0);
        assertThat(snapshot.tomcatThreads().utilization().value()).isEqualTo(0.25);
    }

    /**
     * {@code Map.copyOf} 는 순회 순서를 보장하지 않는다. EnumMap 을 그대로 감싸야 선언 순서가 산다.
     *
     * <p>순서가 어긋나도 값은 같아서 아무 데서도 실패하지 않는다 — 화면 순서만 매번 달라진다.
     */
    @Test
    @DisplayName("자원 여섯 행은 enum 선언 순서로 돌아온다")
    void metricsKeepTheDeclaredOrder() {
        ResourceSnapshot snapshot = new ResourceProvider(
                unsupportedDataSource(), new SimpleMeterRegistry(), timeProvider()).snapshot();

        assertThat(snapshot.metrics().keySet())
                .containsExactly(ResourceMetric.values());
    }

    /**
     * 스크레이프가 1초 주기라 실패가 지속되면 같은 줄이 초당 하나씩 쌓인다. 원인당 첫 1회만 남긴다.
     *
     * <p>이 억제가 만드는 반대 방향 실패 — 같은 원인이 <b>회복됐다가 재발</b>하면 두 번째는
     * 조용하다. 재발 시점을 로그로는 알 수 없다.
     */
    @Test
    @DisplayName("같은 실패 원인은 한 번만 로그로 남는다")
    void repeatedFailuresAreLoggedOnce(CapturedOutput output) {
        ResourceProvider provider = new ResourceProvider(
                new ThrowingPoolDataSource(), new SimpleMeterRegistry(), timeProvider());

        provider.snapshot();
        provider.snapshot();
        provider.snapshot();

        assertThat(countOf(output.getAll(), "자원 원천 getActiveConnections 조회 실패")).isEqualTo(1);
        assertThat(countOf(output.getAll(), "자원 원천 getTotalConnections 조회 실패")).isEqualTo(1);
        assertThat(countOf(output.getAll(), "자원 원천 getThreadsAwaitingConnection 조회 실패")).isEqualTo(1);
    }

    /**
     * 리플렉션 호출 실패는 원인이 무엇이든 InvocationTargetException 으로 싸여 온다. 그것만으로
     * 키를 만들면 첫 원인이 이후의 다른 원인을 전부 덮어 아무도 두 번째 장애를 모른다.
     */
    @Test
    @DisplayName("같은 자리라도 실패 원인이 다르면 각각 한 번씩 남는다")
    void differentCausesAreLoggedSeparately(CapturedOutput output) {
        ResourceProvider provider = new ResourceProvider(
                new SwitchingPoolDataSource(), new SimpleMeterRegistry(), timeProvider());

        provider.snapshot();
        provider.snapshot();

        // 총 2회만 보면 억제가 통째로 깨져 같은 원인이 두 번 찍혀도 통과한다. 원인별로 나눠 센다.
        assertThat(countOf(output.getAll(), "IllegalStateException: first cause")).isEqualTo(1);
        assertThat(countOf(output.getAll(), "IllegalArgumentException: second cause")).isEqualTo(1);
    }

    /** 첫 호출은 IllegalStateException, 다음부터는 IllegalArgumentException 을 던진다. */
    public static final class SwitchingPoolDataSource extends AbstractDataSource {

        private final SwitchingPool pool = new SwitchingPool();

        public SwitchingPool getHikariPoolMXBean() {
            return pool;
        }
    }

    public static final class SwitchingPool {

        private int calls;

        public int getActiveConnections() {
            if (calls++ == 0) {
                throw new IllegalStateException("first cause");
            }
            throw new IllegalArgumentException("second cause");
        }

        public int getTotalConnections() {
            return 12;
        }

        public int getThreadsAwaitingConnection() {
            return 0;
        }
    }

    private static int countOf(String haystack, String needle) {
        int count = 0;
        for (int at = haystack.indexOf(needle); at >= 0; at = haystack.indexOf(needle, at + 1)) {
            count++;
        }
        return count;
    }

    /**
     * 같은 자리에서 ClassCastException·SecurityException 까지 잡아 둔 이유가 "한 원천이 비어도
     * 나머지는 살린다" 는 계약이다. null 반환만 그 그물을 빠져나가면 DB 풀 한 칸 때문에
     * CPU·힙까지 전부 날아간다.
     */
    @Test
    @DisplayName("MXBean 이 null 을 돌려줘도 다른 원천은 살아남는다")
    void aNullReturningMxBeanDoesNotKillTheSnapshot() {
        ResourceSnapshot snapshot = new ResourceProvider(
                new NullReturningPoolDataSource(), new SimpleMeterRegistry(), timeProvider()).snapshot();

        assertThat(snapshot.dbPool().active().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.processCpu().used().state()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.heapMemory().used().state()).isEqualTo(SourceStatus.VALID);
    }

    /** 박싱 타입 null 을 돌려주는 풀. Hikari 는 원시 타입이라 이 경우가 없지만 계약은 대칭이어야 한다. */
    public static final class NullReturningPoolDataSource extends AbstractDataSource {

        public NullReturningPool getHikariPoolMXBean() {
            return new NullReturningPool();
        }
    }

    public static final class NullReturningPool {

        public Integer getActiveConnections() {
            return null;
        }

        public Integer getTotalConnections() {
            return null;
        }

        public Integer getThreadsAwaitingConnection() {
            return null;
        }
    }

    /** 호출마다 터지는 풀. 리플렉션 호출 실패를 InvocationTargetException 으로 만든다. */
    public static final class ThrowingPoolDataSource extends AbstractDataSource {

        public ThrowingPool getHikariPoolMXBean() {
            return new ThrowingPool();
        }
    }

    public static final class ThrowingPool {

        public int getActiveConnections() {
            throw new IllegalStateException("pool broken");
        }

        public int getTotalConnections() {
            throw new IllegalStateException("pool broken");
        }

        public int getThreadsAwaitingConnection() {
            throw new IllegalStateException("pool broken");
        }
    }

    private static DataSource unsupportedDataSource() {
        return (DataSource) Proxy.newProxyInstance(
                ResourceProviderTest.class.getClassLoader(),
                new Class<?>[] { DataSource.class },
                (proxy, method, arguments) -> {
                    if (method.getName().equals("toString")) {
                        return "unsupported-data-source";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static MemoryMXBean heapMxBean(long usedBytes, long maxBytes) {
        MemoryUsage heap = new MemoryUsage(-1L, usedBytes, usedBytes, maxBytes);
        return (MemoryMXBean) Proxy.newProxyInstance(
                ResourceProviderTest.class.getClassLoader(),
                new Class<?>[] { MemoryMXBean.class },
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getHeapMemoryUsage" -> heap;
                    case "toString" -> "stub-memory-mx-bean";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static MemoryMXBean throwingMemoryMxBean() {
        return (MemoryMXBean) Proxy.newProxyInstance(
                ResourceProviderTest.class.getClassLoader(),
                new Class<?>[] { MemoryMXBean.class },
                (proxy, method, arguments) -> {
                    if (method.getName().equals("getHeapMemoryUsage")) {
                        throw new IllegalStateException("heap mxbean unavailable");
                    }
                    return "stub-memory-mx-bean";
                });
    }

    private static com.sun.management.OperatingSystemMXBean throwingCpuMxBean() {
        return (com.sun.management.OperatingSystemMXBean) Proxy.newProxyInstance(
                ResourceProviderTest.class.getClassLoader(),
                new Class<?>[] { com.sun.management.OperatingSystemMXBean.class },
                (proxy, method, arguments) -> {
                    if (method.getName().equals("getProcessCpuLoad")) {
                        throw new IllegalStateException("cpu mxbean unavailable");
                    }
                    return "stub-operating-system-mx-bean";
                });
    }

    private static com.sun.management.OperatingSystemMXBean processCpuLoad(double load) {
        return (com.sun.management.OperatingSystemMXBean) Proxy.newProxyInstance(
                ResourceProviderTest.class.getClassLoader(),
                new Class<?>[] { com.sun.management.OperatingSystemMXBean.class },
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getProcessCpuLoad" -> load;
                    case "toString" -> "stub-operating-system-mx-bean";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static TimeProvider timeProvider() {
        return new TimeProvider(CLOCK);
    }

    public static final class StubHikariDataSource extends AbstractDataSource {

        private final StubPool pool;

        StubHikariDataSource(StubPool pool) {
            this.pool = pool;
        }

        public StubPool getHikariPoolMXBean() {
            return pool;
        }
    }

    public static final class StubPool {

        private final int active;
        private final int total;
        private final Integer awaiting;

        StubPool(int active, int total, Integer awaiting) {
            this.active = active;
            this.total = total;
            this.awaiting = awaiting;
        }

        public int getActiveConnections() {
            return active;
        }

        public int getTotalConnections() {
            return total;
        }

        public int getThreadsAwaitingConnection() {
            if (awaiting == null) {
                throw new IllegalStateException("awaiting unavailable");
            }
            return awaiting;
        }
    }

    private abstract static class AbstractDataSource implements DataSource {

        @Override
        public java.sql.Connection getConnection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.sql.Connection getConnection(String username, String password) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T unwrap(Class<T> iface) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }

        @Override
        public java.io.PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(java.io.PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public java.util.logging.Logger getParentLogger() {
            return java.util.logging.Logger.getGlobal();
        }
    }
}
