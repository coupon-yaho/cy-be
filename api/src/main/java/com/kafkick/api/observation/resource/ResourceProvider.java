package com.kafkick.api.observation.resource;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.api.observation.MeterNames;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 현재 API 프로세스의 자원 사용량을 한 번에 읽습니다.
 *
 * <h2>이 값은 자기 프로세스 것이다</h2>
 *
 * MXBean 과 로컬 {@link MeterRegistry} 를 직접 읽으므로 Prometheus 를 거치지 않는다. 그래서
 * 이 값에는 <b>{@code instance} 라벨이 붙을 자리가 없다</b> — 그 라벨은 Prometheus 가 스크레이프
 * 하면서 붙이는 것이고, 앱이 만들어 낼 수 있는 값이 아니다.
 *
 * <p>화면의 자원 행은 다른 경로가 소유한다 —
 * {@code com.kafkick.api.admin.observability.MetricAggregation} 의 규칙표로 인스턴스들을 줄이고,
 * 어느 대인지는 Prometheus 의 {@code instance} 라벨로 가른다.
 *
 * <p><b>그러므로 이 스냅샷을 화면에 그대로 실으면 안 된다.</b> api 를 여러 대 띄우면 요청을 받은
 * 한 대의 값만 나오는데 화면은 그게 어느 대인지 말할 방법이 없다 — 새로고침마다 다른 대의 수치가
 * 같은 자리에 그려지고, 값이 틀린 게 아니라서 예외도 로그도 남지 않는다. 쓰려면 자기 인스턴스
 * 진단 용도로만 쓴다.
 */
public final class ResourceProvider {

    private static final Logger log = LoggerFactory.getLogger(ResourceProvider.class);

    private static final double BYTES_PER_MEBIBYTE = 1024.0 * 1024.0;

    private final DataSource dataSource;
    private final MeterRegistry meterRegistry;
    private final TimeProvider timeProvider;
    private final MemoryMXBean memoryMxBean;
    private final com.sun.management.OperatingSystemMXBean operatingSystemMxBean;

    /**
     * 이미 로그로 남긴 실패 원인. 이 클래스에서 유일한 가변 상태다.
     *
     * <p>스크레이프가 1초 주기라 실패가 지속되면 같은 줄이 초당 하나씩 쌓인다. 그렇다고 DEBUG 로
     * 낮추면 평시에 꺼 두므로 사고가 난 뒤에는 확인할 방법이 없다 — 원천 부재를 알아채려고 넣는
     * 로그가 정작 알아챌 수 없게 된다. 그래서 원인당 첫 1회만 WARN 으로 남긴다.
     *
     * <p>여러 스레드가 동시에 snapshot() 을 부르므로 동시성 집합을 쓴다. {@code add} 가 원자적이라
     * 경합해도 한 번만 찍힌다.
     */
    private final Set<String> loggedFailures = ConcurrentHashMap.newKeySet();

    public ResourceProvider(
            DataSource dataSource,
            MeterRegistry meterRegistry,
            TimeProvider timeProvider
    ) {
        this(
                dataSource,
                meterRegistry,
                timeProvider,
                ManagementFactory.getMemoryMXBean(),
                operatingSystemMxBean());
    }

    ResourceProvider(
            DataSource dataSource,
            MeterRegistry meterRegistry,
            TimeProvider timeProvider,
            MemoryMXBean memoryMxBean,
            com.sun.management.OperatingSystemMXBean operatingSystemMxBean
    ) {
        this.dataSource = dataSource;
        this.meterRegistry = meterRegistry;
        this.timeProvider = timeProvider;
        this.memoryMxBean = memoryMxBean;
        this.operatingSystemMxBean = operatingSystemMxBean;
    }

    /**
     * 네 원천은 서로 독립적으로 읽습니다. 한 원천이 비어도 다른 값과 DB awaiting을 버리지 않습니다.
     */
    public ResourceSnapshot snapshot() {
        Instant observedAt = timeProvider.instant();
        return new ResourceSnapshot(
                Runtime.getRuntime().availableProcessors(),
                dbPool(observedAt),
                processCpu(observedAt),
                heap(observedAt),
                tomcatThreads(observedAt),
                pendingUsage(),
                notApplicableUsage());
    }

    private ResourceSnapshot.DbPool dbPool(Instant observedAt) {
        Object pool = hikariPoolMxBean();
        ObservedValue<Double> active = mxBeanNumber(pool, "getActiveConnections", observedAt);
        ObservedValue<Double> total = mxBeanNumber(pool, "getTotalConnections", observedAt);
        // 별도 호출이다. awaiting 조회만 실패해도 active/total/utilization은 계속 유효할 수 있다.
        ObservedValue<Double> awaiting = mxBeanNumber(pool, "getThreadsAwaitingConnection", observedAt);
        return new ResourceSnapshot.DbPool(active, total, awaiting,
                utilization(active, total, observedAt));
    }

    /**
     * Hikari 가 아닌 DataSource 도 예외 없이 UNAVAILABLE 로 흘려보낸다.
     *
     * <p><b>예외 없이 null 이 나오는 길도 있다.</b> 풀은 첫 커넥션에서 만들어지므로
     * ({@code ObservationDataSourceConfig} 가 setter 로 지연 초기화한다) 그 전에는
     * {@code getHikariPoolMXBean()} 이 그냥 null 이다. {@link #mxBeanNumber} 가 그것을
     * UNAVAILABLE 로 흘리는데, 기동 직후만 보면 PENDING 이 더 정확하다.
     *
     * <p>그래도 UNAVAILABLE 로 두는 것이 <b>의도</b>다. Hikari 는 "아직 안 떴다" 와 "떠보려다 DB 가
     * 죽어 실패했다" 를 둘 다 null 로 돌려주어 호출부에서 구별되지 않는다. PENDING 으로 바꾸면
     * 기동 직후 몇 초가 정확해지는 대신 DB 완전 장애가 "준비 중" 으로 보인다 — 없는 장애를 찾는
     * 것보다 있는 장애를 놓치는 쪽이 비싸다.
     *
     * <p>{@code SecurityException} 까지 잡는 이유 — SecurityManager 아래에서 리플렉션이 막히면
     * 이것만 검사 예외가 아니라 그대로 snapshot() 밖으로 튀어 나가, "한 원천이 비어도 나머지는
     * 살린다" 는 이 클래스의 계약이 이 경로에서만 깨진다.
     *
     * <p><b>이 catch 는 테스트가 없다.</b> SecurityException 은 JDK 내부가 던지는 것이라
     * 테스트에서 만들려면 SecurityManager 를 세워야 하고, Java 21 에서는 그게
     * {@code -Djava.security.manager=allow} 없이는 되지 않는다. 떼어 내도 아무 테스트도 실패하지
     * 않으므로(확인함) 다음 사람이 죽은 코드로 보고 지울 수 있다. 실행 이미지에 SecurityManager
     * 가 없어 실제로 걸리는 경로도 아니다 — 계약의 대칭을 위해 남긴 것이다.
     */
    private Object hikariPoolMxBean() {
        try {
            Method method = dataSource.getClass().getMethod("getHikariPoolMXBean");
            return method.invoke(dataSource);
        } catch (NoSuchMethodException ex) {
            // Hikari 가 아닌 풀이다. 설정상 정상일 수 있어 로그를 남기지 않는다.
            return null;
        } catch (IllegalAccessException | InvocationTargetException | SecurityException ex) {
            logOnce("getHikariPoolMXBean", ex);
            return null;
        }
    }

    /** 같은 원인은 한 번만 남긴다. 원인은 호출 지점과 예외 타입으로 구분한다. */
    private void logOnce(String source, Exception ex) {
        if (loggedFailures.add(source + "|" + ex.getClass().getName())) {
            log.warn("자원 원천 {} 조회 실패 — 해당 값은 UNAVAILABLE 로 내보낸다."
                    + " 동일 원인은 다시 로그하지 않는다.", source, ex);
        }
    }

    // TODO(OBS-9): 폴링 엔드포인트가 이걸 소비하기 시작하면 Method 조회를 캐시한다. 지금은
    // 호출마다 getMethod 를 다시 한다 — 소비처가 없어 측정된 비용이 없고, 캐시는 이 클래스에
    // 가변 상태를 하나 더 만들므로 실제 폴링 주기가 정해진 뒤에 재본다.
    private ObservedValue<Double> mxBeanNumber(Object bean, String methodName, Instant observedAt) {
        if (bean == null) {
            // 풀 자체가 없다. hikariPoolMxBean() 에서 이미 판단한 것이라 여기서 또 남기지 않는다.
            return unavailable();
        }
        try {
            Method method = bean.getClass().getMethod(methodName);
            Number value = (Number) method.invoke(bean);
            if (value == null) {
                // 캐스팅은 null 을 통과시키므로 여기서 막지 않으면 doubleValue() 의 NPE 가
                // 아래 catch 를 빠져나가 snapshot() 전체를 죽인다. 계약은 이 경로에서도 대칭이다.
                return unavailable();
            }
            return valid(value.doubleValue(), observedAt);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException
                 | ClassCastException | SecurityException ex) {
            logOnce(methodName, ex);
            return unavailable();
        }
    }

    private ResourceSnapshot.Usage processCpu(Instant observedAt) {
        double used;
        try {
            used = operatingSystemMxBean.getProcessCpuLoad();
        } catch (RuntimeException ex) {
            logOnce("getProcessCpuLoad", ex);
            return unavailableUsage();
        }
        if (!Double.isFinite(used) || used < 0.0) {
            return unavailableUsage();
        }
        return usage(used, 1.0, observedAt);
    }

    private ResourceSnapshot.Usage heap(Instant observedAt) {
        MemoryUsage heap;
        try {
            heap = memoryMxBean.getHeapMemoryUsage();
        } catch (RuntimeException ex) {
            logOnce("getHeapMemoryUsage", ex);
            return unavailableUsage();
        }
        double usedMiB = heap.getUsed() / BYTES_PER_MEBIBYTE;
        long maxBytes = heap.getMax();
        if (maxBytes < 0) {
            return new ResourceSnapshot.Usage(
                    valid(usedMiB, observedAt), notApplicable(), notApplicable());
        }
        return usage(usedMiB, maxBytes / BYTES_PER_MEBIBYTE, observedAt);
    }

    private ResourceSnapshot.Usage tomcatThreads(Instant observedAt) {
        OptionalDouble busy = gauge(MeterNames.TOMCAT_BUSY);
        OptionalDouble max = gauge(MeterNames.TOMCAT_MAX);
        ObservedValue<Double> usedValue = observed(busy, observedAt);
        ObservedValue<Double> maxValue = observed(max, observedAt);
        return new ResourceSnapshot.Usage(
                usedValue, maxValue, utilization(usedValue, maxValue, observedAt));
    }

    private OptionalDouble gauge(String name) {
        Gauge gauge = meterRegistry.find(name).gauge();
        if (gauge == null) {
            return OptionalDouble.empty();
        }
        double value = gauge.value();
        return Double.isFinite(value) ? OptionalDouble.of(value) : OptionalDouble.empty();
    }

    private static ResourceSnapshot.Usage usage(double used, double max, Instant observedAt) {
        ObservedValue<Double> usedValue = valid(used, observedAt);
        ObservedValue<Double> maxValue = valid(max, observedAt);
        return new ResourceSnapshot.Usage(
                usedValue, maxValue, utilization(usedValue, maxValue, observedAt));
    }

    /** max가 없으면 N_A, 0 이하이면 아직 유효한 분모가 없으므로 PENDING입니다. */
    private static ObservedValue<Double> utilization(
            ObservedValue<Double> used,
            ObservedValue<Double> max,
            Instant observedAt
    ) {
        if (max.state() == SourceStatus.N_A) {
            return notApplicable();
        }
        if (used.state() != SourceStatus.VALID || max.state() != SourceStatus.VALID) {
            return unavailable();
        }
        if (max.value() <= 0.0) {
            return pending();
        }
        return valid(used.value() / max.value(), observedAt);
    }

    private static ObservedValue<Double> observed(OptionalDouble value, Instant observedAt) {
        return value.isPresent() ? valid(value.getAsDouble(), observedAt) : unavailable();
    }

    private static ResourceSnapshot.Usage pendingUsage() {
        return new ResourceSnapshot.Usage(pending(), pending(), pending());
    }

    private static ResourceSnapshot.Usage notApplicableUsage() {
        return new ResourceSnapshot.Usage(notApplicable(), notApplicable(), notApplicable());
    }

    private static ResourceSnapshot.Usage unavailableUsage() {
        return new ResourceSnapshot.Usage(unavailable(), unavailable(), unavailable());
    }

    private static ObservedValue<Double> valid(double value, Instant observedAt) {
        return new ObservedValue<>(value, SourceStatus.VALID, observedAt);
    }

    private static ObservedValue<Double> pending() {
        return new ObservedValue<>(null, SourceStatus.PENDING, null);
    }

    private static ObservedValue<Double> unavailable() {
        return new ObservedValue<>(null, SourceStatus.UNAVAILABLE, null);
    }

    private static ObservedValue<Double> notApplicable() {
        return new ObservedValue<>(null, SourceStatus.N_A, null);
    }

    /**
     * <b>여기만 UNAVAILABLE로 강등하지 않고 던진다.</b> 나머지 원천 부재는 호출마다 달라지는
     * 일시적 실패라 값 하나만 비우면 되지만, 이 확장 인터페이스가 없는 것은 JVM 전체·영구다 —
     * 조용히 넘기면 CPU 행이 회차 내내 빈 채로 부하 시험이 끝난다. 기동 때 크게 터지는 편이 낫다.
     *
     * <p>실행 이미지가 {@code eclipse-temurin:21-jre-jammy}(HotSpot) 로 고정돼 있어 실제로는
     * 걸리지 않는다. 배포본을 바꾸면 여기부터 확인해라.
     */
    private static com.sun.management.OperatingSystemMXBean operatingSystemMxBean() {
        java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean extended) {
            return extended;
        }
        throw new IllegalStateException("process CPU load를 제공하는 OperatingSystemMXBean이 없습니다.");
    }
}
