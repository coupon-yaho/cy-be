package com.kafkick.api.observation.resource;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import com.kafkick.api.admin.support.ObservedValue;

/**
 * {@link ResourceProvider}가 한 API 프로세스에서 같은 시각에 읽은 자원 현재값입니다.
 *
 * <p>이 스냅샷은 요청을 받은 프로세스 하나만 나타냅니다. 여러 API 인스턴스의 최댓값 비교는
 * 이 값을 fan-out해서 만들지 않습니다. Prometheus가 scrape 때 붙이는 {@code instance} 라벨과
 * Spring Boot 자동 계측 미터를 사용하는 OBS-9 경로의 책임입니다.
 *
 * <p>{@link #availableProcessors()}는 자원 행이 아니라 회차의 실행 조건입니다. JVM 설정을
 * 추측하지 않고 실제 반환값을 남겨, 서로 다른 회차가 같은 CPU 조건이었는지 확인하게 합니다.
 *
 * <p>DISK의 {@code PENDING}은 곧 값이 도착한다는 약속이 아닙니다. DB 머신의 node_exporter
 * 원천이 아직 연결되지 않았다는 뜻도 포함합니다. 따라서 소비자는 {@code PENDING}을 무조건
 * 로딩 또는 스피너로 표현하면 안 됩니다. NETWORK의 {@code N_A}는 이 환경에서 영구적으로
 * 측정하지 않는 자리입니다.
 */
public record ResourceSnapshot(
        int availableProcessors,
        DbPool dbPool,
        Usage processCpu,
        Usage heapMemory,
        Usage tomcatThreads,
        Usage disk,
        Usage network
) {

    public ResourceSnapshot {
        if (availableProcessors <= 0) {
            throw new IllegalArgumentException("availableProcessors는 양수여야 합니다.");
        }
        Objects.requireNonNull(dbPool, "dbPool");
        Objects.requireNonNull(processCpu, "processCpu");
        Objects.requireNonNull(heapMemory, "heapMemory");
        Objects.requireNonNull(tomcatThreads, "tomcatThreads");
        Objects.requireNonNull(disk, "disk");
        Objects.requireNonNull(network, "network");
    }

    /**
     * OBS-9 정렬 대상 여섯 행을 빠짐없이 돌려줍니다.
     *
     * <p>{@code Map.copyOf} 가 아니라 EnumMap 을 그대로 감싼다 — copyOf 가 돌려주는 맵은 순회
     * 순서를 보장하지 않아 {@link ResourceMetric} 선언 순서가 사라진다. 값은 같으므로 화면이
     * 매번 다른 순서로 그려져도 아무 데서도 실패하지 않는다.
     */
    public Map<ResourceMetric, Usage> metrics() {
        EnumMap<ResourceMetric, Usage> metrics = new EnumMap<>(ResourceMetric.class);
        metrics.put(ResourceMetric.DB_POOL, dbPool.usage());
        metrics.put(ResourceMetric.PROCESS_CPU, processCpu);
        metrics.put(ResourceMetric.HEAP_MEMORY, heapMemory);
        metrics.put(ResourceMetric.TOMCAT_THREADS, tomcatThreads);
        metrics.put(ResourceMetric.DISK, disk);
        metrics.put(ResourceMetric.NETWORK, network);
        return Collections.unmodifiableMap(metrics);
    }

    /** used/max와 그 비율을 각자 상태를 가진 값으로 보존합니다. */
    public record Usage(
            ObservedValue<Double> used,
            ObservedValue<Double> max,
            ObservedValue<Double> utilization
    ) {
        public Usage {
            Objects.requireNonNull(used, "used");
            Objects.requireNonNull(max, "max");
            Objects.requireNonNull(utilization, "utilization");
        }
    }

    /**
     * DB 풀 행의 원천 값입니다. awaiting은 사용률과 별개로 읽고 상태도 별도로 보존합니다.
     * 스파이크 부하에서는 사용률이 100%로 고정되므로 버전 차이를 드러내는 핵심 값입니다.
     */
    public record DbPool(
            ObservedValue<Double> active,
            ObservedValue<Double> total,
            ObservedValue<Double> awaiting,
            ObservedValue<Double> utilization
    ) {
        public DbPool {
            Objects.requireNonNull(active, "active");
            Objects.requireNonNull(total, "total");
            Objects.requireNonNull(awaiting, "awaiting");
            Objects.requireNonNull(utilization, "utilization");
        }

        Usage usage() {
            return new Usage(active, total, utilization);
        }
    }
}
