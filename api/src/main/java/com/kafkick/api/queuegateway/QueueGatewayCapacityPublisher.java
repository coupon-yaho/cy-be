package com.kafkick.api.queuegateway;

import java.time.Clock;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.scheduling.annotation.Scheduled;

import com.kafkick.core.queuegateway.QueueGatewayStatePort;

import jakarta.annotation.PreDestroy;

/** API 인스턴스의 설정된 안전 처리 상한을 외부 게이트웨이에 주기적으로 알립니다. */
public final class QueueGatewayCapacityPublisher {

    private static final Logger log = LoggerFactory.getLogger(QueueGatewayCapacityPublisher.class);

    private final QueueGatewayStatePort statePort;
    private final ApplicationAvailability availability;
    private final QueueGatewayPublisherProperties properties;
    private final Clock clock;

    public QueueGatewayCapacityPublisher(
            QueueGatewayStatePort statePort,
            ApplicationAvailability availability,
            QueueGatewayPublisherProperties properties,
            Clock clock
    ) {
        this.statePort = Objects.requireNonNull(statePort, "statePort");
        this.availability = Objects.requireNonNull(availability, "availability");
        this.properties = Objects.requireNonNull(properties, "properties");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** 준비 완료 인스턴스에는 설정된 안전 상한을, 그 밖에는 0을 보고합니다. */
    @Scheduled(
            fixedRateString = "${queue.gateway.publisher.capacity-interval:1s}",
            scheduler = QueueGatewayPublisherConfiguration.CAPACITY_SCHEDULER
    )
    public void publishCapacity() {
        long credits = availability.getReadinessState() == ReadinessState.ACCEPTING_TRAFFIC
                ? properties.creditsPerSecond()
                : 0L;
        try {
            statePort.reportCapacity(properties.instanceId(), credits, clock.instant());
        } catch (RuntimeException exception) {
            log.warn("대기열 게이트웨이 가용량 공급에 실패했습니다. instanceId={}",
                    properties.instanceId(), exception);
        }
    }

    /** 정상 종료 시 이 인스턴스의 필드만 제거하고 Redis 장애는 종료 흐름과 격리합니다. */
    @PreDestroy
    public void removeCapacity() {
        try {
            statePort.removeCapacity(properties.instanceId());
        } catch (RuntimeException exception) {
            log.warn("대기열 게이트웨이 가용량 필드 제거에 실패했습니다. instanceId={}",
                    properties.instanceId(), exception);
        }
    }
}
