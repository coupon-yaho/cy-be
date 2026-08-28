package com.kafkick.batch.observation;

import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

/** 문자열 플레이스홀더 대신 검증이 끝난 설정값으로 수집 주기를 등록한다. */
public class PendingIssuedGaugeScheduler implements SchedulingConfigurer {

    private final PendingIssuedGaugeCollector collector;
    private final PendingIssuedGaugeProperties properties;

    public PendingIssuedGaugeScheduler(
        PendingIssuedGaugeCollector collector,
        PendingIssuedGaugeProperties properties
    ) {
        this.collector = collector;
        this.properties = properties;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addFixedDelayTask(collector::collect, properties.interval());
    }
}
