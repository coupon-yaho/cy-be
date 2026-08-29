package com.kafkick.batch.observation;

import com.kafkick.core.consistency.ConsistencySeverityPolicy;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * LIVE 정합성 심각도 임계값. api 와 <b>같은 키</b>({@code observation.consistency.severity})를 읽는다 —
 * 두 JVM 이 같은 회차를 다른 기준으로 판정하면 화면과 경보가 서로를 반증한다.
 *
 * @param warnThreshold WARN 으로 올리는 절댓값 하한; 생략하면 도메인 기본값
 * @param criticalThreshold CRITICAL 로 올리는 절댓값 하한; 생략하면 도메인 기본값
 */
@ConfigurationProperties(prefix = "observation.consistency.severity")
public record ConsistencySeverityProperties(Long warnThreshold, Long criticalThreshold) {

    /**
     * 설정값과 도메인 기본값을 합쳐 계산기에 주입할 정책을 만든다.
     *
     * @return LIVE 심각도 정책
     */
    public ConsistencySeverityPolicy toPolicy() {
        ConsistencySeverityPolicy defaults = ConsistencySeverityPolicy.defaults();
        return new ConsistencySeverityPolicy(
            warnThreshold == null ? defaults.warnThreshold() : warnThreshold,
            criticalThreshold == null ? defaults.criticalThreshold() : criticalThreshold
        );
    }
}
