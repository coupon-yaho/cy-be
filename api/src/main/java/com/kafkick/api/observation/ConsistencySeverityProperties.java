package com.kafkick.api.observation;

import com.kafkick.core.consistency.ConsistencySeverityPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * LIVE 정합성 심각도 임계값을 외부 설정에서 읽습니다.
 *
 * @param warnThreshold WARN으로 올리는 절댓값 하한; 생략하면 도메인 기본값 사용
 * @param criticalThreshold CRITICAL로 올리는 절댓값 하한; 생략하면 도메인 기본값 사용
 */
@ConfigurationProperties(prefix = "observation.consistency.severity")
public record ConsistencySeverityProperties(Long warnThreshold, Long criticalThreshold) {

    /**
     * 설정값과 도메인 기본값을 합쳐 계산기에 주입할 정책을 생성합니다.
     *
     * @return 검증을 마친 LIVE 심각도 정책
     * @throws IllegalArgumentException 임계값이 양수가 아니거나 순서가 올바르지 않은 경우
     */
    public ConsistencySeverityPolicy toPolicy() {
        ConsistencySeverityPolicy defaults = ConsistencySeverityPolicy.defaults();
        return new ConsistencySeverityPolicy(
                warnThreshold == null ? defaults.warnThreshold() : warnThreshold,
                criticalThreshold == null ? defaults.criticalThreshold() : criticalThreshold
        );
    }
}
