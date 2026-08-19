package com.kafkick.core.consistency;

/**
 * V2 LIVE Redis↔DB 드리프트를 운영 심각도로 변환할 때 사용하는 임계치입니다.
 *
 * <p>임계치는 LIVE 화면의 경보 수준에만 영향을 줍니다. FINAL 정합성 판정은 이 정책과 무관하게
 * 적용 가능한 gap이 하나라도 0이 아니면 실패합니다.
 *
 * @param warnThreshold WARN으로 올리는 절댓값 하한; 0보다 커야 함
 * @param criticalThreshold CRITICAL로 올리는 절댓값 하한; warnThreshold보다 커야 함
 */
public record ConsistencySeverityPolicy(long warnThreshold, long criticalThreshold) {

    private static final long DEFAULT_WARN_THRESHOLD = 10;
    private static final long DEFAULT_CRITICAL_THRESHOLD = 100;

    /** WARN과 CRITICAL 임계치의 순서와 범위를 검증합니다. */
    public ConsistencySeverityPolicy {
        if (warnThreshold <= 0) {
            throw new IllegalArgumentException("warnThreshold는 0보다 커야 합니다.");
        }
        if (criticalThreshold <= warnThreshold) {
            throw new IllegalArgumentException("criticalThreshold는 warnThreshold보다 커야 합니다.");
        }
    }

    /**
     * PRD에서 확정한 WARN 10, CRITICAL 100 정책을 반환합니다.
     *
     * @return 기본 V2 LIVE 심각도 정책
     */
    public static ConsistencySeverityPolicy defaults() {
        return new ConsistencySeverityPolicy(DEFAULT_WARN_THRESHOLD, DEFAULT_CRITICAL_THRESHOLD);
    }
}
