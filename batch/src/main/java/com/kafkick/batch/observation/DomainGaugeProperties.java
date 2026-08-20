package com.kafkick.batch.observation;

import com.kafkick.core.observation.EngineVersion;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 도메인 Gauge 수집 설정.
 *
 * <p>TODO(OBS-14b 담당): {@code engineVersion} 과 {@code couponId} 는 둘 다 진행 중인
 * {@code benchmark_runs} 행에서 읽는다. 측정 회차의 권위는 런타임 토글이 아니라 그 회차를 시작할
 * 때 박제된 값이다. 그 시점은 {@code BatchBenchmarkRunsAssumptionTest} 가 알려 준다.
 *
 * <p>{@code engineVersion} 은 회차 설정과 같은 값이어야 한다. 어긋나면 Redis 를 쓰지 않는 회차에서
 * Redis 계열 gap 이 UNAVAILABLE 로 보이거나(경보가 뜬다), 반대로 실제 불일치가 N_A 로 숨는다.
 *
 * @param engineVersion 발급 엔진 버전; V1 은 Redis 를 아예 조회하지 않고 Redis 계열을 N_A 로 둔다
 * @param couponId 관측 대상 회차; 비우면 "가장 최근 열린 회차" 를 매번 다시 고른다
 * @param consecutiveFailureAlarm 이 횟수부터 수집 실패를 ERROR 로 올린다; 생략하면 3
 * @param remainingKey Redis 재고 잔량 키
 * @param issuedEverKey Redis 누적 발급 카운터 키
 * @param memberEverKey Redis 발급 회원 집합 키
 * @param queueKey 대기열 키
 */
@ConfigurationProperties("observation.domain-gauge")
public record DomainGaugeProperties(
    EngineVersion engineVersion,
    Long couponId,
    Integer consecutiveFailureAlarm,
    String remainingKey,
    String issuedEverKey,
    String memberEverKey,
    String queueKey
) {

    /** 키에서 관측 대상 회차로 치환되는 자리. */
    public static final String COUPON_ID_PLACEHOLDER = "{couponId}";

    private static final int DEFAULT_CONSECUTIVE_FAILURE_ALARM = 3;

    public DomainGaugeProperties {
        engineVersion = engineVersion != null ? engineVersion : EngineVersion.V1;
        // 수집 주기가 경로마다 다르므로(재고 1초 · 정합성 30초) 같은 횟수가 뜻하는 시간도 다르다.
        // 부하 중에 재기동 없이 조일 수 있어야 한다.
        consecutiveFailureAlarm = consecutiveFailureAlarm != null
            ? consecutiveFailureAlarm
            : DEFAULT_CONSECUTIVE_FAILURE_ALARM;
        remainingKey = orDefault(remainingKey, "coupon:" + COUPON_ID_PLACEHOLDER + ":stock:remaining");
        issuedEverKey = orDefault(issuedEverKey, "coupon:" + COUPON_ID_PLACEHOLDER + ":issued");
        memberEverKey = orDefault(memberEverKey, "coupon:" + COUPON_ID_PLACEHOLDER + ":members");
        queueKey = orDefault(queueKey, "coupon:" + COUPON_ID_PLACEHOLDER + ":queue");
    }

    private static String orDefault(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    /**
     * 키 템플릿의 자리표시자를 관측 대상 회차로 채운다.
     *
     * @param key 자리표시자를 포함한 키 템플릿
     * @param couponId 관측 대상 회차 식별자
     * @return 실제 조회에 쓸 키
     */
    public static String resolve(String key, long couponId) {
        return key.replace(COUPON_ID_PLACEHOLDER, Long.toString(couponId));
    }
}
