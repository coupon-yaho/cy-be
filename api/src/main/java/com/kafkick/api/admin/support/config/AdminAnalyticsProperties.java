package com.kafkick.api.admin.support.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** 관리자 분석 API의 조회 범위와 개발용 Mock 노출 및 최신성 설정입니다. */
@ConfigurationProperties(prefix = "admin.analytics")
public record AdminAnalyticsProperties(
        @DefaultValue("1") int maxRangeYears,
        // [OBS-36] 기본값을 뒤집었다. 예전에는 true 라, AdminAnalyticsConfig 의
        // "운영 기본값에서는 PendingSource 를 준다" 는 주석과 반대로 **운영에서도 Mock 이
        // 떴다.** 이 키를 설정하는 yml 이 하나도 없어서 기본값이 곧 운영값이었다.
        // 이제 명시적으로 켜야 가짜 통계가 나간다. 끈 상태는 화면이 죽는 것이 아니라
        // AdminAnalyticsPendingSource 가 "아직 집계되지 않았음" 을 낸다 —
        // 이 화면만은 그 자리를 표현할 계약(AdminAnalyticsDataset)이 이미 있다.
        @DefaultValue("false") boolean mockEnabled,
        Duration staleAfter
) {

    private static final Duration DEFAULT_STALE_AFTER = Duration.ofHours(24);

    /** 조회 범위와 분석 원천에 공통으로 적용할 최신성 기준을 검증합니다. */
    @ConstructorBinding
    public AdminAnalyticsProperties {
        if (maxRangeYears < 1) {
            throw new IllegalArgumentException("admin.analytics.max-range-years는 1 이상이어야 합니다.");
        }
        if (staleAfter != null && (staleAfter.isZero() || staleAfter.isNegative())) {
            throw new IllegalArgumentException("admin.analytics.stale-after는 양수여야 합니다.");
        }
        if (staleAfter == null) {
            // 실제 원천으로 교체돼도 최신성 판정이 비활성화되지 않도록 동일한 기본값을 적용합니다.
            staleAfter = DEFAULT_STALE_AFTER;
        }
    }

    /**
     * 조회 기간 검증용으로 <b>Mock 을 켠</b> 설정을 간단히 구성합니다.
     *
     * <p><b>[OBS-36] 이름에 {@code MockEnabled} 를 넣은 이유.</b> 예전에는
     * {@code new AdminAnalyticsProperties(1)} 이라는 편의 생성자였고, 그것이 {@code true} 를
     * 박고 있었다. 필드 기본값을 {@code false} 로 뒤집은 뒤에는 <b>같은 클래스 안에서 기본값이
     * 두 가지</b>로 보여, 어느 쪽이 운영 기본값인지 읽는 사람이 판단할 수 없었다.
     * 이제 이름이 그 답을 갖는다 — 기본값은 꺼짐이고, 켜는 것은 이 호출을 <b>고른</b> 쪽이다.
     */
    public static AdminAnalyticsProperties withMockEnabled(int maxRangeYears) {
        return new AdminAnalyticsProperties(maxRangeYears, true, null);
    }
}
