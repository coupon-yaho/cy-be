package com.kafkick.core.admin.analytics;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateAvailability;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.exception.BusinessException;

/** 원천 가용성과 관측 경과 시간으로 관리자 분석의 최신성을 판정합니다. */
public final class AdminAnalyticsFreshnessPolicy {

    private final Duration staleAfter;

    /** 실제 집계 원천이 없는 동안 값 없는 상태만 처리하는 정책을 만듭니다. */
    public static AdminAnalyticsFreshnessPolicy pendingOnly() {
        return new AdminAnalyticsFreshnessPolicy();
    }

    /** 임의의 실제 집계 최신성 기준을 만들지 않기 위한 Pending 전용 생성자입니다. */
    private AdminAnalyticsFreshnessPolicy() {
        this.staleAfter = null;
    }

    /** 양수 최신성 허용 시간을 설정합니다. */
    public AdminAnalyticsFreshnessPolicy(Duration staleAfter) {
        this.staleAfter = Objects.requireNonNull(staleAfter, "staleAfter");
        if (staleAfter.isZero() || staleAfter.isNegative()) {
            throw new IllegalArgumentException("staleAfter는 양수여야 합니다.");
        }
    }

    /** Source의 미수집·장애를 보존하고 가용한 값만 VALID 또는 STALE로 판정합니다. */
    public SourceStatus evaluate(
            AggregateAvailability availability,
            Instant observedAt,
            Instant evaluatedAt
    ) {
        Objects.requireNonNull(availability, "availability");
        Objects.requireNonNull(evaluatedAt, "evaluatedAt");
        return switch (availability) {
            case PENDING -> SourceStatus.PENDING;
            case UNAVAILABLE -> SourceStatus.UNAVAILABLE;
            case AVAILABLE -> evaluateAvailable(observedAt, evaluatedAt);
        };
    }

    /** 가용한 관측값의 경과 시간이 허용 시간을 초과했는지 판정합니다. */
    private SourceStatus evaluateAvailable(Instant observedAt, Instant evaluatedAt) {
        if (staleAfter == null) {
            // 실제 Source가 연결됐는데 최신성 설정이 없다면 정상값으로 오인하지 않고 즉시 드러냅니다.
            throw new BusinessException(
                    AdminAnalyticsErrorCode.SOURCE_CONTRACT_MISMATCH,
                    "AVAILABLE 분석에는 staleAfter 설정이 필요합니다.");
        }
        Objects.requireNonNull(observedAt, "observedAt");
        Duration age = Duration.between(observedAt, evaluatedAt);
        return age.compareTo(staleAfter) > 0 ? SourceStatus.STALE : SourceStatus.VALID;
    }
}
