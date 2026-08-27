package com.kafkick.core.admin.queue;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.observation.SourceStatus;

/** 한 캠페인의 현재·이전 대기 인원과 입장 관측을 상태 및 시각과 함께 보존합니다. */
public record CampaignQueueObservation(
        Long couponId,
        Long currentWaitingCount,
        Long previousWaitingCount,
        Long admittedCount,
        Instant windowStart,
        Instant windowEnd,
        Instant lastAdmissionAt,
        Instant admissionStoppedStartedAt,
        SourceStatus sourceStatus,
        Instant observedAt
) {

    /** 값 유무를 SourceStatus와 일치시키고 대기열 시간·수량 관계를 검증합니다. */
    public CampaignQueueObservation {
        Objects.requireNonNull(couponId, "couponId");
        Objects.requireNonNull(sourceStatus, "sourceStatus");
        if (couponId <= 0L) {
            throw new IllegalArgumentException("couponId는 양수여야 합니다.");
        }
        if (!sourceStatus.carriesValue()) {
            // 값 없는 상태를 0으로 보정하면 아직 미연결인 대기열을 빈 대기열로 오해하게 됩니다.
            if (currentWaitingCount != null || previousWaitingCount != null || admittedCount != null
                    || windowStart != null || windowEnd != null || lastAdmissionAt != null
                    || admissionStoppedStartedAt != null || observedAt != null) {
                throw new IllegalArgumentException("값 없는 원천 상태에는 대기열 값과 시각을 실을 수 없습니다.");
            }
            return;
        }
        if (currentWaitingCount == null || previousWaitingCount == null || admittedCount == null
                || windowStart == null || windowEnd == null || observedAt == null) {
            throw new IllegalArgumentException("값이 있는 원천 상태에는 대기열 값·구간·관측 시각이 필요합니다.");
        }
        if (currentWaitingCount < 0L || previousWaitingCount < 0L || admittedCount < 0L) {
            throw new IllegalArgumentException("대기·입장 수는 음수일 수 없습니다.");
        }
        if (sourceStatus == SourceStatus.NO_TRAFFIC
                && (currentWaitingCount != 0L || admittedCount != 0L)) {
            throw new IllegalArgumentException("NO_TRAFFIC 대기·입장 수는 0이어야 합니다.");
        }
        if (!windowEnd.isAfter(windowStart)) {
            throw new IllegalArgumentException("대기열 관측 구간은 양수여야 합니다.");
        }
        if (windowEnd.isAfter(observedAt)) {
            throw new IllegalArgumentException("관측 구간 종료는 observedAt 이후일 수 없습니다.");
        }
        if (admittedCount > 0L && lastAdmissionAt == null) {
            throw new IllegalArgumentException("입장이 있으면 lastAdmissionAt이 필요합니다.");
        }
        if (lastAdmissionAt != null && lastAdmissionAt.isAfter(observedAt)) {
            throw new IllegalArgumentException("lastAdmissionAt은 observedAt 이후일 수 없습니다.");
        }
        if (admittedCount > 0L && (lastAdmissionAt.isBefore(windowStart)
                || lastAdmissionAt.isAfter(windowEnd))) {
            throw new IllegalArgumentException("입장이 있으면 lastAdmissionAt은 관측 구간 안이어야 합니다.");
        }
        if (admittedCount == 0L && lastAdmissionAt != null
                && !lastAdmissionAt.isBefore(windowStart) && !lastAdmissionAt.isAfter(windowEnd)) {
            throw new IllegalArgumentException("무입장 구간의 lastAdmissionAt은 관측 구간 안에 있을 수 없습니다.");
        }
        if (currentWaitingCount > 0L && admittedCount == 0L && admissionStoppedStartedAt == null) {
            throw new IllegalArgumentException("대기 중 무입장은 admissionStoppedStartedAt이 필요합니다.");
        }
        if (admissionStoppedStartedAt != null && admissionStoppedStartedAt.isAfter(observedAt)) {
            throw new IllegalArgumentException("admissionStoppedStartedAt은 observedAt 이후일 수 없습니다.");
        }
        if (admissionStoppedStartedAt != null) {
            if (currentWaitingCount == 0L || admittedCount != 0L) {
                throw new IllegalArgumentException("admissionStoppedStartedAt은 대기 중 무입장에서만 사용합니다.");
            }
            if (lastAdmissionAt != null && lastAdmissionAt.isAfter(admissionStoppedStartedAt)) {
                throw new IllegalArgumentException("lastAdmissionAt은 admissionStoppedStartedAt 이후일 수 없습니다.");
            }
        }
    }
}
