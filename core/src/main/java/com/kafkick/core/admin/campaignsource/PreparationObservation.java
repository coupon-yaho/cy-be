package com.kafkick.core.admin.campaignsource;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.observation.SourceStatus;

/**
 * 캠페인 필수 준비 항목의 완료 여부와 DB 관측 상태입니다.
 *
 * <p>완료 여부는 DB에서 확정한 준비 완료 판정을 나타냅니다. 원천이 완료 여부를 제공하지 않으면
 * {@code false}로 바꾸지 않고 값 없는 상태로 보존합니다.</p>
 */
public record PreparationObservation(Boolean completed, SourceStatus status, Instant observedAt) {

    /** 완료 여부를 알 수 없는 상태를 false로 축약하지 않도록 상태 조합을 검증합니다. */
    public PreparationObservation {
        Objects.requireNonNull(status, "status");
        if (status.carriesValue()) {
            if (completed == null || observedAt == null) {
                throw new IllegalArgumentException(status + " 준비 상태에는 completed와 observedAt이 필요합니다.");
            }
        } else if (completed != null || observedAt != null) {
            throw new IllegalArgumentException(status + " 준비 상태의 completed와 observedAt은 null이어야 합니다.");
        }
    }
}
