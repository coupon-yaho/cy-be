package com.kafkick.core.admin.campaignsource;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.observation.SourceStatus;

/**
 * 캠페인 필수 준비 항목의 완료 여부와 DB 관측 상태입니다.
 *
 * <p>P-06 전 DB 어댑터는 반드시 {@code (null, PENDING, null)}만 생성합니다. 알 수 없는 완료
 * 여부를 {@code false}로 바꾸지 않습니다.</p>
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
