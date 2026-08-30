package com.kafkick.core.admin.couponroundsource;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.observation.SourceStatus;

/**
 * 쿠폰 회차 필수 준비 항목의 완료 여부와 DB 관측 상태입니다.
 *
 * <p>완료 여부와 실패 목록은 DB 원천과 Runtime 설정을 모두 판정한 Core 결과입니다. 원천을
 * 끝까지 확인하지 못하면 {@code false}와 실패 목록으로 바꾸지 않고 값 없는 상태로 보존합니다.</p>
 */
public record PreparationObservation(
        Boolean completed,
        List<PreparationItem> failedItems,
        SourceStatus status,
        Instant observedAt
) {

    /** 완료 여부·확정 실패 목록·원천 상태의 조합과 목록 불변성을 검증합니다. */
    public PreparationObservation {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(failedItems, "failedItems");
        failedItems = List.copyOf(failedItems);
        if (status.carriesValue()) {
            if (completed == null || observedAt == null) {
                throw new IllegalArgumentException(status + " 준비 상태에는 completed와 observedAt이 필요합니다.");
            }
            if (Boolean.TRUE.equals(completed) && !failedItems.isEmpty()) {
                throw new IllegalArgumentException("완료된 준비 상태에는 실패 항목이 없어야 합니다.");
            }
            if (Boolean.FALSE.equals(completed) && failedItems.isEmpty()) {
                throw new IllegalArgumentException("미완료 준비 상태에는 확정 실패 항목이 필요합니다.");
            }
        } else if (completed != null || observedAt != null || !failedItems.isEmpty()) {
            throw new IllegalArgumentException(status + " 준비 상태의 결과와 observedAt은 비어 있어야 합니다.");
        }
    }

    /**
     * 이전 테스트 fixture가 확정한 완료 여부를 실패 목록이 있는 새 계약으로 변환합니다.
     *
     * @param completed 이전 fixture의 완료 여부
     * @param status 이전 fixture의 원천 상태
     * @param observedAt 이전 fixture의 관측 시각
     * @deprecated 새 생산 코드와 fixture는 실패 항목을 명시하는 canonical 생성자를 사용해야 합니다.
     */
    @Deprecated
    public PreparationObservation(Boolean completed, SourceStatus status, Instant observedAt) {
        this(
                completed,
                Boolean.FALSE.equals(completed) ? List.of(PreparationItem.DATABASE_STOCK) : List.of(),
                status,
                observedAt);
    }
}
