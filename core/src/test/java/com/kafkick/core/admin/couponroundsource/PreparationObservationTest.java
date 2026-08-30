package com.kafkick.core.admin.couponroundsource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.SourceStatus;

/** 준비 상태가 알 수 없음을 완료·미완료로 축약하지 않는지 검증합니다. */
class PreparationObservationTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-24T03:00:00Z");

    /** DB 원천이 완료 여부를 알 수 없을 때 PENDING을 보존하는지 검증합니다. */
    @Test
    @DisplayName("알 수 없는 준비 상태는 null PENDING null로 보존한다")
    void preservesPendingPreparationWithoutInventingCompletion() {
        PreparationObservation preparation = new PreparationObservation(
                null, List.of(), SourceStatus.PENDING, null);

        assertThat(preparation.completed()).isNull();
        assertThat(preparation.status()).isEqualTo(SourceStatus.PENDING);
        assertThat(preparation.observedAt()).isNull();
    }

    /** 확인된 미완료가 알 수 없는 상태와 구분되는 값 있는 관측인지 검증합니다. */
    @Test
    @DisplayName("확인된 미완료 준비 상태는 false VALID 시각으로 보존한다")
    void preservesConfirmedIncompletePreparation() {
        PreparationObservation preparation = new PreparationObservation(
                false, List.of(PreparationItem.DATABASE_STOCK), SourceStatus.VALID, OBSERVED_AT);

        assertThat(preparation.completed()).isFalse();
        assertThat(preparation.status()).isEqualTo(SourceStatus.VALID);
        assertThat(preparation.observedAt()).isEqualTo(OBSERVED_AT);
    }

    /** 값 있는 상태가 완료 여부·관측 시각을 모두 요구해 unknown을 false로 바꾸지 않는지 검증합니다. */
    @Test
    @DisplayName("값 있는 준비 상태는 완료 여부와 관측 시각을 모두 요구한다")
    void rejectsIncompleteCarryingPreparation() {
        assertThatThrownBy(() -> new PreparationObservation(null, List.of(), SourceStatus.VALID, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PreparationObservation(
                Boolean.FALSE, List.of(PreparationItem.DATABASE_STOCK), SourceStatus.VALID, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PreparationObservation(
                Boolean.FALSE, List.of(PreparationItem.DATABASE_STOCK), SourceStatus.PENDING, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
