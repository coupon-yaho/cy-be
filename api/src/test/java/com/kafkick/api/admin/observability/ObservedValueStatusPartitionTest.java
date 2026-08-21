package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.observation.SourceStatus;

/**
 * 조립기가 기대는 전제를 실제 {@link ObservedValue} 생성자로 확인합니다.
 *
 * <p>{@code PromMetricsAssembler} 는 {@link SourceStatus#carriesValue()} 로 값을 실을지 말지를
 * 정하고, 그 결과를 {@code ObservedValue} 에 넣습니다. 두 분할이 갈라지면 조립기가 <b>런타임에</b>
 * {@code IllegalArgumentException} 을 던져 관제 응답이 통째로 죽습니다.</p>
 *
 * <p><b>이 대조는 api 에서만 할 수 있습니다.</b> {@code SourceStatus} 는 core 에 있고
 * {@code ObservedValue} 는 api 에 있어, core 쪽 테스트에서는 참조할 방법이 없습니다. core 에
 * 같은 이름의 테스트를 두면 기대값이 {@code carriesValue()} 의 분할을 다시 적는 동어반복이 되어
 * 두 곳이 갈라져도 통과합니다 — 실제로 그렇게 썼다가 리뷰에서 잡혔다.</p>
 *
 * <p>{@code ObservedValue} 는 A 소유 계약이라 여기서 바꾸지 않는다. 읽는 쪽이 전제를 고정할 뿐이다.</p>
 */
class ObservedValueStatusPartitionTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-21T00:00:00Z");

    /** 값을 싣는 상태는 값과 관측 시각을 함께 받아야 통과하고, 비면 거부한다. */
    @ParameterizedTest
    @EnumSource(value = SourceStatus.class, names = {"VALID", "WARMING_UP", "STALE", "NO_TRAFFIC"})
    @DisplayName("carriesValue 가 true 인 상태는 ObservedValue 가 값과 시각을 요구한다")
    void valueCarryingStatesRequireBoth(SourceStatus status) {
        assertThatCode(() -> new ObservedValue<>(1L, status, OBSERVED_AT)).doesNotThrowAnyException();
        assertThatThrownBy(() -> new ObservedValue<>(null, status, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObservedValue<>(1L, status, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 값이 없는 상태에 0 이나 조립 시각을 채워 넣으면 거부한다. */
    @ParameterizedTest
    @EnumSource(value = SourceStatus.class, names = {"PENDING", "UNAVAILABLE", "N_A"})
    @DisplayName("carriesValue 가 false 인 상태는 ObservedValue 가 값과 시각을 모두 거부한다")
    void valueAbsentStatesRejectBoth(SourceStatus status) {
        assertThatCode(() -> new ObservedValue<>(null, status, null)).doesNotThrowAnyException();
        assertThatThrownBy(() -> new ObservedValue<>(0L, status, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObservedValue<>(null, status, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * 위 두 목록이 {@code SourceStatus} 전체를 덮는지 본다. 상태가 하나 늘면 어느 목록에도 안 들어가
     * 조용히 검사 대상에서 빠지는데, 그 순간 조립기가 그 상태를 어떻게 다뤄야 하는지 아무도 모른다.
     */
    @ParameterizedTest
    @EnumSource(SourceStatus.class)
    @DisplayName("모든 상태가 두 목록 중 하나에 들어간다")
    void everyStatusIsCovered(SourceStatus status) {
        assertThatCode(() -> {
            if (status.carriesValue()) {
                new ObservedValue<>(1L, status, OBSERVED_AT);
            } else {
                new ObservedValue<>(null, status, null);
            }
        }).doesNotThrowAnyException();
    }
}
