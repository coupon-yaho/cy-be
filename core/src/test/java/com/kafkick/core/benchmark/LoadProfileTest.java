package com.kafkick.core.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.kafkick.core.support.exception.BusinessException;

/** 부하 입력이 400 으로 거부되는지. 500 으로 뭉개지면 값을 고쳐 다시 보내면 되는 상황이 서버 장애로 보고된다. */
class LoadProfileTest {

    /**
     * NaN 은 {@code < 0} 을 통과한다. 통과시키면 적재 시점에 {@code BigDecimal.valueOf(NaN)} 이
     * {@code NumberFormatException} 으로 죽는데, 그때는 회차를 이미 연 뒤다.
     */
    @ParameterizedTest
    @ValueSource(doubles = { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.001 })
    @DisplayName("유한하지 않거나 음수인 유휴 RTT 를 400 으로 거부한다")
    void rejectsNonFiniteOrNegativeRtt(double rtt) {
        assertThatThrownBy(() -> profile(rtt))
                .isInstanceOf(BusinessException.class)
                .satisfies(it -> {
                    BusinessException business = (BusinessException) it;
                    assertThat(business.getErrorCode())
                            .isEqualTo(BenchmarkErrorCode.INVALID_RUN_CONDITION);
                    assertThat(business.getErrorCode().getStatus()).isEqualTo(400);
                });
    }

    @Test
    @DisplayName("RTT 를 안 잰 회차는 정상이다")
    void unmeasuredRttIsAllowed() {
        assertThatCode(() -> profile(null)).doesNotThrowAnyException();
        assertThatCode(() -> profile(0.0)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("도착률과 유지 시간은 0 보다 커야 한다")
    void spikeShapeMustBeReal() {
        assertThatThrownBy(() -> new LoadProfile(0, 5, 60, 10_000, 0.8))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> new LoadProfile(20_000, 0, 60, 10_000, 0.8))
                .isInstanceOf(BusinessException.class);
    }

    private static LoadProfile profile(Double rtt) {
        return new LoadProfile(20_000, 5, 60, 10_000, rtt);
    }
}
