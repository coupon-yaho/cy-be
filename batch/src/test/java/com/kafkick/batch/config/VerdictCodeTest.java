// 판정을 숫자로 바꾸는 인코딩이 값 개수에 기대고 있음을 못 박습니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.verification.VerdictType;

/**
 * <b>{@code PASS=0}, {@code FAIL=1} 은 값이 둘뿐이라 성립한다.</b>
 *
 * <p>셋째가 생기면 <b>규칙 파일을 안 고쳐도 알림의 뜻이 바뀐다</b> — 새 값을 2 로 매핑했을
 * 때 식이 {@code > 0} 이면 그것도 FAIL 로 울리고 {@code == 1} 이면 조용해진다. 어느 쪽이든
 * 사람이 모르는 사이에 바뀐다.
 *
 * <p>그래서 값을 더하는 사람이 <b>여기서 먼저 멈춘다.</b> 멈춘 자리에서 알림 식을 함께
 * 정하라는 것이 이 테스트의 전부다.
 */
class VerdictCodeTest {

    @Test
    @DisplayName("판정이 둘뿐이라야 0/1 인코딩이 무손실이다")
    void encodingHoldsOnlyWhileVerdictHasTwoValues() {
        assertThat(VerdictType.values())
                .as("값을 더했다면 알림 식(cy_verification_verdict == 1)의 뜻이 바뀐다. "
                        + "infra/prometheus/rules/batch-alerts.yml 을 함께 고쳐라")
                .hasSize(2);
    }

    @Test
    @DisplayName("PASS 는 0, FAIL 은 1")
    void mapsVerdictToGaugeValue() {
        assertThat(VerdictCode.of(VerdictType.PASS)).isZero();
        assertThat(VerdictCode.of(VerdictType.FAIL)).isEqualTo(1.0);
    }

    /**
     * <b>입력 도메인은 셋이다</b> — {@code verdict} 컬럼이 nullable 이라 닫혔는데 판정이 없는
     * 행이 있을 수 있다. 그것을 {@code FAIL} 로 접으면 가짜 알림이 뜬다.
     */
    @Test
    @DisplayName("판정이 없으면 FAIL 이 아니라 모름이다")
    void mapsMissingVerdictToUnknown() {
        assertThat(VerdictCode.of(null))
                .as("null 을 1 로 접으면 VerificationVerdictFailed 가 가짜로 발화한다")
                .isNaN();
    }
}
