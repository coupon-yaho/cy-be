package com.kafkick.core.coupon.v2.port;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 게이트 데이터의 불변식. 여기서 막지 않으면 <b>스크립트가 정상 코드로 거절</b>하므로
 * 잘못된 설정이 사고가 아니라 "정상 마감" 으로 보인다 — 경보가 뜨지 않는 실패다.
 */
class GateMetaTest {

    private static final long OPEN_AT = 1_700_000_000_000L;
    private static final long CLOSE_AT = OPEN_AT + 3_600_000L;

    @Test
    @DisplayName("창이 뒤집힌 meta 는 만들 수 없다 — 모든 요청이 정상적인 마감으로 나간다")
    void rejectsInvertedWindow() {
        assertThatThrownBy(() -> new GateMeta(GateStatus.OPEN, CLOSE_AT, OPEN_AT, 1, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("같은 시각은 허용한다 — 즉시 마감된 회차는 뒤집힌 설정이 아니다")
    void allowsZeroLengthWindow() {
        assertThat(new GateMeta(GateStatus.OPEN, OPEN_AT, OPEN_AT, 1, 10).openAtEpochMillis())
                .isEqualTo(OPEN_AT);
    }

    @Test
    @DisplayName("음수 gradeMask 는 전 등급을 통과시킨다 — bit.band(-1, x) == x 라 초과 발급 방향이다")
    void rejectsNegativeGradeMask() {
        assertThatThrownBy(() -> new GateMeta(GateStatus.OPEN, OPEN_AT, CLOSE_AT, -1, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("OPEN 이 아닌 값은 전부 닫힌 것으로 읽는다 — 스크립트의 판정과 같은 이분법이다")
    void unknownStatusIsClosed() {
        assertThat(GateStatus.fromWireValue("OPEN")).isEqualTo(GateStatus.OPEN);
        assertThat(GateStatus.fromWireValue("REBUILDING")).isEqualTo(GateStatus.CLOSED);
        assertThat(GateStatus.fromWireValue("")).isEqualTo(GateStatus.CLOSED);
        assertThat(GateStatus.fromWireValue(null)).isEqualTo(GateStatus.CLOSED);
    }
}
