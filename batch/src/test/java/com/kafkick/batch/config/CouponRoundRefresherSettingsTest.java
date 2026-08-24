// 회차 전이 대기 되읽기의 설정 가드가 기동 시점에 걸리는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>가드는 그 자체가 안전장치라 안 재면 지워져도 아무도 모른다.</b> 둘 다 어겼을 때의 결말이
 * <i>"조용히 이상해진다"</i> 라서 기동 시점에 막는다.
 *
 * <p><b>{@code refresh-ms} 상한의 근거는 {@code CouponRoundPendingRefresher} 에 적었다.</b>
 * 여기 옮겨 적지 않는다 — 한때 <i>"5분을 넘으면 for 5m 창에 샘플이 안 들어와 타이머가 못
 * 찬다"</i> 를 두 곳에 적었는데 그게 <b>틀린 근거</b>였고(스크레이프가 15초마다 당겨 간다),
 * 한 곳만 고쳐서 <b>같은 PR 안에 서로를 부정하는 두 문장</b>이 남았다.
 *
 * <p><b>{@code timeout-ms} 는 999 가 0 초가 되는 축이다.</b> 스프링 트랜잭션 타임아웃이 초
 * 단위라 내림하는데, 0 은 <i>"무제한"</i> 이 아니라 <b>데드라인이 이미 지났음</b>이다 —
 * 첫 문장에서 만료된다. 형제 되읽기 둘이 같은 가드를 갖고 있다.
 *
 * <p><b>컨테이너를 안 띄운다.</b> 이 검사들은 생성자 안에 있고 DB 와 아무 상관이 없다.
 */
class CouponRoundRefresherSettingsTest {

    private static final long VALID_REFRESH = 60_000L;

    private static final long VALID_TIMEOUT = 5_000L;

    @Test
    @DisplayName("coupon-round-refresh-ms 가 하한보다 작으면 기동하지 못한다")
    void rejectTooFrequentRefresh() {
        assertThatThrownBy(() -> refresher(9_999L, VALID_TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.metrics.coupon-round-refresh-ms");
    }

    @Test
    @DisplayName("coupon-round-refresh-ms 가 상한을 넘으면 기동하지 못한다 — 알림이 조용해진다")
    void rejectTooSlowRefresh() {
        assertThatThrownBy(() -> refresher(120_001L, VALID_TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.metrics.coupon-round-refresh-ms");
    }

    @Test
    @DisplayName("경계값 둘은 통과한다 — 안 그러면 상한이 실제로 하나 좁다")
    void acceptsBoundaryRefresh() {
        assertThatCode(() -> refresher(10_000L, VALID_TIMEOUT)).doesNotThrowAnyException();
        assertThatCode(() -> refresher(120_000L, VALID_TIMEOUT)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("coupon-round-timeout-ms 가 1000 의 배수가 아니면 기동하지 못한다")
    void rejectSubSecondTimeout() {
        assertThatThrownBy(() -> refresher(VALID_REFRESH, 5_500L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.metrics.coupon-round-timeout-ms");
        assertThatThrownBy(() -> refresher(VALID_REFRESH, 999L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.metrics.coupon-round-timeout-ms");
        // **통과 경계가 없으면 가드가 한 칸 좁아져도 초록이다.** refresh-ms 쪽은
        // acceptsBoundaryRefresh 가 그 짝을 진다.
        assertThatCode(() -> refresher(VALID_REFRESH, 1_000L))
                .as("1000 은 정확히 1초라 통과해야 한다 — 거절하면 하한이 실제로 2초인 셈이다")
                .doesNotThrowAnyException();
    }

    /**
     * 홀더 넷을 {@code null} 로 준다 — 생성자의 값 검사는 그 앞에 있고, 검사를 통과하는
     * 경로에서는 미터를 등록하므로 레지스트리만 실물이 필요하다.
     */
    private static CouponRoundPendingRefresher refresher(long refreshMillis, long timeoutMillis) {
        return new CouponRoundPendingRefresher(null, null,
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry(),
                null, refreshMillis, timeoutMillis, "true");
    }
}
