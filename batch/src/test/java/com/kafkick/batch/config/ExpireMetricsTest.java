// 만료 대기 스냅샷의 순서 규칙을 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.expiration.PendingExpiration;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * <b>스냅샷 교체 규칙을 잰다.</b> 게이지 다섯이 한 덩어리로 갈리는지, 세지 못한 것이
 * 0 이 아니라 {@code NaN} 으로 나가는지, 그리고 <b>순서 거절이 없는지</b>다.
 *
 * <p>마지막 축이 이 클래스가 생긴 이유다. 순서 규칙은 기록자가 둘이던 시절의 장치인데,
 * 되읽기 하나가 된 뒤로는 <b>조용한 동결 장치</b>였다 — 거절이 카운터로도 로그로도 안 나가서
 * 게이지가 낡은 값을 든 채 알림 셋이 전부 초록이 된다.
 *
 * <p>컨테이너가 필요 없다. 재는 것은 DB 가 아니라 교체 규칙이다.
 */
class ExpireMetricsTest {

    private static final LocalDateTime EARLIER = LocalDateTime.of(2026, 4, 1, 4, 10);

    private static final LocalDateTime LATER = LocalDateTime.of(2026, 4, 2, 4, 10);

    private MeterRegistry registry;
    private ExpireMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new ExpireMetrics(registry);
    }

    @Test
    @DisplayName("성공한 적이 없으면 0 이 아니라 NaN 이다")
    void unknownBeforeAnyRecord() {
        assertThat(gauge("cy_expire_unexplained_pending"))
                .as("0 은 '밀린 것이 없다' 라 누락 알림을 영원히 침묵시킨다")
                .isNaN();
    }

    @Test
    @DisplayName("더 최신 asOf 의 결과가 이전 값을 덮는다")
    void newerAsOfWins() {
        metrics.record(EARLIER, new PendingExpiration(5, 2), 1);
        metrics.record(LATER, new PendingExpiration(0, 0), 0);

        assertThat(gauge("cy_expire_pending")).isZero();
        assertThat(gauge("cy_expire_measured_at_seconds"))
                .as("어느 시점의 사실인지가 함께 움직여야 한다")
                .isEqualTo(LATER.toEpochSecond(java.time.ZoneOffset.UTC));
    }

    /**
     * <b>순서를 안 따진다 — 되읽기가 읽은 것이 곧 현재다.</b> 한때 <i>"더 오래된
     * {@code asOf} 는 안 받는다"</i> 규칙이 있었는데, 기록자가 되읽기 하나가 된 뒤로는
     * <b>얼어붙는 문</b>이었다: 되읽기가 고르는 것은 <i>7일 창 안에서</i> 가장 나중에 끝난
     * 실행이고 창은 앞이 잘리므로 그 값이 뒤로 갈 수 있는데, 그때 거절이 <b>카운터로도
     * 로그로도 안 나갔다.</b>
     *
     * <p>그 방어가 다시 필요해지는 것은 <b>만료 손 트리거가 생기는 날</b>이다 —
     * 지금은 크론뿐이라 {@code asOf} 와 종료 시각의 순서가 같다.
     */
    @Test
    @DisplayName("더 오래된 asOf 의 결과도 그대로 반영한다 — 거절이 조용한 동결이었다")
    void olderAsOfStillReplaces() {
        metrics.record(LATER, new PendingExpiration(5, 2), 1);
        metrics.record(EARLIER, new PendingExpiration(0, 0), 0);

        assertThat(gauge("cy_expire_pending"))
                .as("거절하면 게이지가 낡은 값을 든 채 알림 셋이 전부 초록이 된다")
                .isZero();
        assertThat(gauge("cy_expire_measured_at_seconds"))
                .isEqualTo(EARLIER.toEpochSecond(java.time.ZoneOffset.UTC));
    }

    /**
     * <b>세지 못한 것과 0 은 다르다.</b> 0 을 내면 <i>"밀린 것이 없다"</i> 가 되어 누락
     * 알림이 영원히 조용해지고, 직전 값을 들고 있으면 관제가 그것을 지금 상태로 읽는다.
     */
    @Test
    @DisplayName("세지 못하면 직전 값을 지우고 다섯 다 NaN 이다")
    void markUnknownClearsEveryGauge() {
        metrics.record(LATER, new PendingExpiration(5, 2), 1);

        metrics.markUnknown();

        assertThat(gauge("cy_expire_pending")).isNaN();
        assertThat(gauge("cy_expire_blocked_pending")).isNaN();
        assertThat(gauge("cy_expire_unexplained_pending")).isNaN();
        assertThat(gauge("cy_expire_blocked_coupons")).isNaN();
        assertThat(gauge("cy_expire_measured_at_seconds"))
                .as("어느 시점의 사실인지도 함께 모른다 — 하나만 살아남으면 섞인 샘플이 된다")
                .isNaN();
    }

    private double gauge(String name) {
        return registry.get(name).gauge().value();
    }

    /**
     * <b>{@code markUnknown} 이 스키마 축까지 지우면 알림이 스스로 꺼진다.</b>
     * {@code ExpireMetricsUnknown} 은 그 게이지로 오염 갈래를 빼는데, 되읽기가 실패할 때마다
     * 축이 {@code NaN} 이 되면 조인이 흔들린다 — 스키마 모양은 프로세스 수명 중 안 바뀌므로
     * 마지막으로 확인한 사실을 들고 있는 것이 맞다.
     */
    @Test
    @DisplayName("recordSchema 는 값을 내고 markUnknown 이 그것까지 지우지는 않는다")
    void schemaGaugeSurvivesUnknown() {
        assertThat(gauge("cy_expire_clean_schema"))
                .as("기록 전에는 모름이다 — 0 으로 태어나면 알림이 처음부터 꺼진 채 산다")
                .isNaN();

        metrics.recordSchema(true);
        assertThat(gauge("cy_expire_clean_schema")).isEqualTo(1.0);

        metrics.recordSchema(false);
        assertThat(gauge("cy_expire_clean_schema"))
                .as("분기가 뒤집히면 오염셋 기동이 상시 warning 이 된다")
                .isZero();

        metrics.recordSchema(true);
        metrics.markUnknown();
        assertThat(gauge("cy_expire_clean_schema"))
                .as("markUnknown 이 이것까지 지우면 ExpireMetricsUnknown 의 조인이 흔들린다")
                .isEqualTo(1.0);
    }

    /**
     * <b>{@code latest} 를 보는 게이지 수를 코드가 답하게 한다.</b> 이 저장소가 반복 결함으로
     * 지목한 것이 <i>"개수 주장과 실제 불일치"</i> 이고, 주석의 <i>"다섯"</i> 이 늘어나는
     * 게이지를 따라가지 못한 채 남는다. 새 게이지를 스냅샷에 얹으면 여기가 먼저 깨진다.
     */
    @Test
    @DisplayName("스냅샷이 미는 게이지는 다섯이다 — 주석의 개수가 이 값을 따라야 한다")
    void snapshotBackedGaugeCount() {
        // **스키마 축을 먼저 채운다.** 안 채우면 그것도 NaN 이라 여섯이 세어지고, 이 테스트가
        // "스냅샷이 미는 게이지 수" 가 아니라 "NaN 인 게이지 수" 를 재게 된다.
        metrics.recordSchema(true);
        metrics.markUnknown();

        long unknown = registry.getMeters().stream()
                .filter(m -> m.getId().getName().startsWith("cy_expire_"))
                .filter(m -> m instanceof Gauge)
                .filter(m -> Double.isNaN(((Gauge) m).value()))
                .count();

        assertThat(unknown)
                .as("cy_expire_clean_schema 는 markUnknown 이 안 건드리므로 여기 안 센다")
                .isEqualTo(5L);
    }
}
