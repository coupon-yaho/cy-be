// 회차 전이 지표가 실제 값으로 관제까지 나가는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>이 스케줄러는 배치 메타를 안 남긴다 — 관측 축이 미터뿐이다.</b> 그런데 미터의 <b>값</b>을
 * 읽는 테스트가 없으면 돌연변이 다섯이 전부 살아남는다(실제로 그랬다):
 *
 * <ol>
 *   <li>{@code metrics.tickCompleted()} 삭제 → runbook 갈래 ①이 죽는다</li>
 *   <li>{@code CouponRoundMetrics} 안의 {@code selectFailures.increment()} 삭제</li>
 *   <li>같은 클래스의 {@code transitionFailures.increment(...)} 삭제</li>
 *   <li>{@code latest.set(null)} 제거 → 게이지가 <b>낡은 값으로 얼어붙어</b> 알림 셋이 침묵한다</li>
 *   <li>{@code refreshFailures.increment()} 삭제 → {@code CouponRoundMetricsStale} 이 죽는다</li>
 * </ol>
 *
 * <p>⚠️ <b>이 클래스가 재는 것은 미터 등록·노출·값이지 <i>호출 지점</i>이 아니다.</b>
 * 카운터를 직접 부르므로, 스케줄러 안의 {@code metrics.selectFailed()} 한 줄을 지워도
 * 여기는 초록이다. 호출 지점은 {@code CouponRoundSchedulerTest} 가 진다 —
 * {@code schedulerIncrementsTickCounterEvenWithNothingToDo} 와
 * {@code closingSurvivesWhenOpenSelectFails}.
 *
 * <p><b>{@link BatchMetricExposureTest} 가 못 메운다.</b> 저쪽은 규칙 파일이 부르는 <i>이름</i>이
 * 나가는지만 본다 — {@code NaN} 게이지도 노출 줄로 찍히므로 {@code refresh()} 가 한 번도 안
 * 돌아도 초록이다. {@code ExpireMetricExposureTest} 가 만료 쪽에서 같은 이유로 값을 잰다.
 *
 * <p><b>노출까지 본다.</b> 값을 게이지에 넣는 것과 그것이 {@code /actuator/prometheus} 로
 * 나가는 것은 다른 일이다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // 노출 목록을 여기 복붙하면 실제 설정에서 prometheus 가 빠져도 초록으로 지나간다.
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        // 스케줄러 빈은 안 띄운다 — 이 클래스는 손으로 부른다. 되읽기 빈과 카운터 빈은
        // 조건부가 아니라 그대로 뜬다(그게 CY-446 의 설계다).
        "batch.scheduling.enabled=false",
        // 되읽기가 테스트의 손 호출과 경합하지 않게 첫 발화를 뒤로 민다.
        "batch.metrics.coupon-round-initial-delay-ms=600000"
})
@Import({MySqlContainerConfig.class, CouponRoundMetricExposureTest.FixedClockConfig.class})
class CouponRoundMetricExposureTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2026, 4, 12, 5, 0);

    @TestConfiguration
    static class FixedClockConfig {
        @Bean
        @Primary
        Clock fixedClock() {
            return Clock.fixed(NOW.toInstant(ZoneOffset.UTC), ZoneOffset.UTC);
        }
    }

    @LocalManagementPort
    private int managementPort;

    @Autowired
    private CouponRoundPendingRefresher refresher;

    @Autowired
    private CouponRoundMetrics metrics;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed seed;

    @BeforeEach
    void resetRounds() {
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
    }

    /**
     * <b>넷이 서로 다른 회차를 센다.</b> 하나만 재면 술어가 뒤섞여도 못 잡는다 —
     * 예컨대 {@code blocked_no_stock} 의 {@code NOT EXISTS} 를 {@code EXISTS} 로 바꾸면
     * 두 게이지가 서로의 값을 갖는데, 한쪽만 보면 그것이 정상으로 보인다.
     */
    @Test
    @DisplayName("게이지 넷이 각자 다른 회차를 센다 — 술어가 뒤섞이면 잡힌다")
    void pendingGaugesReflectDatabase() throws Exception {
        seed.round(CouponStatus.SCHEDULED, NOW.minusMinutes(1), NOW.plusDays(1));
        seed.roundWithoutStock(CouponStatus.SCHEDULED, NOW.minusMinutes(1), NOW.plusDays(1));
        seed.round(CouponStatus.SCHEDULED, NOW.minusDays(2), NOW.minusDays(1));
        seed.round(CouponStatus.OPEN, NOW.minusDays(2), NOW.minusMinutes(1));

        refresher.refresh();

        String body = prometheusBody();
        assertThat(metric(body, "cy_coupon_round_pending_open"))
                .as("창 안 + 재고 있음")
                .isEqualTo(1.0);
        assertThat(metric(body, "cy_coupon_round_blocked_no_stock"))
                .as("창 안 + 재고 없음 — 데이터 축이다")
                .isEqualTo(1.0);
        assertThat(metric(body, "cy_coupon_round_missed_window"))
                .as("close_at 도 지났다 — 열면 마감 시각이 지난 회차에서 발급이 나간다")
                .isEqualTo(1.0);
        assertThat(metric(body, "cy_coupon_round_pending_close"))
                .as("OPEN 인데 close_at 이 지났다")
                .isEqualTo(1.0);
    }

    /**
     * <b>실패는 {@code NaN} 으로 나가야 한다.</b> 낡은 값으로 얼어붙으면 대기 감시도
     * {@code NaN} 감시도 그것을 못 본다 — 이 클래스가 막겠다고 적은 상태다.
     */
    @Test
    @DisplayName("되읽기가 실패하면 게이지가 NaN 이 되고 실패 카운터가 오른다")
    void gaugesGoNaNWhenReadbackFails() throws Exception {
        seed.round(CouponStatus.SCHEDULED, NOW.minusMinutes(1), NOW.plusDays(1));
        refresher.refresh();
        assertThat(metric(prometheusBody(), "cy_coupon_round_pending_open")).isEqualTo(1.0);

        double before = metric(prometheusBody(), "cy_coupon_round_refresh_failures_total");
        // **DROP 이 아니라 RENAME 이다.** 손으로 다시 만들면 V1:635 의 FK 와 V5 의
        // ck_stock_range 가 사라져, 이 클래스에 재고 관련 단언이 붙는 날 그 단언이
        // **마이그레이션과 다른 스키마** 위에서 돈다. RENAME 뒤 되돌리면 제약이 그대로다
        // (SHOW CREATE TABLE 로 확인했다). 스텁을 끼우면 배선이 아니라 스텁을 재게 된다.
        jdbcClient.sql("RENAME TABLE coupon_stocks TO coupon_stocks_hidden").update();
        try {
            refresher.refresh();

            String body = prometheusBody();
            assertThat(metric(body, "cy_coupon_round_pending_open"))
                    .as("0 으로 내면 '밀린 것이 없다' 가 되어 감시가 조용히 꺼진다")
                    .isNaN();
            assertThat(metric(body, "cy_coupon_round_pending_close")).isNaN();
            assertThat(metric(body, "cy_coupon_round_refresh_failures_total"))
                    .as("CouponRoundMetricsStale 이 이 카운터의 증분을 본다")
                    .isEqualTo(before + 1);
        } finally {
            jdbcClient.sql("RENAME TABLE coupon_stocks_hidden TO coupon_stocks").update();
        }
    }

    /**
     * <b>전이가 0건이어도 오른다.</b> 그게 이 카운터의 뜻이다 — runbook 갈래 ①
     * ({@code increase(ticks[5m]) == 0} 이면 스레드가 막혔다)이 그것에 기댄다.
     */
    @Test
    @DisplayName("tick 카운터는 전이가 0건이어도 오른다")
    void tickCounterRisesWithNoTransitions() throws Exception {
        double before = metric(prometheusBody(), "cy_coupon_round_ticks_total");

        metrics.tickCompleted();

        assertThat(metric(prometheusBody(), "cy_coupon_round_ticks_total"))
                .as("0 과 '안 돎' 을 가르는 유일한 신호다")
                .isEqualTo(before + 1);
    }

    /**
     * <b>조회 실패와 전이 실패를 가른다.</b> 한 카운터로 묶으면 runbook 의 갈래 ②③ 이
     * 구분되지 않고, 커넥션 풀 고갈(조회가 먼저 죽는다)을 UPDATE 실패로 오진한다.
     */
    @Test
    @DisplayName("조회 실패와 전이 실패가 다른 카운터로 나간다")
    void selectAndTransitionFailuresAreSeparateSeries() throws Exception {
        double selectBefore = metric(prometheusBody(), "cy_coupon_round_select_failures_total");
        double transitionBefore =
                metric(prometheusBody(), "cy_coupon_round_transition_failures_total");

        metrics.selectFailed();
        metrics.transitionsFailed(3);

        String body = prometheusBody();
        assertThat(metric(body, "cy_coupon_round_select_failures_total"))
                .isEqualTo(selectBefore + 1);
        assertThat(metric(body, "cy_coupon_round_transition_failures_total"))
                .as("전이 실패는 회차 수만큼 오른다")
                .isEqualTo(transitionBefore + 3);
    }

    /**
     * <b>스케줄러를 껐어도 카운터 시리즈는 있어야 한다.</b> 없으면
     * {@code increase(ticks[5m]) == 0} 이 빈 벡터가 되어 <b>참도 거짓도 아니게</b> 되고,
     * runbook 의 첫 갈래가 평가 불가가 된다. 이 컨텍스트가 그 상태다
     * ({@code batch.scheduling.enabled=false}).
     */
    @Test
    @DisplayName("스케줄러를 꺼도 카운터 셋과 스위치 게이지가 나간다")
    void countersExistEvenWithSchedulerDisabled() throws Exception {
        String body = prometheusBody();

        assertThat(metric(body, "cy_coupon_round_ticks_total")).isNotNaN();
        assertThat(metric(body, "cy_coupon_round_select_failures_total")).isNotNaN();
        assertThat(metric(body, "cy_coupon_round_transition_failures_total")).isNotNaN();
        assertThat(metric(body, "cy_coupon_round_scheduling_enabled"))
                .as("알림이 이 값으로 끈 구간을 뺀다 — 없으면 그 갈래가 통째로 안 걸린다")
                .isZero();
    }

    private String prometheusBody() throws Exception {
        return ActuatorProbe.get(managementPort, "/actuator/prometheus").body();
    }

    private static double metric(String body, String name) {
        return body.lines()
                .filter(line -> line.startsWith(name + " ") || line.startsWith(name + "{"))
                .map(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        name + " 가 /actuator/prometheus 에 없다. 노출 목록이나 미터 등록을 "
                                + "확인해라. 실제로 나간 cy_coupon_round_* 는 " + body.lines()
                                        .filter(line -> line.startsWith("cy_coupon_round"))
                                        .toList()));
    }
}
