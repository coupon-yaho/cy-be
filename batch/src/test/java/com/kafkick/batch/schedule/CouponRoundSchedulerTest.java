// 회차 상태 전이가 시각으로만 움직이고, 남의 축은 안 건드리는지 확인합니다.
package com.kafkick.batch.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * <b>전이가 지켜야 하는 것은 "바뀐다" 보다 "안 바뀐다" 쪽이 많다.</b> 이 스케줄러는 발급
 * 경로와 같은 테이블을 쓰므로, 잘못 바꾸면 <b>재고가 0인데 열려 있거나 예정 마감 시각이
 * 소실된다.</b> 그래서 남기는 축을 먼저 잰다.
 *
 * <p><b>스케줄러 자체는 켠다.</b> 형제 테스트들은 {@code batch.scheduling.enabled=false} 로
 * 끄는데, 이 클래스가 재는 것이 바로 그 빈이라 켜야 빈이 뜬다({@code @ConditionalOnProperty}).
 * 대신 크론을 <b>연 1회</b>로 미뤄 테스트가 손으로 부르는 것과 경합하지 않게 한다 —
 * {@code ExpirePendingRefresher} 가 {@code initial-delay} 로 같은 창을 닫는다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        // 이 빈을 재려면 켜야 한다 — @ConditionalOnProperty 가 걸려 있다.
        "batch.scheduling.enabled=true",
        // 크론을 연 1회로 밀어 테스트 중에 실제로 돌지 않게 한다. **그 시각에 도는 CI 는
        // 한 번 발화한다** — 연 1회 1초짜리 창이고, 스케줄러를 끄면 이 클래스가 재려는
        // 축이 사라지므로 그 창을 남긴다. ExpireSchedulerTest 가 같은 판단을 했다.
        //
        // 형제 둘도 함께 민다. 그러지 않으면 04:10·04:30 UTC 를 지나며 도는 CI 에서 진짜
        // 만료·정리가 발화해 공유 컨테이너의 데이터를 지운다 — 무관한 테스트가 그날만
        // 빨개진다. 연 단위 크론은 형제의 SLA 가드에 걸리므로 SLA 도 함께 올린다
        // (이 스케줄러에는 그 가드가 없다 — 마지막 성공 시각 축을 안 쓰기 때문이다).
        "batch.schedule.coupon-open-cron=0 0 0 1 1 *",
        "batch.schedule.expire-cron=0 0 0 1 1 *",
        "batch.schedule.cleanup-cron=0 0 0 1 1 *",
        "batch.metrics.cleanup-sla-seconds=999999999",
        "batch.metrics.expire-sla-seconds=999999999",
        // 검증 크론도 함께 민다(CY-470). 기본값 05:00 UTC 를 그대로 두면
        // 그 시각을 지나며 도는 CI 에서 진짜 검증이 발화해, 공유 컨테이너의
        // asof_state 를 300만 행까지 채우고 다른 테스트의 전제를 바꾼다 —
        // 위 정리 크론을 민 것과 같은 이유다. 연 1회는 SLA 가드에 걸려 SLA 도 올린다.
        "batch.schedule.verify-cron=0 0 0 1 1 *",
        "batch.metrics.verify-sla-seconds=999999999",
        // 되읽기도 이 클래스가 손으로 부른다. 첫 발화를 실행 시간보다 뒤로 미룬다.
        "batch.metrics.coupon-round-initial-delay-ms=600000"
})
@Import({MySqlContainerConfig.class, CouponRoundSchedulerTest.FixedClockConfig.class})
class CouponRoundSchedulerTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2026, 4, 12, 5, 0);

    /**
     * <b>창 판정을 벽시계에 맡기지 않는다.</b> 픽스처는 {@code NOW} 기준 상수로 심는데
     * 스케줄러가 실제 시각을 보면 <b>CI 가 도는 시각에 따라 창 안팎이 뒤집힌다.</b>
     */
    @TestConfiguration
    static class FixedClockConfig {
        @Bean
        @Primary
        Clock fixedClock() {
            return Clock.fixed(NOW.toInstant(ZoneOffset.UTC), ZoneOffset.UTC);
        }
    }

    @Autowired
    private CouponRoundScheduler scheduler;

    @Autowired
    private CouponRoundRepository rounds;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private MeterRegistry registry;

    private VerificationSeed seed;

    @BeforeEach
    void resetRounds() {
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
    }

    @Test
    @DisplayName("open_at 이 지난 SCHEDULED 회차를 연다")
    void opensScheduledRoundsPastOpenAt() {
        long due = seed.round(CouponStatus.SCHEDULED, NOW.minusMinutes(1), NOW.plusDays(1));

        scheduler.transitionRounds();

        assertThat(statusOf(due)).isEqualTo("OPEN");
    }

    /**
     * <b>경계는 포함이다.</b> {@code open_at == now} 를 빼면 1분 크론에서 그 회차가 <b>다음
     * 주기까지</b> 안 열린다 — 시연에서 "정각에 열린다" 가 1분 늦는다.
     */
    @Test
    @DisplayName("open_at 이 정확히 지금이면 그 주기에 연다")
    void opensRoundsExactlyAtOpenAt() {
        long boundary = seed.round(CouponStatus.SCHEDULED, NOW, NOW.plusDays(1));

        scheduler.transitionRounds();

        assertThat(statusOf(boundary)).isEqualTo("OPEN");
    }

    @Test
    @DisplayName("open_at 이 아직 미래면 안 연다")
    void leavesFutureRoundsScheduled() {
        long future = seed.round(CouponStatus.SCHEDULED, NOW.plusMinutes(1), NOW.plusDays(1));

        scheduler.transitionRounds();

        assertThat(statusOf(future))
                .as("미래 회차를 열면 예정보다 일찍 발급이 열린다")
                .isEqualTo("SCHEDULED");
    }

    @Test
    @DisplayName("close_at 이 지난 OPEN 회차를 닫는다")
    void closesOpenRoundsPastCloseAt() {
        long expired = seed.round(CouponStatus.OPEN, NOW.minusDays(2), NOW.minusMinutes(1));

        scheduler.transitionRounds();

        assertThat(statusOf(expired)).isEqualTo("CLOSED");
    }

    /**
     * <b>{@code close_at} 을 갱신하지 않는다.</b> {@code docs/02} F5 가 정한 것이다 —
     * 갱신하면 <i>"언제 닫힐 예정이었나"</i> 가 소실되고, 완판 여부 판정이 근거를 잃는다.
     * {@code SET} 절에 {@code status} 하나만 있는 것이 계약이라 여기서 못 박는다.
     */
    @Test
    @DisplayName("닫을 때 close_at 을 갱신하지 않는다 — 예정 시각이 소실되면 완판 판정이 깨진다")
    void keepsScheduledCloseTimeWhenClosing() {
        LocalDateTime plannedClose = NOW.minusMinutes(30);
        long expired = seed.round(CouponStatus.OPEN, NOW.minusDays(2), plannedClose);

        scheduler.transitionRounds();

        assertThat(closeAtOf(expired))
                .as("실제 마감 시각으로 덮으면 예정값이 사라진다 — docs/02 F5")
                .isEqualTo(plannedClose);
    }

    /**
     * <b>재고를 안 건드린다.</b> 재고를 쓰는 배치는 {@code expireJob} 하나라는 계층 규칙이다.
     * 전이가 재고에 손을 대면 그 규칙이 무너지고, 발급 경로와 경합하는 자리가 하나 는다.
     */
    @Test
    @DisplayName("전이는 coupon_stocks 를 건드리지 않는다")
    void leavesStockUntouched() {
        long due = seed.round(CouponStatus.SCHEDULED, NOW.minusMinutes(1), NOW.plusDays(1));
        LocalDateTime before = stockUpdatedAt(due);

        scheduler.transitionRounds();

        assertThat(stockUpdatedAt(due))
                .as("재고를 쓰는 배치는 expireJob 하나다")
                .isEqualTo(before);
    }

    /**
     * <b>재고 행이 없는 회차는 열지 않는다.</b> 열면 발급 경로가 그 회차에서 죽는다.
     * 안 여는 대신 대기 게이지에 남아 {@code CouponRoundsNotOpening} 으로 올라온다 —
     * <b>조용히 깨는 것보다 보이게 멈추는 쪽</b>을 골랐다.
     */
    @Test
    @DisplayName("재고 행이 없는 회차는 열지 않고 대기로 남긴다")
    void refusesToOpenRoundWithoutStock() {
        long stockless = seed.roundWithoutStock(CouponStatus.SCHEDULED, NOW.minusMinutes(1), NOW.plusDays(1));

        scheduler.transitionRounds();

        assertThat(statusOf(stockless))
                .as("재고 없는 회차를 열면 발급이 그 회차에서 500 으로 죽는다")
                .isEqualTo("SCHEDULED");
        assertThat(rounds.countPending(NOW).blockedByMissingStock())
                .as("안 연 것이 **데이터 축** 게이지에 남아야 한다 — 조용히 멈추면 안 된다")
                .isEqualTo(1);
        assertThat(rounds.countPending(NOW).pendingOpen())
                .as("스케줄러 축에는 안 들어간다. 서버를 봐도 안 사라지는 상태를 서버 채널 "
                        + "critical 로 내보내면 데이터를 고칠 때까지 알림이 상주한다")
                .isZero();
    }

    /**
     * <b>이미 CLOSED 인 회차를 되돌리지 않는다.</b> 재고 소진으로 발급 경로가 먼저 닫은
     * 회차가 그 상태다 — 선착순 쿠폰에서 <b>가장 흔한 마감</b>이고, 되돌리면 재고 0 인 회차가
     * 다시 열려 발급이 통과한다.
     */
    @Test
    @DisplayName("재고 소진으로 이미 닫힌 회차를 되돌리지 않는다")
    void neverReopensClosedRound() {
        // 발급 경로가 close_at 전에 닫은 모양 — open_at 은 지났고 close_at 은 미래다.
        long soldOut = seed.round(CouponStatus.CLOSED, NOW.minusDays(1), NOW.plusDays(1));

        scheduler.transitionRounds();

        assertThat(statusOf(soldOut))
                .as("되돌리면 재고 0 인 회차에 발급이 다시 통과한다")
                .isEqualTo("CLOSED");
    }

    /**
     * <b>한 tick 이 여는 것과 닫는 것을 같은 시각으로 판정한다.</b> 시각을 두 번 읽으면
     * 그 사이에 경계를 넘은 회차가 <b>열리고 바로 닫히는</b> 일이 생긴다.
     */
    @Test
    @DisplayName("한 주기에 열 회차와 닫을 회차를 함께 처리한다")
    void handlesOpenAndCloseInOneTick() {
        long toOpen = seed.round(CouponStatus.SCHEDULED, NOW.minusMinutes(1), NOW.plusDays(1));
        long toClose = seed.round(CouponStatus.OPEN, NOW.minusDays(2), NOW.minusMinutes(1));

        scheduler.transitionRounds();

        assertThat(statusOf(toOpen)).isEqualTo("OPEN");
        assertThat(statusOf(toClose)).isEqualTo("CLOSED");
    }

    /**
     * <b>방금 연 회차를 같은 tick 에 닫지 않는다.</b> 여는 대상을 고른 뒤 닫는 대상을 다시
     * 고르므로, 열기가 만든 {@code OPEN} 이 닫기 후보에 들어올 수 있다 —
     * {@code close_at} 이 미래면 안 들어와야 한다.
     */
    @Test
    @DisplayName("같은 주기에 열린 회차는 close_at 이 미래면 안 닫힌다")
    void doesNotCloseWhatItJustOpened() {
        long due = seed.round(CouponStatus.SCHEDULED, NOW.minusMinutes(1), NOW.plusDays(1));

        scheduler.transitionRounds();

        assertThat(statusOf(due)).isEqualTo("OPEN");
    }

    /**
     * <b>두 번 돌려도 같다.</b> 전이가 조건부라 두 번째 주기는 0행이고, 그것을 실패로 세면
     * <b>정상 주기마다 오류가 보고된다.</b>
     */
    @Test
    @DisplayName("두 번 돌려도 상태가 그대로다 — 조건부라 두 번째는 0행이다")
    void isIdempotentAcrossTicks() {
        long due = seed.round(CouponStatus.SCHEDULED, NOW.minusMinutes(1), NOW.plusDays(1));

        scheduler.transitionRounds();
        scheduler.transitionRounds();

        assertThat(statusOf(due)).isEqualTo("OPEN");
        assertThat(rounds.countPending(NOW).pendingOpen()).isZero();
        assertThat(rounds.countPending(NOW).pendingClose()).isZero();
    }

    /**
     * <b>축을 가른다.</b> 재고 행이 없어 못 여는 회차는 <b>데이터가 틀린 것</b>이고 서버를 봐도
     * 안 사라진다. 스케줄러 축({@code pending_open})에 섞으면 사람이 데이터를 고칠 때까지
     * 서버 채널에 {@code critical} 이 상주한다 — 이 저장소가 <i>"데이터가 틀렸다는 판정과
     * 배치가 일을 안 한다는 판정을 같은 알람으로 묶지 않는다"</i> 를 규칙으로 굳혀 놨다.
     * 대신 <b>합이 맞는지</b>를 함께 잰다 — 어느 쪽도 조용히 사라지면 안 된다.
     */
    @Test
    @DisplayName("재고 없는 회차는 스케줄러 축이 아니라 데이터 축으로 센다")
    void pendingCountIncludesRoundsExcludedFromOpening() {
        seed.roundWithoutStock(CouponStatus.SCHEDULED, NOW.minusMinutes(1), NOW.plusDays(1));
        seed.round(CouponStatus.SCHEDULED, NOW.minusMinutes(1), NOW.plusDays(1));

        assertThat(rounds.roundsToOpen(NOW))
                .as("재고 없는 회차는 여는 목록에서 빠진다")
                .hasSize(1);
        assertThat(rounds.countPending(NOW).pendingOpen())
                .as("스케줄러 축은 **열 수 있는 것**만 센다 — 재고 없는 회차는 데이터 축이다")
                .isEqualTo(1);
        assertThat(rounds.countPending(NOW).blockedByMissingStock())
                .as("빠진 하나는 여기로 간다. 합이 맞아야 어느 쪽도 조용히 사라지지 않는다")
                .isEqualTo(1);
    }

    /**
     * <b>조건부 {@code UPDATE} 의 가드를 포트에서 직접 잰다.</b> 스케줄러 경로로는 이 축에
     * 닿을 수 없다 — 대상 조회가 이미 상태로 걸러서, 가드를 지워도 스케줄러 테스트 전부가
     * 초록이다(돌연변이로 확인했다).
     *
     * <p><b>여는 쪽이 특히 위험하다.</b> {@code AND status = 'SCHEDULED'} 가 없으면
     * {@code open()} 이 <b>재고 소진으로 닫힌 회차를 다시 연다</b> — 재고 0 인 회차에 발급이
     * 통과한다. 조회와 갱신 사이에 발급 경로가 상태를 바꾸는 것이 실제 경합이고,
     * 그것을 이 가드가 진다.
     */
    @Test
    @DisplayName("이미 닫힌 회차에 open() 을 불러도 안 열린다 — 조회와 갱신 사이의 경합을 가드가 진다")
    void openRefusesRoundThatIsNoLongerScheduled() {
        long soldOut = seed.round(CouponStatus.CLOSED, NOW.minusDays(1), NOW.plusDays(1));

        assertThat(rounds.open(soldOut, NOW))
                .as("바뀐 것이 없으면 false 다 — 그것을 오류로 세면 정상 경합마다 오류가 보고된다")
                .isFalse();
        assertThat(statusOf(soldOut))
                .as("가드가 없으면 재고 0 인 회차가 다시 열려 발급이 통과한다")
                .isEqualTo("CLOSED");
    }

    /**
     * <b>닫는 쪽 가드.</b> {@code AND status = 'OPEN'} 이 없으면 아직 열리지도 않은 회차가
     * {@code CLOSED} 로 건너뛴다 — {@code SCHEDULED → CLOSED} 는 전이표에 없는 길이다.
     */
    @Test
    @DisplayName("아직 안 열린 회차에 close() 를 불러도 안 닫힌다 — SCHEDULED→CLOSED 는 없는 전이다")
    void closeRefusesRoundThatIsNotOpenYet() {
        long notYet = seed.round(CouponStatus.SCHEDULED, NOW.plusDays(1), NOW.plusDays(2));

        assertThat(rounds.close(notYet, NOW)).isFalse();
        assertThat(statusOf(notYet))
                .as("가드가 없으면 열린 적 없는 회차가 닫힌 것으로 기록된다")
                .isEqualTo("SCHEDULED");
    }

    /**
     * <b>호출 지점을 고정한다.</b> {@code CouponRoundMetricExposureTest} 는 카운터를 직접 불러
     * <i>미터가 오르는지</i>를 재는데, 그것만으로는 <b>스케줄러가 그 카운터를 부르는지</b>를
     * 못 잡는다 — {@code metrics.tickCompleted()} 한 줄을 지워도 그쪽은 초록이다(돌연변이로
     * 확인했다). 그러면 runbook 갈래 ①({@code increase(ticks[5m]) == 0} 이면 스레드가 막혔다)이
     * 조용히 죽는다.
     *
     * <p><b>전이가 0건인 주기로 잰다.</b> 그게 대부분의 주기이고, <i>"정상과 안 돎 이 로그에서
     * 같다"</i> 는 문제를 이 카운터가 메우는 자리다.
     */
    @Test
    @DisplayName("전이가 0건인 주기에도 스케줄러가 tick 카운터를 올린다")
    void schedulerIncrementsTickCounterEvenWithNothingToDo() {
        double before = tickCount();

        scheduler.transitionRounds();

        assertThat(tickCount())
                .as("스케줄러가 이 카운터를 안 부르면 runbook 의 첫 갈래가 조용히 죽는다")
                .isEqualTo(before + 1);
    }

    /**
     * <b>여는 축과 닫는 축이 독립이라는 계약을 잰다.</b> 한때 대상 조회를 {@code catch} 밖에
     * 뒀는데, 그러면 <b>여는 조회가 실패한 tick 은 닫기를 시도조차 안 했다</b> — 커넥션 풀이
     * 마르는 순간이 정확히 그 경로다. 그 수정을 지키는 것이 지금은 이 테스트뿐이다.
     *
     * <p><b>{@code ROUNDS_TO_OPEN} 만 {@code coupon_stocks} 를 본다</b>(재고 {@code EXISTS}).
     * 그래서 그 테이블만 감추면 <b>여는 조회만</b> 죽는다 — 닫는 조회는 멀쩡하다.
     *
     * <p><b>{@code DROP} 이 아니라 {@code RENAME} 이다.</b> 손으로 다시 만들면 {@code V1} 의 FK 와
     * {@code V5} 의 {@code ck_stock_range} 가 사라져 다음 테스트가 다른 스키마 위에서 돈다.
     *
     * <p>셋을 한 번에 고정한다 — 축 독립 · {@code selectFailed()} 배선 · 예외 비전파.
     */
    @Test
    @DisplayName("여는 조회가 죽어도 닫기는 그 tick 에 돈다 — 축이 독립이라는 계약")
    void closingSurvivesWhenOpenSelectFails() {
        long toClose = seed.round(CouponStatus.OPEN, NOW.minusDays(2), NOW.minusMinutes(1));
        double ticksBefore = tickCount();
        double selectBefore = counter("cy_coupon_round_select_failures_total");

        jdbcClient.sql("RENAME TABLE coupon_stocks TO coupon_stocks_hidden").update();
        try {
            assertThatCode(() -> scheduler.transitionRounds())
                    .as("@Scheduled 밖으로 예외가 나가면 스프링이 로그만 남기고 다음 주기를 "
                            + "잡는다 — 조용히 안 도는 상태가 된다")
                    .doesNotThrowAnyException();

            assertThat(statusOf(toClose))
                    .as("여는 조회가 실패한 tick 이 닫기를 건너뛰면 안 된다 — 전이 종류 단위로 "
                            + "독립이라는 것이 이 클래스의 전제다")
                    .isEqualTo(CouponStatus.CLOSED.name());
            assertThat(counter("cy_coupon_round_select_failures_total"))
                    .as("이 카운터가 안 오르면 runbook 갈래 ②가 평가 불가가 되고, 커넥션 풀 "
                            + "고갈이 UPDATE 실패로 오진된다")
                    .isEqualTo(selectBefore + 1);
            assertThat(tickCount())
                    .as("한 주기를 끝낸 것은 사실이다 — 조회가 죽어도 tick 은 오른다")
                    .isEqualTo(ticksBefore + 1);
        } finally {
            jdbcClient.sql("RENAME TABLE coupon_stocks_hidden TO coupon_stocks").update();
        }
    }

    /**
     * <b>창 가드를 포트에서 직접 잰다.</b> 조회와 갱신에 같은 명제가 두 겹으로 걸려 있어
     * <b>한쪽을 지워도 나머지가 결과를 보존한다</b> — 스케줄러 경로로는 어느 겹도 못 잡는다.
     * 상태 가드에 대해 이미 같은 갈라짐을 만들어 뒀고, 창 가드에는 그 짝이 없었다.
     */
    @Test
    @DisplayName("창을 통째로 지난 회차는 open() 을 직접 불러도 안 열린다 — 조회가 아니라 가드가 진다")
    void openRefusesRoundWhoseWindowAlreadyClosed() {
        long missed = seed.round(CouponStatus.SCHEDULED, NOW.minusDays(2), NOW.minusDays(1));

        assertThat(rounds.roundsToOpen(NOW))
                .as("조회 겹")
                .doesNotContain(missed);
        assertThat(rounds.open(missed, NOW))
                .as("가드 겹 — 열면 마감 시각이 지난 회차에서 발급이 나가고, 그 발급이 "
                        + "300만 건 정합성 검증의 입력을 오염시킨다")
                .isFalse();
        assertThat(statusOf(missed)).isEqualTo(CouponStatus.SCHEDULED.name());
        assertThat(rounds.countPending(NOW).missedWindow())
                .as("안 연 것은 전용 게이지가 진다 — 조용히 멈추면 안 된다")
                .isEqualTo(1);
    }

    private double counter(String name) {
        return registry.get(name).counter().count();
    }

    private double tickCount() {
        return registry.get("cy_coupon_round_ticks_total").counter().count();
    }

    private String statusOf(long couponId) {
        return jdbcClient.sql("SELECT status FROM coupons WHERE id = :id")
                .param("id", couponId).query(String.class).single();
    }

    private LocalDateTime closeAtOf(long couponId) {
        return jdbcClient.sql("SELECT close_at FROM coupons WHERE id = :id")
                .param("id", couponId).query(LocalDateTime.class).single();
    }

    private LocalDateTime stockUpdatedAt(long couponId) {
        return jdbcClient.sql("SELECT updated_at FROM coupon_stocks WHERE coupon_id = :id")
                .param("id", couponId).query(LocalDateTime.class).single();
    }
}
