// SLA 예산 부등식의 항이 실제로 다 들어가는지 확인합니다.
package com.kafkick.batch.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>세 스케줄러 테스트로는 이 축을 못 잰다.</b> 전부 기본값(일 1회 86,400 · 되읽기 60 ·
 * SLA 90,000)만 쓰는데, {@code SlaBudget.worstAge} 에서 <b>잡 소요 항을 통째로 지워도</b>
 * 86,460 과 87,660 이 둘 다 90,000 아래라 <b>전부 그대로 초록이다</b> — 경계값을 쓰는
 * 테스트가 하나도 없다는 것을 확인하고 이 클래스를 만들었다(CY-470 리뷰 2회차).
 *
 * <p><b>왜 그 항이 필요했나.</b> 게이지({@code cy_batch_last_success_seconds} ·
 * {@code cy_verify_last_success_seconds})가 내는 값은 {@code END_TIME} 이다. 이번 실행이
 * 아직 안 끝났으면 그동안 나이가 계속 자라므로, 관제가 볼 수 있는 최악의 나이는
 * <b>크론 간격 + 되읽기 + 이번 실행 소요</b>까지 간다. 그 항이 빠져 있어서
 * <i>"어제보다 오래 걸린 정상 실행"</i> 이 도는 도중에 critical 이 뜰 수 있었다.
 *
 * <p>여기서 재는 것은 <b>부등식 자체</b>다 — 그 항이 정말 더해지는가, 그리고 그 항 때문에
 * 실제로 거절되는 경계가 있는가. 기본값 조합이 통과한다는 것은 각 스케줄러 테스트가 잰다.
 */
class SlaBudgetTest {

    /** 일 1회 크론. 최대 간격이 86,400초다. */
    private static final String DAILY_CRON = "0 0 5 * * *";

    private static final LocalDateTime NOW = LocalDateTime.of(2026, 4, 1, 6, 0);

    private static final long REFRESH_MILLIS = 60_000L;

    /**
     * <b>이 단언이 항의 존재 자체를 잡는다.</b> {@code worstAge} 에서
     * {@code plusSeconds(runningTooLongSeconds)} 를 지우면 두 값이 같아져 빨개진다 —
     * 세 스케줄러 테스트는 그 변경을 하나도 못 본다.
     */
    @Test
    @DisplayName("잡 소요 항이 그대로 예산에 더해진다")
    void addsTheRunDurationTerm() {
        CronSlot slot = new CronSlot(DAILY_CRON);

        Duration without = SlaBudget.worstAge(slot, NOW, 0, REFRESH_MILLIS, 0).orElseThrow();
        Duration with = SlaBudget.worstAge(slot, NOW, 0, REFRESH_MILLIS,
                SlaBudget.DEFAULT_VERIFY_RUNNING_TOO_LONG_SECONDS).orElseThrow();

        assertThat(with.minus(without))
                .as("게이지가 END_TIME 이라, 잡이 도는 동안 자라는 나이만큼 예산이 더 필요하다")
                .isEqualTo(Duration.ofSeconds(SlaBudget.DEFAULT_VERIFY_RUNNING_TOO_LONG_SECONDS));
        assertThat(with.toSeconds())
                .as("일 1회(86,400) + 되읽기(60) + VerifyRunningTooLong(1,200)")
                .isEqualTo(87_660L);
    }

    /**
     * <b>건너뛰기는 크론 간격에만 곱해진다 — 소요 항에는 안 곱해진다.</b> 슬롯을 건너뛰는 것은
     * <i>"그 슬롯에 아예 안 돈다"</i> 이지 <i>"그만큼 더 오래 돈다"</i> 가 아니다.
     * 곱셈 자리를 잘못 옮기면 만료 예산이 소요 항만큼 더 부풀어, 성립하는 설정이 거절된다.
     */
    @Test
    @DisplayName("건너뛰기는 크론 간격에만 곱해진다")
    void multipliesOnlyTheCronGap() {
        CronSlot slot = new CronSlot(DAILY_CRON);

        Duration noSkip = SlaBudget.worstAge(slot, NOW, 0, REFRESH_MILLIS,
                SlaBudget.DEFAULT_EXPIRE_RUNNING_TOO_LONG_SECONDS).orElseThrow();
        Duration oneSkip = SlaBudget.worstAge(slot, NOW, 1, REFRESH_MILLIS,
                SlaBudget.DEFAULT_EXPIRE_RUNNING_TOO_LONG_SECONDS).orElseThrow();

        assertThat(oneSkip.minus(noSkip))
                .as("늘어나는 것은 크론 간격 하나뿐이어야 한다")
                .isEqualTo(Duration.ofSeconds(86_400L));
    }

    /**
     * <b>그 항이 실제로 판정을 뒤집는 구간이 있다.</b> 87,000대는 항이 없으면 통과하고
     * 있으면 거절되는 자리다 — 세 스케줄러 테스트에 이 구간을 쓰는 것이 하나도 없었다.
     */
    @Test
    @DisplayName("소요 항이 없으면 통과하고 있으면 거절되는 구간이 실재한다")
    void theTermFlipsTheVerdictInABand() {
        CronSlot slot = new CronSlot(DAILY_CRON);
        long sla = 87_500L;

        assertThat(SlaBudget.worstAge(slot, NOW, 0, REFRESH_MILLIS, 0)
                .orElseThrow().toSeconds())
                .as("항이 없으면 86,460 이라 이 SLA 를 통과한다 — 그 상태가 결함이었다")
                .isLessThan(sla);
        assertThat(SlaBudget.worstAge(slot, NOW, 0, REFRESH_MILLIS,
                        SlaBudget.DEFAULT_VERIFY_RUNNING_TOO_LONG_SECONDS)
                .orElseThrow().toSeconds())
                .as("항이 있으면 87,660 이라 거절된다")
                .isGreaterThanOrEqualTo(sla);
    }

    /**
     * <b>창 안에 한 번도 안 도는 크론은 통과가 아니라 빈 값이다.</b> 호출부가 그것을 거절로
     * 옮긴다 — 여기서 {@code Duration.ZERO} 로 접으면 그런 설정이 조용히 뜨고, 알림은
     * 배포 직후부터 영구히 운다.
     */
    @Test
    @DisplayName("창 안에 한 번도 안 도는 크론은 값을 못 낸다")
    void cannotMeasureACronThatNeverFiresInTheHorizon() {
        // 2월 30일. 문법은 맞고 영원히 안 돈다.
        CronSlot never = new CronSlot("0 0 5 30 2 *");

        assertThat(SlaBudget.worstAge(never, NOW, 0, REFRESH_MILLIS,
                SlaBudget.DEFAULT_VERIFY_RUNNING_TOO_LONG_SECONDS)).isEmpty();
    }

    /**
     * <b>세 임계는 {@code batch-alerts.yml} 과 손으로 맞춘 값이다.</b> 프로메테우스가 앱
     * 설정을 못 읽어서인데, 그 규율이 지켜지는지는 사람이 볼 수밖에 없다 — 최소한
     * <b>세 값이 서로 다르고 순서가 뒤집히지 않았다</b>는 것은 여기서 잠근다.
     *
     * <p>순서가 뒤집히면 무거운 잡이 더 짧은 소요 예산을 갖게 되어, 그 잡만 정상 상태에서
     * 먼저 critical 이 난다. 검증(실측 472초)이 가장 무겁고 만료가 가장 가볍다.
     */
    @Test
    @DisplayName("소요 상한이 잡의 무게 순서와 같다 — 만료 < 정리 < 검증")
    void thresholdsFollowJobWeight() {
        assertThat(SlaBudget.DEFAULT_EXPIRE_RUNNING_TOO_LONG_SECONDS)
                .isLessThan(SlaBudget.DEFAULT_CLEANUP_RUNNING_TOO_LONG_SECONDS);
        assertThat(SlaBudget.DEFAULT_CLEANUP_RUNNING_TOO_LONG_SECONDS)
                .isLessThan(SlaBudget.DEFAULT_VERIFY_RUNNING_TOO_LONG_SECONDS);
    }
}
