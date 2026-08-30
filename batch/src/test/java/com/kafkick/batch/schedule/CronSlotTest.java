// 크론 슬롯 계산이 asOf 의 신원을 만든다는 것을 지킵니다.
package com.kafkick.batch.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>이 클래스가 만드는 값이 곧 실행의 신원이다.</b> {@code asOf} 는 잡 파라미터이고,
 * 스프링 배치의 중복 방지가 그 값으로 걸린다 — 두 노드가 다른 값을 만들면 중복 방지가
 * 아예 발동하지 않는다.
 *
 * <p>그런데 처음 넣을 때 <b>단위 테스트가 없었다.</b> 스케줄러를 통한 간접 검증 둘뿐이었고
 * 둘 다 {@code 0 *}{@code /5 * * * *} 정규 크론의 정상 경로만 지났다. javadoc 이 주장하던
 * 두 갈래 — 불규칙 크론을 위한 되짚기, 못 찾았을 때의 {@code null} — 는 한 줄도 안 돌았다.
 * 이 저장소가 반복해서 잡아 온 <i>"주장만 있고 잇는 것이 없는"</i> 자리를 신규 코드에 또 만든 것이다.
 */
class CronSlotTest {

    private static final String EVERY_FIVE_MINUTES = "0 */5 * * * *";

    /**
     * <b>정시 발화가 정상 경로다.</b> 트리거는 슬롯 그 순간에 깨우려고 만들어졌으므로,
     * {@code now} 가 발화 시각과 정확히 같은 경우가 하루 288회 중 대부분이다.
     *
     * <p>경계를 {@code t < now} 로 닫는 돌연변이는 <b>이 케이스에서만</b> 드러난다 —
     * 초가 0 이 아닌 시각으로는 결과가 안 바뀐다. 그러면 매 주기가 직전 슬롯을 가리켜
     * <i>"이미 끝난 asOf"</i> 로 스킵되고, 만료가 통째로 멈추는데 로그는 INFO 뿐이다.
     */
    @Test
    @DisplayName("발화 시각 정각에 물으면 그 슬롯 자신이다")
    void returnsTheSlotItselfWhenNowIsExactlyAFireTime() {
        LocalDateTime fire = LocalDateTime.of(2026, 1, 15, 9, 5, 0);

        assertThat(new CronSlot(EVERY_FIVE_MINUTES).atOrBefore(fire))
                .as("경계를 닫으면 여기가 09:00 이 되어 정시 발화가 전부 직전 슬롯을 가리킨다")
                .isEqualTo(fire);
    }

    @Test
    @DisplayName("슬롯 안에서는 언제 물어도 그 슬롯이다")
    void returnsTheSlotStartForAnyMomentInside() {
        CronSlot slot = new CronSlot(EVERY_FIVE_MINUTES);
        LocalDateTime expected = LocalDateTime.of(2026, 1, 15, 9, 5, 0);

        assertThat(slot.atOrBefore(LocalDateTime.of(2026, 1, 15, 9, 5, 1))).isEqualTo(expected);
        // 09:09:59 는 안 쓴다 — 다음 슬롯과 1초 차이라 조기 발화 관용 폭에 걸려 09:10 이 된다.
        assertThat(slot.atOrBefore(LocalDateTime.of(2026, 1, 15, 9, 9, 30))).isEqualTo(expected);
    }

    /**
     * <b>슬롯 직전에 깨어나도 그 슬롯으로 본다.</b> 발화 마감은 단조시계로 잡히고 {@code now}
     * 는 벽시계라, 기다리는 사이 벽시계가 뒤로 조정되면 몇 밀리초 이르게 깨어난다.
     * 그것을 직전 슬롯으로 내리면 이미 끝난 인스턴스라 그 주기가 통째로 사라진다.
     */
    @Test
    @DisplayName("슬롯 직전에 깨어나도 그 슬롯으로 본다 — 관용 폭 안이면")
    void toleratesWakingUpSlightlyEarly() {
        LocalDateTime fire = LocalDateTime.of(2026, 1, 15, 9, 5, 0);

        assertThat(new CronSlot(EVERY_FIVE_MINUTES).atOrBefore(fire.minusSeconds(1)))
                .as("1초 이르게 깨어난 것을 직전 슬롯(09:00)으로 내리면 그 주기가 INFO 로 사라진다")
                .isEqualTo(fire);
        assertThat(new CronSlot(EVERY_FIVE_MINUTES).atOrBefore(fire.minusSeconds(30)))
                .as("관용 폭을 넘으면 정직하게 직전 슬롯이다 — 아무 미래나 당기면 안 된다")
                .isEqualTo(LocalDateTime.of(2026, 1, 15, 9, 0, 0));
    }

    /**
     * <b>되짚기가 실제로 하는 일.</b> 월요일만 도는 크론을 일요일에 물으면 마지막 발화가
     * 엿새 전이다. 시작 창(간격 × 2 = 10분)으로는 절대 못 닿으므로, 창을 두 배씩 넓히는
     * 그 루프가 없으면 여기가 {@code null} 이 된다.
     */
    @Test
    @DisplayName("불규칙 크론이면 창을 넓혀 지난 발화를 찾는다")
    void widensTheWindowForAnIrregularCron() {
        // 2026-08-16 은 일요일, 직전 월요일은 2026-08-10.
        LocalDateTime sunday = LocalDateTime.of(2026, 8, 16, 12, 0);

        assertThat(new CronSlot("0 */5 * * * MON").atOrBefore(sunday))
                .as("되짚기가 없으면 시작 창 10분 안에 발화가 없어 null 이 된다")
                .isEqualTo(LocalDateTime.of(2026, 8, 10, 23, 55));
    }

    /**
     * <b>못 찾으면 대체값을 만들지 않는다.</b> 여기서 아무 값이나 돌려주면 그것이 조용히
     * 실행의 신원이 되고, 노드마다 다른 값이 나와 중복 방지가 사라진다.
     * 부르는 쪽({@code ExpireScheduler})이 그 주기를 건너뛰도록 {@code null} 을 준다.
     */
    @Test
    @DisplayName("되짚기 폭 안에 발화가 없으면 null 이다")
    void returnsNullRatherThanInventingASlot() {
        // 1월 1일에만 도는 크론을 8월에 묻는다. 되짚기 상한(약 14일)으로는 못 닿는다.
        assertThat(new CronSlot("0 */5 * 1 1 *").atOrBefore(LocalDateTime.of(2026, 8, 21, 12, 0)))
                .isNull();
    }

    /**
     * <b>못 찾는 경우에도 스케줄러 스레드를 오래 붙잡으면 안 된다.</b> 못 찾을 때마다 처음부터
     * 다시 걷기 때문에 상한을 넓히면 {@code next()} 호출이 기하급수로 는다. 풀 크기가 1 이면
     * 그동안 batch 의 모든 스케줄러가 멈춘다.
     */
    @Test
    @DisplayName("못 찾는 경우에도 예산 안에서 끝난다")
    void givesUpWithinABudget() {
        assertTimeoutPreemptively(Duration.ofMillis(500),
                () -> new CronSlot("0 */5 * 1 1 *")
                        .atOrBefore(LocalDateTime.of(2026, 8, 21, 12, 0)));
    }
}
