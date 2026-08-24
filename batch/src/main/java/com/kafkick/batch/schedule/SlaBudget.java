// 크론 주기·되읽기·잡 소요가 SLA 예산 안에 드는지 기동 때 검사합니다.
package com.kafkick.batch.schedule;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * <b>세 스케줄러가 같은 부등식을 쓴다.</b> 만료·정리·검증이 각자 자기 판을 들고 있었는데,
 * 항이 하나 빠져 있는 것을 CY-470 리뷰가 잡았다 — 셋에 같은 결함이 있었다.
 *
 * <h2>빠져 있던 항 — 잡 소요</h2>
 *
 * <p>게이지({@code cy_batch_last_success_seconds} · {@code cy_verify_last_success_seconds})가
 * 내는 값은 <b>{@code END_TIME}</b> 이다. 그래서 관제가 보는 나이는
 * <i>"직전 실행이 끝난 뒤 흐른 시간"</i> 이고, <b>이번 실행이 아직 안 끝났으면 그동안 계속
 * 자란다.</b> 즉 실질 나이는 <b>크론 간격 + 이번 실행 소요</b> 까지 간다.
 *
 * <p>그전 검사는 {@code 크론 최대간격 + run-refresh-ms < SLA} 뿐이라 그 항을 안 봤다.
 * 구체적으로 무엇이 나빴나 — 검증 기준으로:
 * <pre>
 * 어제 05:00 시작 → 05:08 종료.  게이지 = 어제 05:08
 * 오늘 05:00 시작.               게이지는 잡이 끝나야 움직인다
 * 오늘 06:08 → 나이 25시간 = SLA. for(5분) 뒤 06:13 에 critical
 * </pre>
 * 잡은 그 시각에 <b>정상적으로 돌고 있다.</b> 그리고 이 알림들은 <b>무시되기 시작하는 것이
 * 가장 나쁜 결말</b>이라, 사고 없이 뜨는 것을 구조적으로 막아야 한다.
 *
 * <h2>왜 {@code RunningTooLong} 임계를 쓰나</h2>
 *
 * <p>그 값이 이미 <i>"정상 상태에서 이보다 오래 걸리면 사람이 본다"</i> 는 선언이다. 같은
 * 값을 예산의 항으로 쓰면 <b>두 알림이 한 축 위에 선다</b> — 먼저 {@code RunningTooLong}
 * (warning)이 뜨고, 그래도 안 끝나면 {@code NotSucceeding}(critical)이 뜬다. 순서가 뒤집히면
 * critical 이 warning 보다 먼저 나가 진단이 거꾸로 간다.
 *
 * <p>⚠️ <b>이 값들은 {@code batch-alerts.yml} 과 손으로 맞춘다.</b> 프로메테우스가 앱 설정을
 * 못 읽는다 — SLA 초 값이 이미 같은 방식으로 두 곳에 있고, 이 상수도 같은 규율을 따른다.
 */
final class SlaBudget {

    /**
     * SLA 예산 검사가 크론을 들여다보는 창.
     *
     * <p><b>연 단위까지 품는다.</b> 좁게 잡으면 드문 크론이 <i>"간격을 못 잼"</i> 으로 빠지는데,
     * 그것을 통과로 접으면 SLA 를 못 맞추는 설정이 조용히 뜨고 거절로 접으면 발화를 막으려
     * 먼 미래 크론을 쓰는 테스트가 통째로 막힌다. <b>측정 가능하게 만들어 판정은 SLA 값이
     * 하게 하는 것</b>이 두 문제를 한 번에 없앤다.
     */
    static final Duration CHECK_HORIZON = Duration.ofDays(400);

    /**
     * <b>세 임계는 설정으로 연다 — 상수가 아니다.</b> 배포마다 다른 값이기 때문이다:
     * 시연용 5분 크론에서는 이 항이 지배적이 되어(300 + 60 + <b>600</b>) SLA 를 961 이상으로
     * 밀어 올린다. 손잡이가 없으면 <b>그 조합을 아예 못 쓴다.</b>
     * {@code expire-sla-seconds} 가 같은 이유로 같은 모양이다.
     *
     * <p>여기 있는 값은 <b>{@code .example} 과 알림 규칙이 함께 쓰는 기본값</b>이고,
     * {@code @Value} 의 폴백이자 이 클래스의 문서다 — 셋을 손으로 맞춘다는 사실은
     * {@code batch-alerts.yml} 이 SLA 초 값에 대해 이미 여러 번 적어 뒀다.
     */
    static final long DEFAULT_EXPIRE_RUNNING_TOO_LONG_SECONDS = 600L;

    static final long DEFAULT_CLEANUP_RUNNING_TOO_LONG_SECONDS = 900L;

    /** 300만에서 실측 472초의 2.5배다(CY-470). 셋 중 가장 큰 것은 검증이 가장 무거워서다. */
    static final long DEFAULT_VERIFY_RUNNING_TOO_LONG_SECONDS = 1200L;

    /**
     * <b>0 이하는 그 항을 없애는 것</b>이라 이 손잡이의 존재 이유가 사라진다. 상한은 SLA 가
     * 대신 진다 — 너무 크면 부등식이 안 서서 기동이 거절되므로 따로 안 막는다.
     */
    static void requirePositive(String key, long runningTooLongSeconds) {
        if (runningTooLongSeconds < 1) {
            throw new IllegalArgumentException(
                    key + " 는 1 이상이어야 합니다. 이 값은 정상 상태의 잡 소요 상한이고, "
                            + "게이지가 END_TIME 이라 그만큼을 SLA 예산에서 빼 둬야 "
                            + "도는 도중에 오탐 알림이 안 납니다. 받은 값="
                            + runningTooLongSeconds);
        }
    }

    private SlaBudget() {
    }

    /**
     * 관제가 볼 수 있는 <b>최악의 나이</b>.
     *
     * @param cronSlot        그 잡의 크론
     * @param now             기준 시각. 크론 최대간격을 재는 출발점이다
     * @param skips           슬롯을 연속으로 건너뛸 수 있는 최대 횟수. 건너뛰기가 없으면 0
     * @param refreshMillis   되읽기 주기 — 게이지가 그만큼 늦게 갱신된다
     * @param runningTooLongSeconds 정상 상태의 소요 상한
     * @return 창 안에 크론이 한 번도 안 돌면 비어 있다 — 그런 주기는 어떤 SLA 도 못 맞춘다
     */
    static java.util.Optional<Duration> worstAge(CronSlot cronSlot, LocalDateTime now,
            int skips, long refreshMillis, long runningTooLongSeconds) {
        return cronSlot.maxGap(now, CHECK_HORIZON)
                .map(gap -> gap.multipliedBy(skips + 1L)
                        .plusMillis(refreshMillis)
                        .plusSeconds(runningTooLongSeconds));
    }
}
