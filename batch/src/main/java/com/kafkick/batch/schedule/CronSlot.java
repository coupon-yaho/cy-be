// 크론 표현식의 슬롯을 구합니다.
package com.kafkick.batch.schedule;

import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.scheduling.support.CronExpression;

/**
 * <b>"지금이 어느 발화 슬롯인가" 를 구한다.</b>
 *
 * <p>예전에는 {@code asOf} 를 {@code now().truncatedTo(MINUTES)} 로 만들었다. 그 값이
 * 실행의 신원이자 중복 방지 장치인데, <b>트리거가 실제로 실행된 시각</b>을 자른 것이라
 * 두 가지가 어긋난다.
 *
 * <ul>
 *   <li><b>노드마다 다른 값이 나온다.</b> 크론 슬롯이 {@code 12:05} 인데 한 노드는
 *       {@code 12:05:59}, 다른 노드는 앞 실행이 밀려 {@code 12:06:01} 에 돌면 {@code asOf} 가
 *       {@code 12:05} 와 {@code 12:06} 으로 갈린다. 그러면 서로 <b>다른 JobInstance</b> 라
 *       중복 방지가 아예 발동하지 않는다 — 다중 인스턴스 안전 논증 전체가 그 위에 서 있다.</li>
 *   <li><b>주기를 1분 미만으로 줄이면 절반이 조용히 스킵된다.</b> 30초 주기면 두 발화 중
 *       하나가 같은 분에 들어가 같은 {@code asOf} 가 되고, 그쪽은 INFO 한 줄로 건너뛴다.
 *       처리량은 그대로인데 크론만 두 배가 된다.</li>
 * </ul>
 *
 * <p>슬롯에서 뽑으면 둘 다 사라진다. 늦게 떠도 같은 슬롯이면 같은 값이고, 주기가 짧아지면
 * 슬롯도 그만큼 촘촘해져 서로 다른 값이 된다.
 *
 * <p><b>불규칙한 크론도 있다</b>(예: 평일만, 매월 1일). 그래서 간격을 상수로 가정하지 않고
 * <b>뒤로 넓혀 가며</b> 찾는다. 그래도 못 찾으면 부르는 쪽이 정하도록 {@code null} 을 준다 —
 * 여기서 임의로 대체값을 만들면 그 값이 조용히 실행의 신원이 된다.
 */
final class CronSlot {

    /** 되짚는 횟수 상한. 매번 두 배로 넓히므로 5분 주기에서 약 5개월 전까지 본다. */
    private static final int MAX_WIDENING = 16;

    private final CronExpression expression;

    CronSlot(String cron) {
        this.expression = CronExpression.parse(cron);
    }

    /**
     * {@code now} 이하의 마지막 발화 시각. 없으면 {@code null}.
     *
     * <p>{@link CronExpression} 은 앞으로만 걸을 수 있어서, 충분히 과거에서 시작해 {@code now}
     * 를 넘기 직전까지 걸은 값을 쓴다. 시작점을 얼마나 뒤로 잡을지는 <b>다음 두 발화의 간격</b>
     * 으로 어림잡고, 못 찾으면 그 폭을 두 배씩 넓힌다.
     */
    LocalDateTime atOrBefore(LocalDateTime now) {
        Duration span = estimateSpan(now);
        for (int i = 0; i < MAX_WIDENING; i++) {
            LocalDateTime found = lastFireIn(now.minus(span), now);
            if (found != null) {
                return found;
            }
            span = span.multipliedBy(2);
        }
        return null;
    }

    /** 다음 두 발화의 간격. 불규칙하면 어림값이고, 못 구하면 하루로 시작한다. */
    private Duration estimateSpan(LocalDateTime now) {
        LocalDateTime first = expression.next(now);
        LocalDateTime second = first == null ? null : expression.next(first);
        if (first == null || second == null) {
            return Duration.ofDays(1);
        }
        // 두 배로 잡아 시작한다 — 간격이 한 칸이면 그 앞 발화를 반드시 품는다.
        return Duration.between(first, second).multipliedBy(2);
    }

    private LocalDateTime lastFireIn(LocalDateTime from, LocalDateTime now) {
        LocalDateTime last = null;
        for (LocalDateTime t = expression.next(from); t != null && !t.isAfter(now);
                t = expression.next(t)) {
            last = t;
        }
        return last;
    }
}
