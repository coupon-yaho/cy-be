// 크론 표현식의 슬롯을 구합니다.
package com.kafkick.batch.schedule;

import java.time.Duration;
import java.util.Optional;
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
public final class CronSlot {

    /**
     * 되짚는 횟수 상한. 시작 창이 <b>간격 × 2</b> 이고 매번 두 배가 되므로, 5분 주기에서
     * 마지막 시도의 창은 {@code 10분 × 2^11 ≈ 14일} 이다. 주 단위 크론(평일만, 월요일만)까지
     * 품는 폭이고, 그보다 드문 크론은 만료 배치의 주기로 쓸 값이 아니다.
     *
     * <p>더 넓히지 않는 이유는 <b>비용</b>이다. 못 찾을 때마다 처음부터 다시 걷기 때문에
     * 마지막 창의 발화 수만큼 {@code next()} 를 부르는데, 그것이 {@code @Scheduled} 스레드
     * 위에서 돈다 — 풀 크기가 1 이면 그동안 batch 의 모든 스케줄러가 멈춘다.
     */
    private static final int MAX_WIDENING = 11;

    /**
     * <b>슬롯 직전에 깨어난 것을 슬롯 안으로 본다.</b>
     *
     * <p><b>이 값은 잡과의 계약이다.</b> 스케줄러가 이만큼 미래인 {@code asOf} 를 만들 수 있으므로,
     * {@code ExpireJobConfig} 의 미래 {@code asOf} 가드도 같은 폭만큼 열려 있어야 한다.
     * 한쪽만 바뀌면 스케줄러가 만든 값을 잡이 거부한다 — 그래서 여기 한 곳에서만 정의한다.
     *
     * <p>발화 마감은 단조시계({@code System.nanoTime})로 잡히고 {@code now} 는 벽시계로 읽는다.
     * 기다리는 5분 사이에 벽시계가 뒤로 조정되면 <b>깨어난 순간 벽시계가 슬롯보다 이르다.</b>
     * 그대로 두면 직전 슬롯을 돌려주는데, 그 인스턴스는 이미 끝났으므로 그 주기가
     * INFO 한 줄로 사라진다 — 예전 {@code truncatedTo(MINUTES)} 는 이 오차에 관용적이었고
     * 슬롯 방식은 관용도가 0 이다.
     */
    public static final Duration EARLY_FIRE_TOLERANCE = Duration.ofSeconds(2);

    private final CronExpression expression;

    public CronSlot(String cron) {
        this.expression = CronExpression.parse(cron);
    }

    /**
     * 앞으로 {@code horizon} 동안 나타나는 <b>연속 발화 사이의 최대 간격.</b>
     *
     * <p><b>"주기" 라고 안 부르는 이유가 있다.</b> 크론에 주기가 늘 있는 것이 아니다 —
     * {@code 0 0 4 * * MON-FRI} 는 금요일과 월요일 사이가 72시간이라, 연속 두 발화의 차로는
     * 최악을 못 잡는다. 그래서 창을 걷어 <b>가장 넓은 간격</b>을 돌려준다.
     *
     * <p>쓰는 곳은 하나다 — 만료가 검증을 피해 슬롯을 건너뛸 때 생기는 <b>최대 지연</b>이
     * {@code ExpireNotSucceeding} 의 SLA 예산 안에 드는지 기동 때 검사하는 자리다.
     * 그 검사가 없으면 {@code max-expire-skips} 를 2 로만 올려도 정상 상태에서 오탐
     * critical 이 난다.
     */
    public Optional<Duration> maxGap(LocalDateTime from, Duration horizon) {
        LocalDateTime end = from.plus(horizon);
        LocalDateTime first = expression.next(from);
        if (first == null || !first.isBefore(end)) {
            // 창 안에 한 번도 안 돈다. 테스트가 발화를 막으려고 먼 미래 크론
            // (0 0 0 1 1 *)을 주는 것이 이 저장소의 관행이고, 그때 "간격" 은 뜻이 없다.
            // 비어 있음을 그대로 돌려줘 부르는 쪽이 판단하게 한다 —
            // 0 을 돌려주면 "간격이 없다(=즉시 반복)" 와 구분되지 않는다.
            return Optional.empty();
        }

        Duration worst = Duration.between(from, first);
        for (LocalDateTime cursor = first; cursor.isBefore(end); ) {
            LocalDateTime next = expression.next(cursor);
            if (next == null) {
                break;
            }
            Duration gap = Duration.between(cursor, next);
            if (gap.compareTo(worst) > 0) {
                worst = gap;
            }
            cursor = next;
        }
        return Optional.of(worst);
    }

    /**
     * {@code now} 이하의 마지막 발화 시각. 없으면 {@code null}.
     *
     * <p>{@link CronExpression} 은 앞으로만 걸을 수 있어서, 충분히 과거에서 시작해 {@code now}
     * 를 넘기 직전까지 걸은 값을 쓴다. 시작점을 얼마나 뒤로 잡을지는 <b>다음 두 발화의 간격</b>
     * 으로 어림잡고, 못 찾으면 그 폭을 두 배씩 넓힌다.
     */
    public LocalDateTime atOrBefore(LocalDateTime now) {
        LocalDateTime upcoming = expression.next(now);
        if (upcoming != null
                && Duration.between(now, upcoming).compareTo(EARLY_FIRE_TOLERANCE) <= 0) {
            return upcoming;
        }
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
