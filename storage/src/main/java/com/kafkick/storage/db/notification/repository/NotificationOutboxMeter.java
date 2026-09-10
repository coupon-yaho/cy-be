package com.kafkick.storage.db.notification.repository;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import com.kafkick.core.notification.OutboxRetryReason;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.observation.DomainMeterNames;

/**
 * outbox 명령이 되돌려지거나 종착할 때 세는 것들.
 *
 * <h2>왜 세는 곳이 여기 하나인가</h2>
 *
 * <p>첫 판은 릴레이({@code infra:mq})와 어댑터가 <b>각자 셌다.</b> 그러면 릴레이가
 * 셋 중 둘을 세는데, <b>릴레이는 그 쓰기가 먹었는지도 상한을 넘겼는지도 모른다.</b>
 * 그래서 이런 것들이 잘못 세어졌다(리뷰가 셋으로 나눠 짚었다):
 *
 * <ul>
 *   <li><b>0행</b> — 토큰이 안 맞아 아무것도 안 고쳤는데 재시도로 셌다</li>
 *   <li><b>종착</b> — 열 번째 실패는 {@code DEAD} 로 가서 <b>다시 시도되지 않는데</b>
 *       재시도로 셌고, 기다릴 일 없는 지연을 히스토그램에 넣었다</li>
 *   <li><b>사유</b> — 어댑터는 무엇이 실패했는지 몰라 만료 종착까지 발행 실패로 적었다</li>
 * </ul>
 *
 * <p>결과를 아는 곳이 여기뿐이라 세는 것도 여기로 모았다. <b>사유는 포트가 실어 온다</b>
 * ({@link OutboxRetryReason}) — 그것만 부르는 쪽이 안다.
 *
 * <h2>커밋 뒤에 센다</h2>
 *
 * <p>미터는 트랜잭션을 안 탄다. 쓰기 전에 세면 그 트랜잭션이 뒤에 롤백돼도 <b>지표에는
 * 남는다</b> — 만료 회수는 한 트랜잭션에서 여러 행을 고치므로, 뒤쪽 행의 오류가 앞쪽까지
 * 되돌려도 숫자만 그대로다. 그래서 부르는 쪽이 <b>커밋을 확인한 뒤</b> 넘긴다.
 */
@Component
public class NotificationOutboxMeter {

    private final Map<OutboxRetryReason, Counter> retries =
            new EnumMap<>(OutboxRetryReason.class);
    private final Map<OutboxRetryReason, Timer> delays =
            new EnumMap<>(OutboxRetryReason.class);
    private final Map<OutboxRetryReason, Counter> deaths =
            new EnumMap<>(OutboxRetryReason.class);
    private final Map<AttemptTrigger, Counter> claimed =
            new EnumMap<>(AttemptTrigger.class);

    /**
     * <b>모든 사유의 시계열을 미리 만든다.</b> 실패가 나야 생기게 두면 대시보드가
     * <b>0 과 "지표 없음" 을 구분하지 못한다</b> — {@code rate()} 가 둘 다 빈칸으로 그린다.
     *
     * @throws NullPointerException {@code registry} 가 {@code null} 일 때. 안 막으면 첫
     *         실패가 날 때까지 잘못된 배선이 드러나지 않는다
     */
    public NotificationOutboxMeter(MeterRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        for (OutboxRetryReason reason : OutboxRetryReason.values()) {
            retries.put(reason, Counter.builder(DomainMeterNames.OUTBOX_RETRY)
                    .tag(DomainMeterNames.TAG_REASON, reason.tag())
                    .register(registry));
            delays.put(reason, Timer.builder(DomainMeterNames.OUTBOX_RETRY_DELAY)
                    .tag(DomainMeterNames.TAG_REASON, reason.tag())
                    .register(registry));
            deaths.put(reason, Counter.builder(DomainMeterNames.OUTBOX_DEAD)
                    .tag(DomainMeterNames.TAG_REASON, reason.tag())
                    .register(registry));
        }
        for (AttemptTrigger kind : AttemptTrigger.outboxKinds()) {
            claimed.put(kind, Counter.builder(DomainMeterNames.OUTBOX_CLAIMED)
                    .tag(DomainMeterNames.TAG_TRIGGER, kind.name().toLowerCase(Locale.ROOT))
                    .register(registry));
        }
    }

    /**
     * 이 회차에 <b>집힌</b> 명령을 종류별로 센다.
     *
     * <p><b>안 집힌 종류는 안 부른다.</b> 한때 "0 으로라도 불러야 시계열이 안 끊긴다"
     * 고 적었는데 <b>틀렸다</b> — 시계열을 존재하게 만드는 것은 위 생성자의 사전
     * 등록이고, {@code increment(0)} 은 그 위에 0 을 더할 뿐이라 관측되는 변화가 없다.
     *
     * <p><b>커밋된 뒤에 불러야 한다.</b> 선점은 한 트랜잭션이고, 그 안에서 세면 뒤에
     * 롤백돼도 지표에는 집힌 것으로 남는다. 부르는 쪽이 그 순서를 진다.
     *
     * @throws NullPointerException {@code counts} 가 {@code null} 일 때
     * @throws IllegalArgumentException 세는 수가 음수이거나, outbox 행에 올 수 없는
     *         종류가 섞였을 때. 후자를 안 막으면 <b>선점이 못 집는 종류를 셌다는
     *         뜻</b>인데 지표에는 정상으로 보인다
     */
    public void claimed(Map<AttemptTrigger, Integer> counts) {
        Objects.requireNonNull(counts, "counts");
        for (Map.Entry<AttemptTrigger, Integer> entry : counts.entrySet()) {
            Counter counter = claimed.get(entry.getKey());
            if (counter == null) {
                throw new IllegalArgumentException(
                        "outbox 행에 올 수 없는 종류입니다. 받은 값=" + entry.getKey());
            }
            int count = entry.getValue();
            if (count < 0) {
                throw new IllegalArgumentException(
                        "집힌 수는 음수일 수 없습니다. " + entry.getKey() + "=" + count);
            }
            counter.increment(count);
        }
    }

    /**
     * 되돌렸다 — <b>다시 시도될 것</b>이다.
     *
     * <p>{@code delay} 는 <b>계획한</b> 대기이지 잰 시간이 아니다. 분포가 평평하면 지터가
     * 일하는 것이고 뾰족하면 아직 뭉치는 것인데, 뭉침이 가장 잘 나는 것이
     * {@link OutboxRetryReason#LEASE_EXPIRED} 다 — 릴레이가 죽으면 인플라이트 lease 가
     * 한꺼번에 만료된다.
     *
     * @throws NullPointerException 인자가 {@code null} 일 때
     */
    public void retried(OutboxRetryReason reason, Duration delay) {
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(delay, "delay");
        retries.get(reason).increment();
        delays.get(reason).record(delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * 상한을 넘겨 종착했다 — 그 <b>명령</b>은 다시 시도되지 않는다.
     *
     * <p><b>"그만큼의 알림이 영영 안 간다" 로 읽으면 안 된다.</b> 세는 단위가 명령이라,
     * 종착한 알림을 사람이 다시 보내면 새 명령이 생긴다 — 그렇게 읽으면 재발송으로
     * 살아난 것까지 실패로 센다. <b>지금 사람 손이 필요한 건수</b>다.
     *
     * <p><b>여기서는 지연을 안 넣는다.</b> 종착한 건에도 {@code next_attempt_at} 은 적히지만
     * 아무도 그것을 기다리지 않는다. 히스토그램에 넣으면 <b>일어나지 않을 대기</b>가 분포에
     * 섞인다.
     *
     * @throws NullPointerException {@code reason} 이 {@code null} 일 때
     */
    public void dead(OutboxRetryReason reason) {
        Objects.requireNonNull(reason, "reason");
        deaths.get(reason).increment();
    }
}
