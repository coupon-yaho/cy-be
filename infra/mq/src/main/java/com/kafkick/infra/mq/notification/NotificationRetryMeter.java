package com.kafkick.infra.mq.notification;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import com.kafkick.core.observation.DomainMeterNames;

/**
 * 릴레이가 발행 명령을 <b>되돌릴 때</b> 세는 것들.
 *
 * <p>CY-903 이 재시도 타이밍을 고정 1초에서 Full Jitter 로 바꾸고 CY-907 이 만료 회수까지
 * 같은 정책으로 묶었는데, <b>그것이 실제로 흩어지고 있는지 볼 수단이 없었다.</b> 릴레이에는
 * 지표도 로그도 0건이었다.
 *
 * <h2>미터를 미리 만들어 두는 이유</h2>
 *
 * <p>{@code register} 를 생성자에서 한 번만 한다. 실패 경로에서 매번 {@code Counter.builder}
 * 를 부르면 <b>가장 바쁠 때</b> 레지스트리 조회가 늘고, 더 나쁘게는 <b>아직 한 번도 실패하지
 * 않은 인스턴스에는 시계열이 아예 없다.</b> 그러면 대시보드가 0 과 "지표 없음" 을 구분하지
 * 못한다 — {@code rate()} 는 둘 다 빈칸으로 그린다.
 *
 * <p>{@link NotificationResultMeter} 가 같은 이유로 같은 모양이다.
 */
public class NotificationRetryMeter {

    /** 발행이 던졌다. */
    public static final String PUBLISH_FAILED = "publish_failed";

    /** 발행 대상 알림이 사라졌다. */
    public static final String NOTIFICATION_MISSING = "notification_missing";

    private final Counter publishFailed;
    private final Counter notificationMissing;
    private final Timer publishFailedDelay;
    private final Timer notificationMissingDelay;

    public NotificationRetryMeter(MeterRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        publishFailed = retries(registry, PUBLISH_FAILED);
        notificationMissing = retries(registry, NOTIFICATION_MISSING);
        publishFailedDelay = delays(registry, PUBLISH_FAILED);
        notificationMissingDelay = delays(registry, NOTIFICATION_MISSING);
    }

    private static Counter retries(MeterRegistry registry, String reason) {
        return Counter.builder(DomainMeterNames.OUTBOX_RETRY)
                .tag(DomainMeterNames.TAG_REASON, reason)
                .register(registry);
    }

    private static Timer delays(MeterRegistry registry, String reason) {
        return Timer.builder(DomainMeterNames.OUTBOX_RETRY_DELAY)
                .tag(DomainMeterNames.TAG_REASON, reason)
                .register(registry);
    }

    /**
     * 되돌린 것 하나를 센다.
     *
     * <p><b>{@code delay} 는 잰 시간이 아니라 계획한 시간이다.</b> 실제로 그만큼 뒤에
     * 집혔는지는 여기서 모른다 — 그것을 재려면 되돌린 시각과 다시 집힌 시각을 이어야 하고,
     * 그것은 이 클래스가 볼 수 있는 범위 밖이다.
     */
    public void retried(String reason, Duration delay) {
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(delay, "delay");
        if (PUBLISH_FAILED.equals(reason)) {
            publishFailed.increment();
            publishFailedDelay.record(delay.toNanos(), TimeUnit.NANOSECONDS);
            return;
        }
        if (NOTIFICATION_MISSING.equals(reason)) {
            notificationMissing.increment();
            notificationMissingDelay.record(delay.toNanos(), TimeUnit.NANOSECONDS);
            return;
        }
        // **여기서 막는다.** 태그 값을 열어 두면 오타 하나가 새 시계열을 만들고, 그 시계열은
        // 대시보드에도 알림에도 안 잡힌 채 카디널리티만 늘린다.
        throw new IllegalArgumentException(
                "모르는 재시도 사유입니다. 값은 " + PUBLISH_FAILED + " · "
                        + NOTIFICATION_MISSING + " 둘로 닫혀 있습니다. 받은 값=" + reason);
    }
}
