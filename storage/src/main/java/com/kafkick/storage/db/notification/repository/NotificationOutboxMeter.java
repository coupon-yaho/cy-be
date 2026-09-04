package com.kafkick.storage.db.notification.repository;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import com.kafkick.core.observation.DomainMeterNames;

/**
 * 저장소 어댑터 안에서만 일어나는 일을 센다.
 *
 * <h2>왜 릴레이가 못 세나</h2>
 *
 * <p>둘 다 <b>여기 말고는 셀 자리가 없다.</b>
 *
 * <ul>
 *   <li><b>lease 만료 회수</b> — {@code claimBatch} 가 안쪽에서 회수하고 결과를 안 돌려준다.
 *       릴레이는 그 일이 있었다는 사실조차 모른다.</li>
 *   <li><b>{@code DEAD} 전이</b> — {@code failure_count} 를 읽고 상한과 견주는 것이
 *       {@code markFailed} 와 만료 회수 두 곳뿐이고, 둘 다 여기다.</li>
 * </ul>
 *
 * <p>그래서 이 모듈이 처음으로 미터를 갖게 됐다. 이름은 {@code core} 가 소유한다
 * ({@link DomainMeterNames}) — 다른 모듈이 같은 이름으로 다른 태그를 올리므로,
 * 이름이 두 벌이면 그 순간 갈린다.
 */
@Component
public class NotificationOutboxMeter {

    /** 잡고 있던 워커가 lease 안에 못 끝냈다. */
    public static final String LEASE_EXPIRED = "lease_expired";

    /**
     * 발행이 실패해 되돌리다 상한에 닿았다.
     *
     * <p><b>{@code notification_missing} 과 구분하지 못한다.</b> 둘 다 릴레이가
     * {@code markFailed} 로 오고, 어댑터는 그 이유를 안 받는다. 이유까지 나누려면 포트에
     * 사유를 실어야 하는데, <b>종착 자체가 드문 사건</b>이라 그 값어치가 없다고 봤다 —
     * 나눠야 할 이유가 생기면 그때 포트를 넓힌다.
     */
    public static final String PUBLISH_FAILED = "publish_failed";

    private final Counter leaseExpiredRetries;
    private final Timer leaseExpiredDelays;
    private final Counter deadFromPublishFailure;
    private final Counter deadFromLeaseExpiry;

    public NotificationOutboxMeter(MeterRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        leaseExpiredRetries = Counter.builder(DomainMeterNames.OUTBOX_RETRY)
                .tag(DomainMeterNames.TAG_REASON, LEASE_EXPIRED)
                .register(registry);
        leaseExpiredDelays = Timer.builder(DomainMeterNames.OUTBOX_RETRY_DELAY)
                .tag(DomainMeterNames.TAG_REASON, LEASE_EXPIRED)
                .register(registry);
        deadFromPublishFailure = dead(registry, PUBLISH_FAILED);
        deadFromLeaseExpiry = dead(registry, LEASE_EXPIRED);
    }

    private static Counter dead(MeterRegistry registry, String reason) {
        return Counter.builder(DomainMeterNames.OUTBOX_DEAD)
                .tag(DomainMeterNames.TAG_REASON, reason)
                .register(registry);
    }

    /**
     * 만료된 클레임 하나를 되돌렸다.
     *
     * <p>{@code delay} 는 <b>계획한</b> 대기이지 잰 시간이 아니다. 분포가 평평하면 지터가
     * 일하는 것이고 뾰족하면 아직 뭉치는 것인데, <b>뭉침이 가장 잘 나는 것이 이 경로다</b> —
     * 릴레이가 죽으면 인플라이트 lease 가 한꺼번에 만료된다.
     */
    public void leaseExpired(Duration delay) {
        Objects.requireNonNull(delay, "delay");
        leaseExpiredRetries.increment();
        leaseExpiredDelays.record(delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** 상한을 넘겨 종착했다. 0 이 아니면 그만큼의 알림이 <b>영영 안 간다.</b> */
    public void dead(String reason) {
        if (LEASE_EXPIRED.equals(reason)) {
            deadFromLeaseExpiry.increment();
            return;
        }
        if (PUBLISH_FAILED.equals(reason)) {
            deadFromPublishFailure.increment();
            return;
        }
        // 태그 값을 열어 두면 오타 하나가 대시보드에도 알림에도 안 잡히는 시계열을 만든다.
        throw new IllegalArgumentException(
                "모르는 종착 사유입니다. 값은 " + PUBLISH_FAILED + " · " + LEASE_EXPIRED
                        + " 둘로 닫혀 있습니다. 받은 값=" + reason);
    }
}
