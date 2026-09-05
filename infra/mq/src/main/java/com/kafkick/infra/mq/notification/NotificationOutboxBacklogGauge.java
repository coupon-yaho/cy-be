package com.kafkick.infra.mq.notification;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.scheduling.annotation.Scheduled;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.observation.DomainMeterNames;

/**
 * 백로그를 <b>주기적으로 세어 게이지에 담는다.</b>
 *
 * <h2>왜 게이지 콜백에서 직접 세지 않나</h2>
 *
 * <p>Micrometer 의 게이지 콜백은 <b>스크레이프마다</b> 불린다. 거기서 DB 를 세면
 * 조회 주기를 <b>프로메테우스 설정이 정하게</b> 되고, 스크레이프 대상이 늘거나 간격이
 * 짧아지면 <b>관측이 사고를 키운다</b>. 주기를 우리가 쥐고, 게이지는 마지막으로 센 값을
 * 읽기만 한다.
 *
 * <p>그 대가는 <b>해상도</b>다 — 이 주기보다 짧은 스파이크는 안 보인다. 백로그는 "지금
 * 몇 건 밀렸나" 를 보는 값이라 그 정도면 충분하고, 순간 폭증은 인플라이트와
 * {@code app.outbox.retry} 가 따로 진다.
 *
 * <h2>세는 비용</h2>
 *
 * <p>{@code ix_notification_outbox_pending} 이 {@code status} 를 선두로 갖고 있어
 * <b>그 상태 구간만 읽는다</b>(실측: {@code EXPLAIN} 이 {@code type=ref}).
 * 상태마다 따로 세서 더하는 이유가 그것이다 — {@code IN} 하나로 묶으면 {@code type=index}
 * 가 되어 <b>인덱스를 끝까지 훑고</b>, {@code PUBLISHED}·{@code DEAD} 가 쌓일수록 비싸진다.
 * 자세한 근거는 어댑터의 {@code countBacklog} 에 적었다.
 */
public class NotificationOutboxBacklogGauge {

    /**
     * 마지막으로 센 값. <b>{@code -1} 로 시작한다</b> — 아직 한 번도 못 센 상태와
     * "백로그가 0" 을 가르기 위해서다. 둘을 0 으로 합치면 <b>DB 를 못 읽는 상황이
     * 가장 평온해 보인다.</b>
     */
    private final AtomicLong backlog = new AtomicLong(-1);

    private final NotificationOutboxRepository outboxes;

    public NotificationOutboxBacklogGauge(NotificationOutboxRepository outboxes,
            MeterRegistry registry) {
        this.outboxes = Objects.requireNonNull(outboxes, "outboxes");
        Objects.requireNonNull(registry, "registry");
        Gauge.builder(DomainMeterNames.OUTBOX_BACKLOG, backlog, AtomicLong::doubleValue)
                .register(registry);
    }

    /**
     * 한 주기.
     *
     * <p><b>실패해도 값을 안 바꾼다.</b> 못 셌다고 0 을 넣으면 화면이 "다 나갔다" 로 읽는다 —
     * 직전 값을 유지하면 사람이 <b>값이 멈춘 것</b>을 보고 의심할 수 있다. 그 판정을 돕는
     * 것이 프로메테우스 쪽 stale 검사다.
     *
     * <p>예외를 삼키는 것이 맞는 자리다. 여기서 던지면 스케줄러가 그 작업을 계속 다시
     * 부르고 로그만 쌓이는데, <b>이 값이 늦는 것은 사고가 아니다.</b>
     */
    @Scheduled(fixedDelayString = "${kafka.notification.relay.backlog-refresh-ms:15000}")
    public void refresh() {
        try {
            backlog.set(outboxes.countBacklog());
        } catch (RuntimeException failure) {
            // 직전 값을 유지한다 — 위 설명.
        }
    }

    /** 테스트가 주기를 안 기다리고 재기 위한 자리. */
    long current() {
        return backlog.get();
    }
}
