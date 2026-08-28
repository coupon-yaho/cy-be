package com.kafkick.infra.redis.lifecycle;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import com.kafkick.core.observation.CampaignClosedEvent;
import com.kafkick.core.observation.CampaignLifecycleRecorder;

import tools.jackson.databind.ObjectMapper;

public final class RedisCampaignClosedEventSubscriber
        implements MessageListener {

    private static final long LOG_INTERVAL_NANOS =
            TimeUnit.SECONDS.toNanos(10);

    private static final Logger log = LoggerFactory.getLogger(
            RedisCampaignClosedEventSubscriber.class
    );

    private final ObjectMapper objectMapper;
    private final CampaignLifecycleRecorder recorder;
    private final AtomicLong failureCount = new AtomicLong();
    private final AtomicLong nextLogAtNanos = new AtomicLong();

    public RedisCampaignClosedEventSubscriber(
            ObjectMapper objectMapper,
            CampaignLifecycleRecorder recorder
    ) {
        this.objectMapper = Objects.requireNonNull(objectMapper);
        this.recorder = Objects.requireNonNull(recorder);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            CampaignClosedEvent event = objectMapper.readValue(
                    message.getBody(),
                    CampaignClosedEvent.class
            );
            recorder.retireCampaign(
                    event.campaignCouponId(),
                    event.closedAt()
            );
        } catch (Exception exception) {
            logFailure(exception);
        }
    }

    private void logFailure(Exception exception) {
        long count = failureCount.incrementAndGet();
        long now = System.nanoTime();
        long due = nextLogAtNanos.get();
        boolean logDue = now - due >= 0
                && nextLogAtNanos.compareAndSet(
                        due,
                        now + LOG_INTERVAL_NANOS
                );
        if (logDue) {
            log.warn(
                    "캠페인 종료 Redis 수신 처리 실패 누적 {}건. cause={}",
                    count,
                    exception.toString()
            );
        }
    }
}
