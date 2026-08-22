package com.kafkick.infra.mq.attempt;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * 실패 카운터는 <b>발급 경로에서</b> 실행된다. 원인을 로그가 아니라 태그로 남기는 이유이자,
 * 태그 값을 닫힌 집합으로 두는 이유다 — 예외 이름을 그대로 넣으면 카디널리티가 열린다.
 */
class AttemptFailureCounterTest {

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final AttemptFailureCounter counter = new AttemptFailureCounter(meterRegistry);

    @Test
    @DisplayName("실패가 없어도 시계열이 0 으로 보인다 — 없는 것과 0 은 화면에서 다르다")
    void knownReasonsAreRegisteredUpFront() {
        assertThat(meterRegistry.find(AttemptFailureCounter.FAILURE_COUNTER).counters())
                .extracting(meter -> meter.getId().getTag("reason"))
                .containsExactlyInAnyOrder("timeout", "serialization", "other");
    }

    @Test
    @DisplayName("원인을 태그로 가른다 — 직렬화 실패는 코드 결함이고 타임아웃은 아니다")
    void reasonsAreSeparatedByTag() {
        counter.record(new TimeoutException("버퍼 만석"), null);
        counter.record(new SerializationException("직렬화 실패"), null);
        counter.record(new IllegalStateException("그 밖"), null);

        assertThat(count("timeout")).isEqualTo(1.0);
        assertThat(count("serialization")).isEqualTo(1.0);
        assertThat(count("other")).isEqualTo(1.0);
    }

    @Test
    @DisplayName("태그 값은 닫힌 집합이라 예외 종류가 늘어도 시계열이 안 늘어난다")
    void unknownExceptionsCollapseIntoOneSeries() {
        counter.record(new IllegalArgumentException("A"), null);
        counter.record(new UnsupportedOperationException("B"), null);
        counter.record(new NullPointerException("C"), null);

        assertThat(meterRegistry.find(AttemptFailureCounter.FAILURE_COUNTER).counters()).hasSize(3);
        assertThat(count("other")).isEqualTo(3.0);
    }

    /**
     * {@code KafkaTemplate} 이 이미 실패한 future 를 만나면 원인을 spring 의
     * {@code KafkaException} 으로 감싸 다시 던진다. 최상위 타입만 보면 브로커 다운이
     * {@code other} 로 분류되어 원인별로 나눈 의미가 사라진다.
     */
    @Test
    @DisplayName("감싸여 온 예외도 원인 체인을 따라 분류한다")
    void wrappedFailuresAreClassifiedByTheirCause() {
        counter.record(new org.springframework.kafka.KafkaException(
                "Send failed", new TimeoutException("메타데이터 미확보")), null);

        assertThat(count("timeout")).isEqualTo(1.0);
        assertThat(count("other")).isZero();
    }

    private double count(String reason) {
        return meterRegistry.get(AttemptFailureCounter.FAILURE_COUNTER)
                .tag("reason", reason)
                .counter()
                .count();
    }

    /**
     * 메타데이터가 이미 있으면 실패 알림은 호출 스레드가 아니라
     * {@code kafka-producer-network-thread} 에서 온다. 두 경로가 섞여도 합계와 사유별 수가
     * 어긋나면 안 된다 — 이 클래스는 요청 스레드와 네트워크 스레드가 <b>동시에</b> 부른다.
     */
    @Test
    @DisplayName("호출 스레드와 네트워크 스레드가 섞여 들어와도 집계가 어긋나지 않는다")
    void countsAreCorrectWhenFailuresArriveOnAnotherThread() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AttemptFailureCounter counter = new AttemptFailureCounter(registry);
        int threads = 8;
        int perThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    start.await();
                    for (int n = 0; n < perThread; n++) {
                        counter.record(new org.apache.kafka.common.errors.TimeoutException("t"), null);
                    }
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(registry.get("app.kafka.attempt.publish.failures")
                .tag("reason", "timeout").counter().count())
                .as("집계가 스레드에 따라 새면 지표가 실패를 축소해서 보고한다")
                .isEqualTo(threads * perThread);
    }
}
