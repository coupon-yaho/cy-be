package com.kafkick.infra.mq.attempt;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.ReleaseStage;

import org.junit.jupiter.api.Test;

class StratifiedSamplerTest {

    private static final IssuanceFlowEventFactory FACTORY =
            new IssuanceFlowEventFactory(UUID::randomUUID);

    /**
     * 이 티켓의 인수 조건 그 자체다 — <b>부하 중 화면이 409 로만 채워지지 않는다.</b>
     *
     * <p>재고 소진 구간을 그대로 만든다: 409 가 압도적으로 많고 5xx 는 뒤늦게 한 건 온다.
     * 최소 보장이 없으면 409 가 상한을 먼저 다 써서 그 5xx 가 버려지고, 관리자는 장애가 난
     * 회차에서 <b>정상적인 재고 소진 화면</b>을 본다.
     */
    @Test
    void letsARareServerFailureThroughAfterAFloodOfRejections() {
        StratifiedSampler sampler = sampler(properties(1, 10, 20));

        int admittedRejections = 0;
        for (int i = 0; i < 500; i++) {
            if (sampler.sample(rejected(409, ReasonCode.ALREADY_ISSUED))) {
                admittedRejections++;
            }
        }

        assertThat(admittedRejections)
                .as("409 는 전체 상한까지만 먹는다")
                .isEqualTo(10);
        assertThat(sampler.sample(rejected(503, ReasonCode.TEMPORARILY_UNAVAILABLE)))
                .as("상한을 다 쓴 뒤에도 새 층은 최소 보장을 받는다")
                .isTrue();
    }

    /** 최소 보장이 전체 상한을 이긴다. 그 대가(상한이 하드가 아님)를 명시적으로 고정한다. */
    @Test
    void allowsMinimumsToExceedTheGlobalCap() {
        StratifiedSampler sampler = sampler(properties(3, 4, 20));

        // 층 셋이 각각 최소 3 건 = 9 건. 전체 상한은 4 다.
        int admitted = 0;
        for (int i = 0; i < 3; i++) {
            admitted += sampler.sample(rejected(409, ReasonCode.ALREADY_ISSUED)) ? 1 : 0;
            admitted += sampler.sample(rejected(503, ReasonCode.TEMPORARILY_UNAVAILABLE)) ? 1 : 0;
            admitted += sampler.sample(rejected(403, ReasonCode.GRADE_NOT_ELIGIBLE)) ? 1 : 0;
        }

        assertThat(admitted).isEqualTo(9);
    }

    /** 창은 1 초다. 경계를 넘으면 층 카운터와 전체 카운터가 함께 0 이 된다. */
    @Test
    void resetsBothCountersAtTheSecondBoundary() {
        MovableClock clock = new MovableClock(Instant.parse("2026-08-25T00:00:00Z"));
        StratifiedSampler sampler = new StratifiedSampler(properties(0, 2, 20), clock);

        assertThat(sampler.sample(attempt())).isTrue();
        assertThat(sampler.sample(attempt())).isTrue();
        assertThat(sampler.sample(attempt())).isFalse();

        clock.advanceMillis(1_000);

        assertThat(sampler.sample(attempt())).isTrue();
    }

    /**
     * 층 카운터는 <b>비워야</b> 한다. 값만 0 으로 되돌리면 한 번 나타난 층이 맵에 영원히 남아
     * {@code maxStrata} 가 계속 차 있고, 나중에 나타난 진짜 장애 층이 최소 보장을 못 받는다.
     */
    @Test
    void forgetsStrataThatStoppedAppearing() {
        MovableClock clock = new MovableClock(Instant.parse("2026-08-25T00:00:00Z"));
        StratifiedSampler sampler = new StratifiedSampler(properties(1, 1, 2), clock);

        assertThat(sampler.sample(rejected(409, ReasonCode.ALREADY_ISSUED))).isTrue();
        assertThat(sampler.sample(rejected(403, ReasonCode.GRADE_NOT_ELIGIBLE))).isTrue();
        // 층 상한 2 를 다 썼다. 세 번째 층은 최소 보장을 못 받고, 전체 상한도 이미 찼다.
        assertThat(sampler.sample(rejected(503, ReasonCode.TEMPORARILY_UNAVAILABLE))).isFalse();

        clock.advanceMillis(1_000);

        // 새 창에서 409 하나가 전체 상한(1)을 다시 채운다. 이제 503 이 들어오는 유일한 길은
        // 최소 보장인데, 그 길은 층 자리가 비어 있어야 열린다.
        //
        // ⚠️ 이 한 줄이 없으면 테스트가 clear() 와 "값만 0 으로" 를 구분하지 못한다 — 값만
        //    되돌린 구현도 초록이었다(일부러 바꿔 돌려서 확인했다). 값만 되돌리면 층 자리는
        //    계속 차 있고, 새 층은 최소 보장 대신 전체 상한만 적용받아 여기서 떨어진다.
        assertThat(sampler.sample(rejected(409, ReasonCode.ALREADY_ISSUED))).isTrue();

        assertThat(sampler.sample(rejected(503, ReasonCode.TEMPORARILY_UNAVAILABLE)))
                .as("다음 창에서는 앞선 층이 자리를 비워 줘야 새 층이 최소 보장을 받는다")
                .isTrue();
    }

    /** {@code httpStatus} 가 없는 두 단계 이벤트도 각자 하나의 층이다. */
    @Test
    void treatsStageEventsWithoutHttpStatusAsTheirOwnStrata() {
        StratifiedSampler sampler = sampler(properties(1, 1, 20));

        assertThat(sampler.sample(attempt())).isTrue();
        assertThat(sampler.sample(admitted())).as("ISSUE_ATTEMPT 와 다른 층이다").isTrue();
    }

    /**
     * <b>리스너 스레드 셋이 이 인스턴스 하나를 공유한다.</b> 그 성질을 검증하는 테스트가 없었다.
     *
     * <p>{@code AttemptConsumerConfig} 는 {@code concurrency = 3} 이고 샘플러는 인스턴스당
     * 싱글턴이다. 그런데 단위 테스트는 전부 단일 스레드였고 실제 브로커 통합 테스트는 컨테이너
     * concurrency 를 1 로 낮춘다 — {@code sample()} 의 동기화가 이 클래스의 유일한 정확성
     * 근거인데 그것을 확인하는 테스트가 하나도 없었다.
     *
     * <p>지금 코드는 안전하다. 이 테스트가 막는 것은 <b>다음 변경</b>이다 — 잠금을 좁히거나
     * {@code HashMap} 을 그대로 둔 채 {@code synchronized} 를 떼면, 동시 {@code put} 이
     * resize 경합으로 무한 루프에 빠진다. 그건 컨슈머 스레드 하나가 100% CPU 로 멈추는 형태라
     * 리밸런싱만 반복되고 원인 로그가 없다.
     *
     * <p>시계를 고정한다. 창이 넘어가면 카운터가 리셋돼 기대값이 흔들리고, 그러면 이 테스트가
     * 동시성이 아니라 타이밍을 재게 된다.
     */
    @Test
    void keepsTheGlobalCapExactUnderConcurrentListenerThreads() throws Exception {
        StratifiedSampler sampler = sampler(properties(0, 100, 20));
        int threads = 8;
        int perThread = 500;
        AtomicInteger admitted = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    start.await();
                    for (int n = 0; n < perThread; n++) {
                        if (sampler.sample(attempt())) {
                            admitted.incrementAndGet();
                        }
                    }
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(admitted.get())
                .as("상한이 스레드에 따라 새면 Redis 쓰기 예산이 예고 없이 배수가 된다")
                .isEqualTo(100);
    }

    /** 층별 최소 보장도 동시에 들어와도 정확해야 한다. 층이 여럿일 때 맵 경합이 드러난다. */
    @Test
    void keepsPerStratumMinimumsExactUnderConcurrentListenerThreads() throws Exception {
        // 전체 상한 0 에 가깝게 두어 통과가 오직 층별 최소 보장으로만 일어나게 한다.
        StratifiedSampler sampler = sampler(properties(10, 1, 20));
        ReasonCode[] reasons = {
                ReasonCode.ALREADY_ISSUED, ReasonCode.GRADE_NOT_ELIGIBLE,
                ReasonCode.STOCK_EXHAUSTED, ReasonCode.TEMPORARILY_UNAVAILABLE};
        int[] statuses = {409, 403, 409, 503};
        AtomicInteger admitted = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch start = new CountDownLatch(1);
            for (int i = 0; i < 8; i++) {
                int lane = i % reasons.length;
                pool.submit(() -> {
                    start.await();
                    for (int n = 0; n < 300; n++) {
                        if (sampler.sample(rejected(statuses[lane], reasons[lane]))) {
                            admitted.incrementAndGet();
                        }
                    }
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        // 층은 (eventType, httpStatus) 다 — 409 짜리 둘은 같은 층이라 층은 셋이다.
        // 층 셋 × 최소 10 = 30, 거기에 전체 상한 1 이 더해질 자리는 이미 최소 보장이 다 썼다.
        assertThat(admitted.get()).isEqualTo(30);
    }

    private static StratifiedSampler sampler(AttemptSamplingProperties properties) {
        return new StratifiedSampler(properties,
                Clock.fixed(Instant.parse("2026-08-25T00:00:00Z"), ZoneOffset.UTC));
    }

    private static AttemptSamplingProperties properties(int min, int max, int strata) {
        return new AttemptSamplingProperties(min, max, strata);
    }

    private static IssuanceFlowEvent rejected(int httpStatus, ReasonCode reasonCode) {
        return FACTORY.issueRejected(context(), httpStatus, reasonCode, Dependency.NONE);
    }

    private static IssuanceFlowEvent attempt() {
        return FACTORY.issueAttempt(context());
    }

    private static IssuanceFlowEvent admitted() {
        return FACTORY.admitted(context(), 7L);
    }

    private static IssuanceFlowEvent.Ctx context() {
        return new IssuanceFlowEvent.Ctx("request-1", 101L, 201L, Grade.GOLD, false,
                Instant.parse("2026-08-25T00:00:00Z"), EngineVersion.V3, ReleaseStage.V3,
                QueueMode.ADAPTIVE, null, "api-1");
    }

    private static final class MovableClock extends Clock {

        private Instant instant;

        private MovableClock(Instant instant) {
            this.instant = instant;
        }

        private void advanceMillis(long millis) {
            instant = instant.plusMillis(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
