package com.kafkick.infra.mq.config;

import java.time.Clock;

import javax.sql.DataSource;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.event.NotificationRequestedEventPublisher;
import com.kafkick.core.notification.retry.FullJitterBackOff;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.infra.mq.notification.NotificationOutboxBacklogGauge;
import com.kafkick.infra.mq.notification.NotificationOutboxRelay;
import com.kafkick.infra.mq.notification.NotificationRelayProperties;
import com.kafkick.infra.mq.notification.NotificationRelayScheduler;
import com.kafkick.infra.mq.notification.RelayBinlogFormatGuard;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("kafka.enabled")
@EnableScheduling
@EnableConfigurationProperties(NotificationRelayProperties.class)
public class NotificationRelayConfig {
    /**
     * 릴레이 전용 워커 풀.
     *
     * <p><b>전용이어야 한다.</b> 공용 {@code TaskExecutor} 를 쓰면 발행이 느려질 때 그 풀을
     * 쓰는 다른 작업이 함께 굶는다. 이름을 붙이는 것도 같은 이유다 — 스레드 덤프에서
     * 누가 물려 있는지 바로 보인다.
     *
     * <h2>큐 용량이 인플라이트 상한과 같은 이유</h2>
     *
     * <p>릴레이가 {@code maxInFlight} 를 넘겨 넘기지 않으므로 <b>큐가 그 이상 찰 수 없다.</b>
     * 그래도 용량을 적어 두는 것은, 누가 릴레이의 백프레셔를 걷어냈을 때 <b>메모리가 아니라
     * 여기서 거부로 터지게</b> 하기 위해서다. 무한 큐면 그 실수가 OOM 으로 나타난다.
     *
     * <p>거부 정책은 기본값({@code AbortPolicy})을 그대로 둔다. {@code CallerRunsPolicy} 는
     * <b>스케줄러 스레드에서 발행을 돌리게 되어</b> 이 티켓이 막으려는 바로 그 블로킹이 된다.
     *
     * <p><b>{@code destroyMethod} 를 안 적는다.</b> {@link ThreadPoolTaskExecutor} 는 이미
     * {@code DisposableBean} 이라 컨테이너가 {@code destroy()} 를 부르고 그것이
     * {@code shutdown()} 을 부른다 — 첫 판에 {@code destroyMethod = "shutdown"} 을 적었더니
     * <b>두 번 불렸다</b>(실측). {@code shutdown()} 이 멱등이라 증상은 없지만, 적은 것이
     * 실제로 무슨 일을 하는지 모르고 적은 셈이라 뺀다.
     */
    @Bean
    public ThreadPoolTaskExecutor notificationRelayWorkers(NotificationRelayProperties properties) {
        ThreadPoolTaskExecutor workers = new ThreadPoolTaskExecutor();
        workers.setCorePoolSize(properties.getWorkerCount());
        workers.setMaxPoolSize(properties.getWorkerCount());
        workers.setQueueCapacity(properties.getMaxInFlight());
        workers.setThreadNamePrefix("notify-relay-");
        // 배수는 릴레이가 한다(awaitDrain). 풀은 그 뒤에 닫히면 되므로 여기서 또 기다리지
        // 않는다 — 두 벌로 기다리면 종료가 두 배로 늦어진다.
        workers.setWaitForTasksToCompleteOnShutdown(false);
        workers.initialize();
        return workers;
    }

    /**
     * <b>배수는 {@code SmartLifecycle.stop()} 이 한다 — {@code destroyMethod} 가 아니다.</b>
     *
     * <p>{@code ThreadPoolTaskExecutor} 도 {@code SmartLifecycle} 이고 단계가
     * {@code Integer.MAX_VALUE / 2} 라(실측), 기본 단계인 릴레이가 <b>풀보다 먼저</b> 멈춘다.
     * 소멸 콜백은 lifecycle {@code stop} 뒤라서, 첫 판({@code destroyMethod = "close"})은
     * <b>풀이 이미 멈춘 뒤에 배수를 시작했다</b> — 그 사이 스케줄러가 한 회차를 돌면
     * 제출이 거부되고 집어 둔 행이 붕 뜬다.
     */
    @Bean
    public NotificationOutboxRelay notificationOutboxRelay(
            NotificationOutboxRepository outboxes,
            NotificationRepository notifications,
            NotificationRequestedEventPublisher publisher,
            NotificationRelayProperties properties,
            ThreadPoolTaskExecutor notificationRelayWorkers,
            FullJitterBackOff notificationRetryBackOff,
            ObjectProvider<Clock> clocks) {
        return new NotificationOutboxRelay(outboxes, notifications, publisher,
                properties.getLease(),
                properties.getClaimBatchSize(),
                notificationRelayWorkers,
                properties.getMaxInFlight(),
                // **풀 크기와 같은 값이어야 한다.** 여기서 갈리면 lease 검사가 실제보다
                // 얕은 파도를 가정하고 통과시킨다. 그래서 둘 다 같은 속성에서 꺼낸다.
                properties.getWorkerCount(),
                // **여기서 새로 만들지 않는다.** lease 만료 회수 경로(storage 어댑터)가 같은
                // 빈을 주입받으므로, 두 경로가 한 벌을 공유한다는 것이 배선으로 강제된다.
                notificationRetryBackOff,
                clocks.getIfAvailable(Clock::systemUTC));
    }

    /**
     * 인플라이트 게이지.
     *
     * <p><b>"지금 몇 건 물고 있나" 하나로만 읽는다.</b> 백프레셔가 걸렸는지는 여기서 못
     * 읽는다 — 그 판정은 {@code poll()} 이 불린 순간에만 나고 스크레이프는 그 사이 아무
     * 때나 찍힌다. 한가한 것과 막힌 것을 가르는 것은
     * {@link NotificationOutboxBacklogGauge} 와 함께 보는 일이다(CY-913).
     */
    @Bean
    public Gauge notificationRelayInFlightGauge(MeterRegistry registry,
            NotificationOutboxRelay relay) {
        return Gauge.builder(DomainMeterNames.NOTIFY_RELAY_IN_FLIGHT, relay,
                        NotificationOutboxRelay::inFlight)
                .register(registry);
    }

    /**
     * <b>인플라이트 게이지의 짝이다.</b> 인플라이트만으로는 한가한 것과 막힌 것을 구분하지
     * 못한다 — 상한에 붙어 있는데 백로그가 안 줄면 워커가 모자란 것이고, 백로그도 0 이면
     * 그냥 보낼 것이 없는 것이다. CY-906·CY-908 이 "붙인다" 고 적어 두고 안 붙였던 자리다.
     */
    @Bean
    public NotificationOutboxBacklogGauge notificationOutboxBacklogGauge(
            NotificationOutboxRepository outboxes, MeterRegistry registry) {
        return new NotificationOutboxBacklogGauge(outboxes, registry);
    }

    @Bean
    public NotificationRelayScheduler notificationRelayScheduler(NotificationOutboxRelay relay) {
        return new NotificationRelayScheduler(relay);
    }

    /**
     * <b>릴레이가 도는 곳에 둔다.</b> 같은 검사가 {@code batch} 모듈에 있지만 이 릴레이는
     * {@code api} 애플리케이션에서 돌고, {@code api} 는 그 모듈을 의존하지 않는다.
     */
    @Bean
    public RelayBinlogFormatGuard relayBinlogFormatGuard(DataSource dataSource) {
        return new RelayBinlogFormatGuard(dataSource);
    }
}
