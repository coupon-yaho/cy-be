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
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.infra.mq.notification.FullJitterBackOff;
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
     * <b>{@code destroyMethod = "close"} 가 배수를 부른다.</b> 안 부르면 인플라이트가
     * {@code IN_PROGRESS} 로 남아 lease 만료까지 아무도 못 집는다.
     */
    @Bean(destroyMethod = "close")
    public NotificationOutboxRelay notificationOutboxRelay(
            NotificationOutboxRepository outboxes,
            NotificationRepository notifications,
            NotificationRequestedEventPublisher publisher,
            NotificationRelayProperties properties,
            ThreadPoolTaskExecutor notificationRelayWorkers,
            ObjectProvider<Clock> clocks) {
        return new NotificationOutboxRelay(outboxes, notifications, publisher,
                properties.getLease(),
                properties.getClaimBatchSize(),
                notificationRelayWorkers,
                properties.getMaxInFlight(),
                // **풀 크기와 같은 값이어야 한다.** 여기서 갈리면 lease 검사가 실제보다
                // 얕은 파도를 가정하고 통과시킨다. 그래서 둘 다 같은 속성에서 꺼낸다.
                properties.getWorkerCount(),
                new FullJitterBackOff(properties.getBackoffBase(), properties.getBackoffCap()),
                clocks.getIfAvailable(Clock::systemUTC));
    }

    /**
     * 인플라이트 게이지.
     *
     * <p>이 값만으로는 <b>한가한 것과 막힌 것을 구분하지 못한다</b> — 상한에 붙어 있는데
     * 백로그가 안 줄면 워커가 모자란 것이고, 백로그도 0 이면 그냥 보낼 것이 없는 것이다.
     * 백로그 쪽 지표는 CY-908(#197)이 붙인다.
     */
    @Bean
    public Gauge notificationRelayInFlightGauge(MeterRegistry registry,
            NotificationOutboxRelay relay) {
        return Gauge.builder(DomainMeterNames.NOTIFY_RELAY_IN_FLIGHT, relay,
                        NotificationOutboxRelay::inFlight)
                .register(registry);
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
