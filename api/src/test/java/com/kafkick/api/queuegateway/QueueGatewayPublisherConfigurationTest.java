package com.kafkick.api.queuegateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.scheduling.TaskScheduler;

import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataReader;
import com.kafkick.core.queuegateway.QueueGatewayStatePort;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;

class QueueGatewayPublisherConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(QueueGatewayPublisherConfiguration.class)
            .withBean(QueueGatewayStatePort.class, () -> mock(QueueGatewayStatePort.class))
            .withBean(ApplicationAvailability.class, () -> mock(ApplicationAvailability.class))
            .withBean(AdminCouponRoundDataReader.class, () -> mock(AdminCouponRoundDataReader.class))
            .withBean(RuntimeConfigStore.class, () -> mock(RuntimeConfigStore.class));

    @Test
    void disabledByDefault() {
        runner.run(context -> {
            assertThat(context).doesNotHaveBean(QueueGatewayCapacityPublisher.class);
            assertThat(context).doesNotHaveBean(QueueGatewayCouponRoundPublisher.class);
        });
    }

    @Test
    void enabledConfigurationCreatesBothPublishers() {
        runner.withPropertyValues(
                        "queue.gateway.publisher.enabled=true",
                        "queue.gateway.publisher.credits-per-second=250",
                        "queue.gateway.publisher.instance-id=api-1")
                .run(context -> {
                    assertThat(context).hasSingleBean(QueueGatewayCapacityPublisher.class);
                    assertThat(context).hasSingleBean(QueueGatewayCouponRoundPublisher.class);
                    assertThat(context).hasBean(QueueGatewayPublisherConfiguration.CAPACITY_SCHEDULER);
                    assertThat(context).hasBean(QueueGatewayPublisherConfiguration.COUPON_ROUND_SCHEDULER);
                    assertThat(context.getBean(QueueGatewayPublisherConfiguration.CAPACITY_SCHEDULER))
                            .isNotSameAs(context.getBean(
                                    QueueGatewayPublisherConfiguration.COUPON_ROUND_SCHEDULER));
                });
    }

    /**
     * <b>종료 순서가 뒤집히면 내려간 인스턴스의 몫이 되살아난다.</b>
     *
     * <p>{@code @PreDestroy} 가 가용량 필드를 지운 뒤 늦게 끝난 보고가 그 필드를 다시 만들면,
     * 게이트웨이는 이미 사라진 api 의 몫을 계속 합산해 <b>죽은 서버로 사람을 들여보낸다.</b>
     *
     * <p>스프링은 의존하는 빈을 의존 대상보다 먼저 파괴하므로, <b>스케줄러가 공급기에
     * 의존하면 스케줄러가 먼저 멈춘다.</b> 그 의존이 실제로 등록됐는지를 본다 — 경쟁을
     * 타이밍으로 재면 깜빡이는 테스트가 되고, 깜빡이는 테스트는 결국 꺼진다.
     */
    @Test
    void schedulersAreDestroyedBeforeTheirPublishers() {
        runner.withPropertyValues(
                        "queue.gateway.publisher.enabled=true",
                        "queue.gateway.publisher.credits-per-second=250",
                        "queue.gateway.publisher.instance-id=api-1")
                .run(context -> {
                    assertThat(context.getBeanFactory()
                            .getDependentBeans("queueGatewayCapacityPublisher"))
                            .as("스케줄러가 공급기에 의존해야 먼저 멈춘다. 이 의존이 빠지면 "
                                    + "제거 뒤에 늦은 보고가 필드를 되살린다")
                            .contains(QueueGatewayPublisherConfiguration.CAPACITY_SCHEDULER);

                    assertThat(context.getBeanFactory()
                            .getDependentBeans("queueGatewayCouponRoundPublisher"))
                            .contains(QueueGatewayPublisherConfiguration.COUPON_ROUND_SCHEDULER);
                });
    }

    @Test
    void blockedCouponRoundTaskDoesNotDelayCapacityScheduler() {
        runner.withPropertyValues(
                        "queue.gateway.publisher.enabled=true",
                        "queue.gateway.publisher.credits-per-second=250",
                        "queue.gateway.publisher.instance-id=api-1")
                .run(context -> {
                    TaskScheduler capacity = context.getBean(
                            QueueGatewayPublisherConfiguration.CAPACITY_SCHEDULER,
                            TaskScheduler.class);
                    TaskScheduler couponRound = context.getBean(
                            QueueGatewayPublisherConfiguration.COUPON_ROUND_SCHEDULER,
                            TaskScheduler.class);
                    CountDownLatch roundStarted = new CountDownLatch(1);
                    CountDownLatch releaseRound = new CountDownLatch(1);
                    CountDownLatch capacityRan = new CountDownLatch(1);
                    try {
                        couponRound.schedule(() -> {
                            roundStarted.countDown();
                            try {
                                releaseRound.await();
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                            }
                        }, Instant.now());
                        assertThat(roundStarted.await(1, TimeUnit.SECONDS)).isTrue();

                        capacity.schedule(capacityRan::countDown, Instant.now());

                        assertThat(capacityRan.await(1, TimeUnit.SECONDS)).isTrue();
                    } finally {
                        releaseRound.countDown();
                    }
                });
    }

    @Test
    void invalidEnabledCapacityFailsAtStartup() {
        runner.withPropertyValues(
                        "queue.gateway.publisher.enabled=true",
                        "queue.gateway.publisher.credits-per-second=0")
                .run(context -> assertThat(context).hasFailed());
    }
}
