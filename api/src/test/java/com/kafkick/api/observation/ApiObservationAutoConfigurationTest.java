package com.kafkick.api.observation;

import com.kafkick.api.observation.issuance.IssuanceObservationService;
import com.kafkick.api.observation.issuance.CompositeEventRecorder;
import com.kafkick.api.observation.issuance.MeterEventRecorder;
import com.kafkick.api.observation.resource.ResourceProvider;
import com.kafkick.core.consistency.ConsistencyCalculator;
import com.kafkick.core.consistency.ConsistencySeverityPolicy;
import com.kafkick.core.consistency.DefaultConsistencyCalculator;
import com.kafkick.core.observation.CampaignLifecycleRecorder;
import com.kafkick.core.observation.EventIdGenerator;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.support.TimeProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import javax.sql.DataSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(OutputCaptureExtension.class)
class ApiObservationAutoConfigurationTest {

    private static final TimeProvider TIME_PROVIDER = new TimeProvider(Clock.fixed(
            Instant.parse("2026-08-19T01:00:05Z"),
            ZoneOffset.UTC
    ));

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ApiObservationAutoConfiguration.class))
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new);

    private final ApplicationContextRunner withoutMeterRegistryRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ApiObservationAutoConfiguration.class));

    @Test
    void registersDefaultBeansWhenImplementationsAreMissing() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(ConsistencyCalculator.class);
            assertThat(context).hasSingleBean(ConsistencySeverityPolicy.class);
            assertThat(context).hasSingleBean(EventIdGenerator.class);
            assertThat(context).hasSingleBean(IssuanceFlowEventFactory.class);
            assertThat(context).hasSingleBean(IssuanceObservationService.class);
            assertThat(context).hasSingleBean(TimeProvider.class);
            assertThat(context).hasSingleBean(CampaignLifecycleRecorder.class);
            assertThat(context).hasSingleBean(MeterEventRecorder.class);
            assertThat(context.getBean(EventRecorder.class))
                    .isInstanceOf(CompositeEventRecorder.class);
            assertThat(context.getBean(CampaignLifecycleRecorder.class))
                    .isInstanceOf(NoOpCampaignLifecycleRecorder.class);
            assertThat(context.getBean(ConsistencyCalculator.class))
                    .isInstanceOf(DefaultConsistencyCalculator.class);
            assertThat(context.getBean(ConsistencySeverityPolicy.class).warnThreshold()).isEqualTo(10);
            assertThat(context.getBean(ConsistencySeverityPolicy.class).criticalThreshold()).isEqualTo(100);
        });
    }

    @Test
    void remainsStartableWithoutAMeterRegistry() {
        withoutMeterRegistryRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(EventRecorder.class);
            assertThat(context.getBean(EventRecorder.class)).isInstanceOf(NoOpEventRecorder.class);
        });
    }

    @Test
    void warnsWhenObservationFallsBackToNoOp(CapturedOutput output) {
        withoutMeterRegistryRunner.run(context -> assertThat(output)
                .contains("MeterRegistry와 Kafka EventRecorder가 없어 no-op을 사용합니다."));
    }

    @Test
    void keepsKafkaPublisherUsableWhenMeterRegistryIsAbsent() {
        AtomicInteger publisherCalls = new AtomicInteger();

        withoutMeterRegistryRunner
                .withBean("attemptEventPublisher", EventRecorder.class,
                        () -> event -> publisherCalls.incrementAndGet())
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(MeterEventRecorder.class);
                    assertThat(context.getBean(EventRecorder.class))
                            .isInstanceOf(CompositeEventRecorder.class);

                    context.getBean(EventRecorder.class).record(context
                            .getBean(IssuanceFlowEventFactory.class).issueAttempt(new IssuanceFlowEvent.Ctx(
                                    "publisher-only", 101L, 201L, Grade.GOLD, false,
                                    Instant.parse("2026-08-23T00:00:00Z"), EngineVersion.V3,
                                    ReleaseStage.V3, QueueMode.ADAPTIVE, 901L, "api-1"
                            )));

                    assertThat(publisherCalls).hasValue(1);
                });
    }

    @Test
        // 미터 기록기를 직접 주입하지 않는다. 그러면 "MeterRegistry → meterEventRecorder →
        // 합성 빈" 배선이 끊겨도 테스트가 통과한다 — 단언 대상은 컨텍스트의 레지스트리다.
    void fansOutToTheCampaignMeterAndKafkaPublisher() {
        AtomicInteger publisherCalls = new AtomicInteger();

        contextRunner
                .withBean("attemptEventPublisher", EventRecorder.class,
                        () -> event -> publisherCalls.incrementAndGet())
                .run(context -> {
                    context.getBean(EventRecorder.class).record(context
                            .getBean(IssuanceFlowEventFactory.class)
                            .issueAttempt(eventContext("fanout")));

                    assertThat(publisherCalls).hasValue(1);
                    assertThat(context.getBean(MeterRegistry.class).find(MeterNames.ISSUANCE_FLOW)
                            .tags("coupon_id", "201", "stage", "attempt").counter().count())
                            .isEqualTo(1.0);
                });
    }

    @Test
        // 사용자가 자기 기록기에 붙일 법한 첫 번째 이름이 eventRecorder 다. 자동설정이 그 이름을
        // 쥐고 있으면 정의 덮어쓰기 금지에 걸려 기동이 죽는다.
    void doesNotClaimTheEventRecorderBeanNameForItself() {
        AtomicInteger auditCalls = new AtomicInteger();

        contextRunner
                .withBean("eventRecorder", EventRecorder.class,
                        () -> event -> auditCalls.incrementAndGet())
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(EventRecorder.class))
                            .isInstanceOf(CompositeEventRecorder.class);

                    context.getBean(EventRecorder.class).record(context
                            .getBean(IssuanceFlowEventFactory.class)
                            .issueAttempt(eventContext("name-clash")));

                    assertThat(auditCalls).hasValue(1);
                });
    }

    @Test
    void includesAUserSuppliedRecorderInTheFanOut() {
        AtomicInteger publisherCalls = new AtomicInteger();
        AtomicInteger auditCalls = new AtomicInteger();

        contextRunner
                .withBean("attemptEventPublisher", EventRecorder.class,
                        () -> event -> publisherCalls.incrementAndGet())
                .withBean("auditEventRecorder", EventRecorder.class,
                        () -> event -> auditCalls.incrementAndGet())
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(MeterEventRecorder.class);

                    context.getBean(EventRecorder.class).record(context
                            .getBean(IssuanceFlowEventFactory.class).issueAttempt(eventContext("fanout")));

                    assertThat(publisherCalls).hasValue(1);
                    assertThat(auditCalls).hasValue(1);
                });
    }

    @Test
    void startsWithAUserSuppliedRecorderWhileKafkaIsDisabled() {
        contextRunner
                .withBean("auditEventRecorder", EventRecorder.class, () -> event -> { })
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(MeterEventRecorder.class);
                    assertThat(context.getBean(EventRecorder.class))
                            .isInstanceOf(CompositeEventRecorder.class);
                });
    }

    @Test
        // 유일한 기록기가 합성 기록기이면 delegate 목록이 비어 기동이 죽었다.
    void startsWhenTheOnlyRecorderIsItselfAComposite() {
        AtomicInteger auditCalls = new AtomicInteger();
        CompositeEventRecorder userComposite =
                new CompositeEventRecorder(event -> auditCalls.incrementAndGet());

        withoutMeterRegistryRunner
                .withBean("auditEventRecorder", CompositeEventRecorder.class, () -> userComposite)
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    context.getBean(EventRecorder.class).record(context
                            .getBean(IssuanceFlowEventFactory.class)
                            .issueAttempt(eventContext("nested-composite")));

                    assertThat(auditCalls).hasValue(1);
                });
    }

    @Test
    void defaultCampaignLifecycleRecorderDoesNothing() {
        contextRunner.run(context -> {
            CampaignLifecycleRecorder recorder = context.getBean(CampaignLifecycleRecorder.class);
            Instant closedAt = Instant.parse("2026-08-19T01:00:00Z");

            assertThatCode(() -> recorder.retireCampaign(201L, closedAt))
                    .doesNotThrowAnyException();
        });
    }

    @Test
    void warnsWhenCampaignLifecycleRecorderFallsBackToNoOp(CapturedOutput output) {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(CampaignLifecycleRecorder.class);
            assertThat(output).contains("CampaignLifecycleRecorder 실구현이 없어 no-op을 사용합니다.");
        });
    }

    @Test
    void defaultCampaignLifecycleRecorderWarnsInsteadOfThrowingOnNonPositiveCampaignCouponId(
            CapturedOutput output
    ) {
        contextRunner.run(context -> {
            CampaignLifecycleRecorder recorder = context.getBean(CampaignLifecycleRecorder.class);
            Instant closedAt = Instant.parse("2026-08-19T01:00:00Z");

            // 관측 통지가 캠페인 종료 트랜잭션을 롤백시키면 안 된다. 대신 조용하지도 않아야 한다.
            assertThatCode(() -> recorder.retireCampaign(0L, closedAt))
                    .doesNotThrowAnyException();
            assertThat(output).contains("캠페인 수명 통지의 값 계약을 위반했습니다");
            assertThat(output).contains("campaignCouponId=0");
        });
    }

    @Test
    void defaultCampaignLifecycleRecorderWarnsInsteadOfThrowingOnNullClosedAt(
            CapturedOutput output
    ) {
        contextRunner.run(context -> {
            CampaignLifecycleRecorder recorder = context.getBean(CampaignLifecycleRecorder.class);

            assertThatCode(() -> recorder.retireCampaign(201L, null))
                    .doesNotThrowAnyException();
            assertThat(output).contains("캠페인 수명 통지의 값 계약을 위반했습니다");
            assertThat(output).contains("closedAt=null");
        });
    }

    @Test
    void defaultCampaignLifecycleRecorderStaysSilentOnValidNotification(CapturedOutput output) {
        contextRunner.run(context -> {
            CampaignLifecycleRecorder recorder = context.getBean(CampaignLifecycleRecorder.class);

            recorder.retireCampaign(201L, Instant.parse("2026-08-19T01:00:00Z"));

            assertThat(output).doesNotContain("캠페인 수명 통지의 값 계약을 위반했습니다");
        });
    }

    @Test
    void backsOffWhenCampaignLifecycleRecorderExists() {
        CampaignLifecycleRecorder recorder = (couponId, closedAt) -> { };

        contextRunner
                .withBean(CampaignLifecycleRecorder.class, () -> recorder)
                .run(context -> assertThat(context.getBean(CampaignLifecycleRecorder.class))
                        .isSameAs(recorder));
    }

    @Test
    void createsTimeProviderFromApplicationClock() {
        Instant fixedInstant = Instant.parse("2026-08-19T02:00:00Z");
        Clock applicationClock = Clock.fixed(fixedInstant, ZoneOffset.UTC);

        contextRunner
                .withBean(Clock.class, () -> applicationClock)
                .run(context -> assertThat(context.getBean(TimeProvider.class).instant())
                        .isEqualTo(fixedInstant));
    }

    @Test
    void usesSystemUtcWhenClockAndTimeProviderAreMissing(CapturedOutput output) {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(TimeProvider.class);
            assertThat(output).contains("Clock 빈이 없어 시스템 UTC 시계를 사용합니다.");
        });
    }

    @Test
    void backsOffWhenTimeProviderExists() {
        contextRunner
                .withBean(TimeProvider.class, () -> TIME_PROVIDER)
                .run(context -> assertThat(context.getBean(TimeProvider.class))
                        .isSameAs(TIME_PROVIDER));
    }

    @Test
    // 빈 이름이 mainDataSource 인 것은 우연이 아니다. 자동설정이 @Qualifier("mainDataSource") 로
    // 운영 풀을 지목하므로, 이름을 바꾸면 주입 대상이 사라져 이 테스트가 먼저 깨진다.
    void resourceProviderRequiresTheNamedDataSource() {
        contextRunner.run(context -> assertThat(context).doesNotHaveBean(ResourceProvider.class));

        contextRunner
                .withBean("mainDataSource", DataSource.class, ApiObservationAutoConfigurationTest::dataSource)
                .run(context -> assertThat(context).hasSingleBean(ResourceProvider.class));
    }

    @Test
    void resourceProviderStillRequiresAMeterRegistry() {
        withoutMeterRegistryRunner
                .withBean("mainDataSource", DataSource.class,
                        ApiObservationAutoConfigurationTest::dataSource)
                .run(context -> assertThat(context)
                        .hasNotFailed()
                        .doesNotHaveBean(ResourceProvider.class));
    }

    /**
     * 조건과 주입이 어긋나면 컨텍스트가 통째로 죽는다. 조건을 타입으로 두었을 때 실제로 그랬다 —
     * Boot 기본 {@code dataSource} 빈이 타입 조건을 통과시키고 이름 주입만 실패했다.
     *
     * <p>여기서 고정하는 것은 "빈이 없다" 가 아니라 "<b>기동은 살아 있다</b>" 쪽이다.
     */
    @Test
    void resourceProviderIsSkippedInsteadOfFailingWhenTheMainPoolIsAbsent() {
        contextRunner
                .withBean("dataSource", DataSource.class, ApiObservationAutoConfigurationTest::dataSource)
                .run(context -> assertThat(context)
                        .hasNotFailed()
                        .doesNotHaveBean(ResourceProvider.class));
    }

    /** 나머지 빈과 같은 오버라이드 규약을 따른다 — 사용자가 등록하면 자동설정이 물러난다. */
    @Test
    void userDefinedResourceProviderWins() {
        ResourceProvider stub = new ResourceProvider(
                dataSource(), new SimpleMeterRegistry(), TIME_PROVIDER);

        contextRunner
                .withBean("mainDataSource", DataSource.class, ApiObservationAutoConfigurationTest::dataSource)
                .withBean("myResourceProvider", ResourceProvider.class, () -> stub)
                .run(context -> assertThat(context)
                        .hasSingleBean(ResourceProvider.class)
                        .getBean(ResourceProvider.class).isSameAs(stub));
    }

    @Test
    void bindsConsistencySeverityThresholdsFromConfiguration() {
        contextRunner
                .withPropertyValues(
                        "observation.consistency.severity.warn-threshold=20",
                        "observation.consistency.severity.critical-threshold=200"
                )
                .run(context -> {
                    ConsistencySeverityPolicy policy = context.getBean(ConsistencySeverityPolicy.class);

                    assertThat(policy.warnThreshold()).isEqualTo(20);
                    assertThat(policy.criticalThreshold()).isEqualTo(200);
                    assertThat(context.getBean(ConsistencyCalculator.class))
                            .isInstanceOf(DefaultConsistencyCalculator.class);
                });
    }

    @Test
    void failsStartupWhenConsistencySeverityThresholdsAreInvalid() {
        contextRunner
                .withPropertyValues(
                        "observation.consistency.severity.warn-threshold=100",
                        "observation.consistency.severity.critical-threshold=10"
                )
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(IllegalArgumentException.class);
                });
    }

    @Test
    // EventRecorder 만 예외다. 나머지는 사용자 빈이 자동설정을 대체하지만, 기록기는 대체가
    // 아니라 합류한다 — 관측 sink 를 하나 얹었다고 캠페인 미터가 사라지면 안 된다.
    void backsOffWhenImplementationsExist() {
        EventRecorder eventRecorder = event -> { };
        IssuanceFlowEventFactory eventFactory = new IssuanceFlowEventFactory(
                () -> java.util.UUID.randomUUID()
        );
        IssuanceObservationService observationService =
                new IssuanceObservationService(eventFactory, eventRecorder, TIME_PROVIDER);
        ConsistencyCalculator calculator = (snapshot, phase, engineVersion) -> null;
        ConsistencySeverityPolicy policy = new ConsistencySeverityPolicy(30, 300);

        contextRunner
                .withBean(TimeProvider.class, () -> TIME_PROVIDER)
                .withBean(EventRecorder.class, () -> eventRecorder)
                .withBean(IssuanceObservationService.class, () -> observationService)
                .withBean(ConsistencyCalculator.class, () -> calculator)
                .withBean(ConsistencySeverityPolicy.class, () -> policy)
                .run(context -> {
                    assertThat(context).hasSingleBean(ConsistencyCalculator.class);
                    assertThat(context.getBean(EventRecorder.class))
                            .isInstanceOf(CompositeEventRecorder.class);
                    assertThat(context.getBeansOfType(EventRecorder.class)).containsValue(eventRecorder);
                    assertThat(context.getBean(IssuanceObservationService.class))
                            .isSameAs(observationService);
                    assertThat(context.getBean(ConsistencyCalculator.class)).isSameAs(calculator);
                    assertThat(context.getBean(ConsistencySeverityPolicy.class)).isSameAs(policy);
                });
    }

    private static IssuanceFlowEvent.Ctx eventContext(String requestId) {
        return new IssuanceFlowEvent.Ctx(
                requestId, 101L, 201L, Grade.GOLD, false,
                Instant.parse("2026-08-23T00:00:00Z"), EngineVersion.V3, ReleaseStage.V3,
                QueueMode.ADAPTIVE, 901L, "api-1"
        );
    }

    private static DataSource dataSource() {
        return org.mockito.Mockito.mock(DataSource.class);
    }
}
