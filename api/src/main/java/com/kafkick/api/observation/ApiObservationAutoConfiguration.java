package com.kafkick.api.observation;

import java.time.Clock;

import javax.sql.DataSource;

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
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@AutoConfiguration(
        after = { MetricsAutoConfiguration.class, CompositeMeterRegistryAutoConfiguration.class },
        afterName = "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration")
@EnableConfigurationProperties({ConsistencySeverityProperties.class, ObservationIssuanceProperties.class})
public class ApiObservationAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ApiObservationAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(EventIdGenerator.class)
    public EventIdGenerator eventIdGenerator() {
        return new UuidEventIdGenerator();
    }

    @Bean
    @ConditionalOnMissingBean(IssuanceFlowEventFactory.class)
    public IssuanceFlowEventFactory issuanceFlowEventFactory(EventIdGenerator eventIdGenerator) {
        return new IssuanceFlowEventFactory(eventIdGenerator);
    }

    /**
     * MeterRegistry 가 있는 프로세스에 JVM 내 캠페인 미터 기록기를 등록합니다.
     *
     * <p>조건은 {@link MeterRegistry} 하나뿐입니다. 다른 {@link EventRecorder} 가 있다고 해서
     * 물러서지 않습니다 — 이 기록기는 대체재가 아니라 병렬 sink 이고, 아래 fan-out 이 모두를
     * 함께 호출합니다. 예전처럼 "다른 EventRecorder 가 있으면 등록하지 않는다" 로 두면 사용자가
     * 자기 기록기를 하나 얹는 순간 캠페인 미터가 통째로, <b>로그 한 줄 없이</b> 사라집니다.
     *
     * @param meterRegistry 캠페인 미터를 등록할 레지스트리
     * @param issuanceProperties 기록 실패 로그의 유량 제한 간격
     * @return 캠페인별 발급 미터 기록기
     */
    @Bean(name = "meterEventRecorder")
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean(MeterEventRecorder.class)
    public MeterEventRecorder meterEventRecorder(
            MeterRegistry meterRegistry,
            ObservationIssuanceProperties issuanceProperties
    ) {
        return new MeterEventRecorder(
                meterRegistry,
                issuanceProperties.resolvedAttemptFailureLogInterval()
        );
    }

    /**
     * 등록된 모든 {@link EventRecorder} 를 하나로 묶어 발급 경로가 잡을 단일 진입점을 만듭니다.
     *
     * <p><b>목록을 손으로 적지 않는 이유.</b> 예전에는 {@code attemptEventPublisher} 와
     * {@code meterEventRecorder} 두 이름만 delegate 로 넣었다. 그러면 제3의 기록기는 컨텍스트에
     * 있는데도 {@code @Primary} 인 이 빈에 가려 한 번도 호출되지 않는다. 이름·타입 문자열이
     * 어긋나도 마찬가지로 조용히 빠진다 — 관측이 빠지는 실패는 예외가 아니라 빈 그래프로만
     * 드러나므로, 배선은 열거가 아니라 수집이어야 한다.
     *
     * <p><b>빈 이름을 {@code eventRecorder} 로 두지 않는다.</b> 그 이름은 사용자가 자기 기록기에
     * 붙일 법한 첫 번째 후보이고, 겹치는 순간 정의 덮어쓰기 금지에 걸려 컨텍스트가 통째로 죽거나
     * (덮어쓰기를 허용한 프로세스라면) 이 합성 빈이 조용히 사라진다.
     *
     * <p>자기 자신은 스프링의 self-reference 배제로 목록에서 빠진다. 그 밖에는 타입을 보지
     * 않는다 — 합성 기록기라는 이유로 걸러 내면, 컨텍스트에 있는 유일한 기록기가 합성 기록기일 때
     * delegate 가 0개가 되어 기동이 죽는다. 남의 합성 기록기는 그냥 sink 하나로 취급하면 되고,
     * 그 안의 leaf 는 여전히 이벤트를 한 번만 받는다.
     *
     * @param recorders 컨텍스트에 등록된 모든 기록기
     * @param issuanceProperties 전달 실패 로그의 유량 제한 간격
     * @return 모든 sink 로 이벤트를 병렬 전달하는 기록기
     */
    @Bean
    @Primary
    @ConditionalOnBean(EventRecorder.class)
    public CompositeEventRecorder issuanceEventRecorder(
            ObjectProvider<EventRecorder> recorders,
            ObservationIssuanceProperties issuanceProperties
    ) {
        EventRecorder[] delegates = recorders.orderedStream().toArray(EventRecorder[]::new);
        return new CompositeEventRecorder(
                issuanceProperties.resolvedAttemptFailureLogInterval(), delegates);
    }

    @Bean
    @ConditionalOnMissingBean(EventRecorder.class)
    public EventRecorder fallbackEventRecorder() {
        log.warn("MeterRegistry와 Kafka EventRecorder가 없어 no-op을 사용합니다.");
        return new NoOpEventRecorder();
    }

    /**
     * 캠페인 수명 통지를 받을 기본 포트를 등록합니다.
     *
     * <p>실구현(OBS-26)이 들어오기 전에도 호출부를 붙일 수 있게 무동작 구현을 기본값으로 둡니다.
     *
     * @return 통지를 버리는 기본 수명 기록 포트
     */
    @Bean
    @ConditionalOnMissingBean(CampaignLifecycleRecorder.class)
    public CampaignLifecycleRecorder campaignLifecycleRecorder() {
        log.warn("CampaignLifecycleRecorder 실구현이 없어 no-op을 사용합니다.");
        return new NoOpCampaignLifecycleRecorder();
    }

    /**
     * 애플리케이션 공통 시계를 사용하는 기본 시간 공급자를 등록합니다.
     *
     * <p>사용자가 {@link TimeProvider}를 직접 등록하면 이 Bean은 생성되지 않습니다. 별도 시간
     * 공급자는 없지만 {@link Clock} Bean이 있으면 해당 시계를 사용해 테스트와 애플리케이션의
     * 시각 기준을 유지합니다. 둘 다 없을 때만 시스템 UTC 시계를 최후 기본값으로 사용합니다.
     *
     * @param clockProvider 애플리케이션이 제공하는 공통 시계 조회 경계
     * @return 발급 관측 결과 시각을 제공할 시간 공급자
     */
    @Bean
    @ConditionalOnMissingBean(TimeProvider.class)
    public TimeProvider observationTimeProvider(ObjectProvider<Clock> clockProvider) {
        Clock clock = clockProvider.getIfAvailable(() -> {
            log.warn("Clock 빈이 없어 시스템 UTC 시계를 사용합니다. 시각 고정 테스트가 무력화됩니다.");
            return Clock.systemUTC();
        });
        return new TimeProvider(clock);
    }

    /**
     * DB와 Micrometer 원천이 모두 있는 애플리케이션에만 로컬 자원 공급자를 등록합니다.
     *
     * <p>이 자동설정만 단독으로 띄우는 컨텍스트에는 두 원천이 없으므로 빈도 없습니다. 기존
     * 자동설정 빈을 대체하지 않고 신규 빈 하나만 더합니다.
     *
     * <p><b>운영 풀을 한정자로 못박는다.</b> api 는 {@code observation.datasource.enabled} 로
     * {@code ObservationDataSourceConfig} 를 켜므로 DataSource 빈이 둘이다 — 운영 풀
     * ({@code mainDataSource}, {@code @Primary})과 관측 SELECT 전용 풀({@code obs}). 자원 6행의
     * DB_POOL 은 부하 시험의 병목인 <b>운영 풀</b>을 재는 자리다. 관측 풀은 그 병목을 건드리지
     * 않으려고 떼어 낸 측정 도구이지 측정 대상이 아니다.
     *
     * <p>한정자 없이 두면 지금은 {@code @Primary} 덕에 우연히 운영 풀에 붙지만, 나중에 그
     * {@code @Primary} 가 옮겨 가면 측정 대상이 조용히 바뀐다 — 값이 나오므로 아무도 모른다.
     *
     * <p><b>조건과 주입이 같은 것을 봐야 한다.</b> 조건을 {@code DataSource.class} 타입으로 두면
     * {@code observation.datasource.enabled} 를 끈 모듈에서 Boot 기본 {@code dataSource} 빈이
     * 타입 조건을 통과시키고, 이름 주입만 실패해 컨텍스트 전체가 죽는다. 그래서 조건도 이름으로
     * 본다. {@code value} 와 {@code name} 을 같이 준 것은 AND 다(둘 다 있어야 등록).
     *
     * <p><b>이 방어가 만드는 반대 방향 실패</b> — 이름이 안 맞으면 이제 죽지 않고 빈이 조용히
     * 사라진다. 자원 6행이 통째로 없는 화면이 되고 아무 로그도 남지 않는다. 지금 api 는
     * {@code application.yml} 이 스위치를 켠 채 고정돼 있고, 끄면 {@code management.yml} 의
     * health {@code group.obs} 가 먼저 기동을 막는다({@code ObservationOptInContractTest}) —
     * 즉 이 침묵이 실제로 드러나는 경로는 그 그룹까지 같이 걷어낸 뒤뿐이다. 그 조합을 만들려면
     * 여기 조건도 함께 재검토한다.
     */
    @Bean
    @ConditionalOnMissingBean(ResourceProvider.class)
    @ConditionalOnBean(value = MeterRegistry.class, name = "mainDataSource")
    public ResourceProvider resourceProvider(
            @Qualifier("mainDataSource") DataSource dataSource,
            MeterRegistry meterRegistry,
            TimeProvider timeProvider
    ) {
        return new ResourceProvider(dataSource, meterRegistry, timeProvider);
    }

    /**
     * 요청 단위 발급 관측 Session을 생성하는 서비스를 등록합니다.
     *
     * <p>실제 {@link EventRecorder} 구현이 등록되면 조건부 기본 NoOp 대신 해당 구현을 사용합니다.
     *
     * @param eventFactory 발급 관측 이벤트 생성기
     * @param eventRecorder 완성된 이벤트를 전달할 기록 포트
     * @param timeProvider 결과 발생 시각을 제공하는 시간 공급자
     * @param issuanceProperties 기록 실패 로그의 유량 제한 간격 등 발급 관측 운영 임계치
     * @return 발급 관측 Session 생성 서비스
     */
    @Bean
    @ConditionalOnMissingBean(IssuanceObservationService.class)
    public IssuanceObservationService issuanceObservationService(
            IssuanceFlowEventFactory eventFactory,
            EventRecorder eventRecorder,
            TimeProvider timeProvider,
            ObservationIssuanceProperties issuanceProperties
    ) {
        return new IssuanceObservationService(
                eventFactory,
                eventRecorder,
                timeProvider,
                issuanceProperties.resolvedAttemptFailureLogInterval()
        );
    }

    @Bean
    @ConditionalOnMissingBean(ConsistencySeverityPolicy.class)
    public ConsistencySeverityPolicy consistencySeverityPolicy(
            ConsistencySeverityProperties properties
    ) {
        return properties.toPolicy();
    }

    @Bean
    @ConditionalOnMissingBean(ConsistencyCalculator.class)
    public ConsistencyCalculator consistencyCalculator(ConsistencySeverityPolicy severityPolicy) {
        return new DefaultConsistencyCalculator(severityPolicy);
    }
}
