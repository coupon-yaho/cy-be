package com.kafkick.infra.redis.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.data.redis.autoconfigure.ClientResourcesBuilderCustomizer;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class RedisLatencyAutoConfigurationTest {

    private static final String FIRST_RESPONSE_METER = "lettuce.command.firstresponse";
    private static final String COMPLETION_METER = "lettuce.command.completion";

    private final RedisLatencyAutoConfiguration config = new RedisLatencyAutoConfiguration();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @Test
    @DisplayName("자동설정으로 등록돼 있어야 한다 — imports 에서 빠지면 계측이 통째로 사라진다")
    void isRegisteredAsAutoConfiguration() {
        // 아래 테스트들은 전부 AutoConfigurations.of(...) 로 이 클래스를 직접 등록한다. 그래서
        // imports 파일에서 줄이 사라져도 전부 통과한다(실측). 등록 자체를 여기서 본다.
        assertThat(ImportCandidates.load(AutoConfiguration.class, getClass().getClassLoader()))
                .as("META-INF/spring 의 imports 에서 빠지면 예외도 로그도 없이 lettuce_command_* 만 사라진다")
                .contains(RedisLatencyAutoConfiguration.class.getName());
    }

    @Test
    @DisplayName("MeterRegistry 가 있으면 커맨드 레코더를 등록한다")
    void registersRecorderWhenMeterRegistryPresent() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisLatencyAutoConfiguration.class))
                .withBean(SimpleMeterRegistry.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(ClientResourcesBuilderCustomizer.class);
                    // 필터가 빈으로 안 올라와도 아래 태그 테스트들은 필터를 직접 호출하니 통과한다.
                    // 그 상태로는 remote 태그(Redis host:port)가 그대로 scrape 로 나간다.
                    assertThat(context).hasBean("redisLatencyTags");
                });
    }

    @Test
    @DisplayName("lettuce 가 클래스패스에 없으면 자동설정이 통째로 물러난다")
    void backsOffWhenLettuceAbsent() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisLatencyAutoConfiguration.class))
                .withBean(SimpleMeterRegistry.class)
                .withClassLoader(new FilteredClassLoader(MicrometerCommandLatencyRecorder.class))
                .run(context -> assertThat(context).doesNotHaveBean(ClientResourcesBuilderCustomizer.class));
    }

    @Test
    @DisplayName("다른 커스터마이저가 있어도 물러나지 않는다 — 가산형 확장점이다")
    void coexistsWithOtherCustomizers() {
        // Boot 는 LettuceObservationAutoConfiguration 에서 같은 타입 빈을 낸다. 타입으로
        // backoff 를 걸면 트레이싱을 붙이는 순간 계측이 예외도 로그도 없이 사라진다.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisLatencyAutoConfiguration.class))
                .withBean(SimpleMeterRegistry.class)
                .withBean("lettuceObservation", ClientResourcesBuilderCustomizer.class,
                        () -> builder -> { })
                .run(context -> assertThat(context)
                        .getBeans(ClientResourcesBuilderCustomizer.class)
                        .hasSize(2)
                        .containsKey("redisLatencyCustomizer"));
    }

    @Test
    @DisplayName("Boot 가 ClientResources 에 레코더를 실제로 붙인다")
    void recorderIsAppliedByDataRedisAutoConfiguration() {
        // 다른 테스트는 커스터마이저를 손으로 호출한다. beforeName 이 전제하는 경로
        // (DataRedisAutoConfiguration 이 커스터마이저를 모아 ClientResources 에 붙인다)가
        // 깨져도 그 테스트들은 전부 통과한다.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        RedisLatencyAutoConfiguration.class, DataRedisAutoConfiguration.class))
                .withBean(SimpleMeterRegistry.class)
                .run(context -> assertThat(context.getBean(LettuceConnectionFactory.class)
                        .getClientResources()
                        .commandLatencyRecorder())
                        .isInstanceOf(MicrometerCommandLatencyRecorder.class));
    }

    @Test
    @DisplayName("MeterRegistry 가 없으면 레코더를 등록하지 않는다")
    void skipsRecorderWhenMeterRegistryAbsent() {
        // @ConditionalOnBean 이 지워져도 위 테스트는 통과한다. 부재 쪽을 함께 고정해야
        // 조건이 실제로 걸려 있는지 회귀로 잡힌다.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisLatencyAutoConfiguration.class))
                .run(context -> assertThat(context).doesNotHaveBean(ClientResourcesBuilderCustomizer.class));
    }

    @Test
    @DisplayName("명령별로 firstresponse · completion 을 기록하고 접속 정보 태그는 남기지 않는다")
    void recordsFirstResponseAndCompletionPerCommandWithoutAddressTags() throws Exception {
        registry.config().meterFilter(config.redisLatencyTags());

        ClientResources resources = clientResources();
        try {
            record(resources, CommandType.GET, 2, 5);
            record(resources, CommandType.GET, 3, 7);
            record(resources, CommandType.SET, 4, 9);
        } finally {
            resources.shutdown();
        }

        assertThat(timers(FIRST_RESPONSE_METER))
                .extracting(timer -> timer.getId().getTag("command"))
                .containsExactlyInAnyOrder("GET", "SET");
        assertThat(timers(COMPLETION_METER))
                .extracting(timer -> timer.getId().getTag("command"))
                .containsExactlyInAnyOrder("GET", "SET");

        assertThat(registry.getMeters()).allSatisfy(meter ->
                assertThat(tagKeys(meter)).doesNotContain("key", "local", "remote"));
    }

    @Test
    @DisplayName("나중에 걸리는 공통 태그는 지우지 않는다")
    void keepsCommonTags() {
        // allowlist 로 짜면 여기서 instance 태그가 사라진다 — 인스턴스를 2대로 늘리는 순간
        // Redis 미터만 한 시계열로 합쳐져 어느 쪽이 느린지 못 가린다.
        registry.config().commonTags(Tags.of("instance", "batch-1"));
        registry.config().meterFilter(config.redisLatencyTags());

        Timer timer = registry.timer(FIRST_RESPONSE_METER, "command", "GET", "remote", "10.0.0.1:6379");

        assertThat(tagKeys(timer)).containsExactlyInAnyOrder("command", "instance");
    }

    @Test
    @DisplayName("scrape 출력에 히스토그램 버킷이 없다")
    void publishesWithoutHistogramBuckets() throws Exception {
        // SimpleMeterRegistry 로는 이걸 증명할 수 없다 — histogram(true) 로 바꿔도
        // histogramCounts() 가 비어 있어 가드가 통과한다(실측). 버킷은 Prometheus 레지스트리의
        // scrape 출력에만 나타나므로 운영과 같은 레지스트리로 본다.
        PrometheusMeterRegistry prometheus = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        prometheus.config().meterFilter(config.redisLatencyTags());

        ClientResources resources = clientResources(prometheus);
        try {
            record(resources, CommandType.GET, 2, 5);
        } finally {
            resources.shutdown();
        }

        assertThat(prometheus.scrape())
                .as("버킷이 켜지면 command 종류 × 버킷 수만큼 시계열이 쏟아진다")
                .doesNotContain("lettuce_command_firstresponse_seconds_bucket")
                .doesNotContain("lettuce_command_completion_seconds_bucket");
    }

    private List<Timer> timers(String name) {
        return List.copyOf(registry.find(name).timers());
    }

    private ClientResources clientResources() {
        return clientResources(registry);
    }

    private ClientResources clientResources(MeterRegistry target) {
        ClientResources.Builder builder = ClientResources.builder();
        config.redisLatencyCustomizer(target).customize(builder);
        return builder.build();
    }

    private static void record(ClientResources resources, CommandType command, long first, long completion) {
        resources.commandLatencyRecorder().recordCommandLatency(
                new InetSocketAddress("127.0.0.1", 10_001),
                new InetSocketAddress("127.0.0.1", 6_379),
                command,
                TimeUnit.MILLISECONDS.toNanos(first),
                TimeUnit.MILLISECONDS.toNanos(completion));
    }

    private static Set<String> tagKeys(Meter meter) {
        return meter.getId().getTags().stream()
                .map(Tag::getKey)
                .collect(Collectors.toSet());
    }
}
