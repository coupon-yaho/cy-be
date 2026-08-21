package com.kafkick.infra.redis.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.data.redis.autoconfigure.ClientResourcesBuilderCustomizer;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.Profiles;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.Resource;

/**
 * lettuce 레이턴시 미터를 다루는 방법. 미터를 등록하는 모듈이 그것을 검사하는 방법도 소유한다.
 *
 * <p>백분위·expiry 를 소유하는 설정 파일은 모듈마다 다르지만(api 는 {@code observation.yml},
 * batch 는 {@code management.yml}) 계측기를 돌리고 값을 꺼내는 절차는 같다. 각자 복붙하면
 * Micrometer·Lettuce 시그니처가 바뀔 때 한쪽만 고쳐진 채 남는다.
 */
public final class RedisLatencyMeterTestSupport {

    /** 자동설정이 순서를 거는 이름을 옮겨 적지 않는다 — 갈라지면 조용히 다른 것을 태운다. */
    private static final List<String> METRICS_AUTO_CONFIGURATION_NAMES = List.of(
        RedisLatencyAutoConfiguration.METRICS_AUTO_CONFIGURATION,
        RedisLatencyAutoConfiguration.COMPOSITE_METER_REGISTRY_AUTO_CONFIGURATION);

    /** Gradle 이 해석해 넘겨준 런타임 프로젝트 목록. 루트 build.gradle 의 configure([:api, :batch]) 가 채운다. */
    private static final String RUNTIME_PROJECTS_PROPERTY = "cy.runtimeProjects";

    /** Spring 프로퍼티 키다. 모듈 고유 로직이 아니라 양쪽 계약 테스트가 함께 쓴다. */
    public static final String PERCENTILES_PREFIX = "management.metrics.distribution.percentiles";
    public static final String EXPIRY_PREFIX = "management.metrics.distribution.expiry";
    public static final String HTTP_SERVER_REQUESTS = "http.server.requests";

    private static final String ACTIVATE_ON_PROFILE = "spring.config.activate.on-profile";

    private RedisLatencyMeterTestSupport() {
    }

    /**
     * 콤마 목록을 값으로 비교한다. 배포 설정과 모듈 템플릿의 표기가 다르다 —
     * 루트는 {@code "0.5,0.95,0.99"}, 모듈은 공백을 넣는다. 문자열로 대조하면 값이 같은데 깨진다.
     */
    public static List<String> normalizeCsv(String value) {
        return value == null ? List.of() : Arrays.stream(value.split(",")).map(String::trim).toList();
    }

    /**
     * 계측 모듈이 이 모듈의 <b>런타임</b> 클래스패스에 실려 있는지 본다.
     *
     * <p>build.gradle 을 문자열로 읽지 않는다 — 주석 형태 하나로 뚫린다(실측: 선언을 지우면
     * 잡았지만 {@code /* *}{@code /} 로 감싸면 통과했다). 또 테스트 클래스패스로는 판정할 수
     * 없다 — {@code testImplementation} 이 같은 모듈을 이미 올려 두어 {@code runtimeOnly} 를
     * 지워도 컴파일과 실행이 전부 초록불이고, bootJar 에서만 빠진다.
     */
    public static void assertOnRuntimeClasspath(String projectPath) {
        String resolved = System.getProperty(RUNTIME_PROJECTS_PROPERTY);
        assertThat(resolved)
            .as("build.gradle 의 test 태스크가 " + RUNTIME_PROJECTS_PROPERTY + " 를 넘겨야 한다")
            .isNotNull();
        assertThat(List.of(resolved.split(",")))
            .as("이 줄이 없으면 AutoConfiguration.imports 가 bootJar 클래스패스에 올라오지 않는다")
            .contains(projectPath);
    }

    public static Class<?>[] metricsAutoConfigurations() {
        return METRICS_AUTO_CONFIGURATION_NAMES.stream()
            .map(RedisLatencyMeterTestSupport::load)
            .toArray(Class<?>[]::new);
    }

    /** 커스터마이저가 실제로 붙인 레코더로 명령 하나를 기록한다. 이래야 미터가 생긴다. */
    public static void recordOneCommand(ClientResourcesBuilderCustomizer customizer) {
        ClientResources.Builder builder = ClientResources.builder();
        customizer.customize(builder);
        ClientResources resources = builder.build();
        try {
            resources.commandLatencyRecorder().recordCommandLatency(
                new InetSocketAddress("127.0.0.1", 10_001),
                new InetSocketAddress("127.0.0.1", 6_379),
                CommandType.GET,
                TimeUnit.MILLISECONDS.toNanos(2),
                TimeUnit.MILLISECONDS.toNanos(5));
        } finally {
            resources.shutdown();
        }
    }

    public static List<Double> percentiles(MeterRegistry registry, String meter) {
        return Arrays.stream(timer(registry, meter).takeSnapshot().percentileValues())
            .map(value -> value.percentile())
            .toList();
    }

    /** 관측 창이 닫히면 값이 0 이 된다 — 미터 자체는 남으므로 존재 여부로는 못 가른다. */
    public static boolean hasPositivePercentile(MeterRegistry registry, String meter) {
        return Arrays.stream(timer(registry, meter).takeSnapshot().percentileValues())
            .anyMatch(value -> value.value(TimeUnit.NANOSECONDS) > 0);
    }

    /** 없으면 던진다. 픽스처는 값과 예외만 내고, 단정은 호출하는 계약 테스트가 한다. */
    public static Timer timer(MeterRegistry registry, String meter) {
        Timer found = registry.find(meter).timer();
        if (found == null) {
            throw new IllegalStateException(meter + " 미터가 등록되지 않았다");
        }
        return found;
    }

    /**
     * 배포 설정에서 한 프로파일이 실제로 보는 값. 공통 문서 + 그 프로파일 문서만 겹친다.
     *
     * <p>{@link #yaml(Resource)} 로 통째로 읽으면 {@code ---} 로 갈린 문서가 전부 병합되고
     * 뒤 문서가 이겨, 프로파일별 분기가 통째로 안 보인다 — api 문서에만 창을 벌리는 변경이
     * batch 문서에 가려 잡히지 않는다(실측).
     */
    public static Properties yamlProfile(Resource resource, String profile) {
        Properties merged = new Properties();
        for (Properties document : yamlDocuments(resource)) {
            String activated = document.getProperty(ACTIVATE_ON_PROFILE);
            // 문자열 동등비교로 보면 안 된다. on-profile 은 표현식을 받는다 —
            // "api,batch" · "api | batch" · "!batch" 가 전부 유효하고, equals 로는 그런 문서가
            // 어느 프로파일에서도 안 잡혀 배포 설정이 실제로 적용하는 값을 통째로 놓친다(실측).
            //
            // 콤마는 먼저 나눈다. Boot 가 이 키를 배열로 바인딩한 뒤 각 원소를 표현식으로
            // 해석하기 때문이고, "api,batch" 를 한 표현식으로 넘기면 파서가 못 읽는다(실측).
            if (activated == null || Profiles.of(splitExpressions(activated)).matches(profile::equals)) {
                merged.putAll(document);
            }
        }
        return merged;
    }

    private static String[] splitExpressions(String onProfile) {
        return Arrays.stream(onProfile.split(",")).map(String::trim).toArray(String[]::new);
    }

    /**
     * {@code ---} 로 갈린 문서를 각각 읽는다.
     *
     * <p>직접 나누지 않는다. 정규식으로 자르면 블록 스칼라나 따옴표 안의 {@code ---} 를
     * 구분자로 오인한다 — Boot 가 실제로 쓰는 로더에 맡겨 분할 규칙을 하나로 둔다.
     */
    public static List<Properties> yamlDocuments(Resource resource) {
        if (!resource.exists()) {
            throw new IllegalStateException(resource + " 를 찾지 못했다");
        }
        List<PropertySource<?>> sources;
        try {
            sources = new YamlPropertySourceLoader().load(resource.getDescription(), resource);
        } catch (IOException e) {
            throw new IllegalStateException(resource + " 를 읽지 못했다", e);
        }
        List<Properties> documents = new ArrayList<>();
        for (PropertySource<?> source : sources) {
            Properties document = new Properties();
            if (source.getSource() instanceof Map<?, ?> map) {
                map.forEach((key, value) -> {
                    if (value != null) {
                        document.setProperty(String.valueOf(key), String.valueOf(value));
                    }
                });
            }
            documents.add(document);
        }
        return documents;
    }

    /** 없으면 던진다 — 이름만 보고 순수 파서로 읽히면 안 되므로 실패를 예외로 드러낸다. */
    public static Properties yaml(Resource resource) {
        if (!resource.exists()) {
            throw new IllegalStateException(resource + " 를 찾지 못했다");
        }
        YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
        factory.setResources(resource);
        Properties properties = factory.getObject();
        if (properties == null) {
            throw new IllegalStateException(resource + " 를 파싱하지 못했다");
        }
        return properties;
    }

    private static Class<?> load(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("metrics 자동설정 이름이 바뀌었다: " + name, e);
        }
    }
}
