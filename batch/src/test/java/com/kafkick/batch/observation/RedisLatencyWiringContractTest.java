package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.EXPIRY_PREFIX;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.HTTP_SERVER_REQUESTS;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.PERCENTILES_PREFIX;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.assertOnRuntimeClasspath;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.normalizeCsv;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.yamlProfile;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.yaml;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import com.kafkick.infra.redis.observation.RedisLatencyAutoConfiguration;
import com.kafkick.infra.redis.runtimeconfig.RuntimeConfigRedisAutoConfiguration;

/**
 * Redis 명령 레이턴시(CY-310)는 미터를 등록하는 쪽(infra:redis)과 백분위·관측 창을 소유하는
 * 쪽(이 모듈의 management.yml)이 다르다. 둘 중 하나만 빠져도 예외 없이 지표만 조용히 빈다.
 */
class RedisLatencyWiringContractTest {

    // 키 문자열을 옮겨 적지 않는다 — infra:redis 가 미터 네임스페이스를 바꾸면 여기가 깨져야
    // 한다. 옮겨 적으면 자기 상수와 yml 만 비교하게 되어 둘이 어긋나도 계속 통과한다.
    private static final String LETTUCE_DISTRIBUTION_KEY =
        "[" + RedisLatencyAutoConfiguration.COMMAND_METER_NAMESPACE + "]";

    @Test
    @DisplayName("계측 모듈이 batch 런타임 클래스패스에 실려 있어야 한다")
    void redisLatencyModuleIsOnRuntimeClasspath() {
        // 왜 런타임 클래스패스로 보는지는 assertOnRuntimeClasspath 의 javadoc 에 있다.
        assertOnRuntimeClasspath(":infra:redis");
    }

    @Test
    @DisplayName("자동설정 순서를 거는 이름이 실제로 해석돼야 한다")
    void autoConfigurationOrderingNamesResolve() {
        // afterName 은 이름이 틀려도 예외 없이 무시된다. 그러면 MeterRegistry 빈 정의 이전에
        // 조건이 평가되어 계측이 클래스명 정렬 결과에 따라 사라진다.
        // Boot 마이너 업그레이드에서 패키지가 갈리면 여기서 잡는다.
        for (String name : orderingNames()) {
            assertThatCode(() -> Class.forName(name)).doesNotThrowAnyException();
        }
    }

    @Test
    @DisplayName("백분위와 관측 창을 설정 템플릿이 쥐고 있어야 한다")
    void distributionIsOwnedByTemplate() {
        Properties template = managementTemplate();

        assertThat(normalizeCsv(template.getProperty(PERCENTILES_PREFIX + LETTUCE_DISTRIBUTION_KEY)))
            .as("없으면 백분위가 아예 나오지 않는다")
            .containsExactly("0.5", "0.95", "0.99");
        assertThat(template.getProperty(EXPIRY_PREFIX + LETTUCE_DISTRIBUTION_KEY))
            .as("빠지면 기본 2분이 걸려 HTTP p99(10초 창)와 같은 시점의 값이 아니게 된다")
            .isEqualTo("10s");
    }

    @Test
    @DisplayName("관측 창이 api 의 HTTP 창과 같아야 한다")
    void expiryMatchesApiObservationWindow() {
        // 배포 설정의 HTTP 창은 api 프로파일 문서 안에 있어 batch 는 그것을 보지 않는다.
        // 그래도 두 JVM 의 Redis p99 와 api 의 HTTP p99 를 겹쳐 읽는 게 이 계측의 목적이라,
        // 세 값이 같은 창을 써야 한다. 기준은 api 가 소유한 observation.yml 이다.
        Properties api = yaml(new FileSystemResource(
            Path.of("..", "api", "src", "main", "resources", "observation.yml.example")));
        String apiHttp = api.getProperty(EXPIRY_PREFIX + "[" + HTTP_SERVER_REQUESTS + "]");

        assertThat(apiHttp).as("api 쪽 키 표기가 바뀌면 비교 자체가 무의미해진다").isNotNull();
        assertThat(managementTemplate().getProperty(EXPIRY_PREFIX + LETTUCE_DISTRIBUTION_KEY))
            .as("관측 창이 다르면 HTTP p99 와 Redis p99 가 같은 시점의 값이 아니다")
            .isEqualTo(apiHttp);
    }

    @Test
    @DisplayName("배포 설정이 관측 창을 덮어쓰면 모듈 값과 같아야 한다")
    void deployedConfigDoesNotDivergeOnDistributionWindows() {
        // api 쪽과 대칭이다. 루트 application.yml 은 두 컨테이너 모두에 얹히므로 batch 에도
        // 같은 위험이 있다 — 지금 루트의 batch 프로파일에 lettuce 키가 없는 것을 여기가 지킨다.
        Properties deployed = yamlProfile(deployedResource(), "batch");
        Properties module = managementTemplate();

        for (String prefix : List.of(EXPIRY_PREFIX, PERCENTILES_PREFIX)) {
            String key = prefix + LETTUCE_DISTRIBUTION_KEY;
            String deployedValue = deployed.getProperty(key);
            if (deployedValue == null) {
                continue;
            }
            assertThat(normalizeCsv(deployedValue))
                .as("배포 설정이 " + key + " 를 덮으면 모듈 값과 같아야 한다")
                .isEqualTo(normalizeCsv(module.getProperty(key)));
        }
    }

    private static FileSystemResource deployedResource() {
        return new FileSystemResource(Path.of("..", "application.yml.example"));
    }

    @Test
    @DisplayName("계측만 켜고 같은 모듈의 런타임 설정 자동설정은 뺀다")
    void runtimeConfigAutoConfigurationStaysExcluded() {
        // infra:redis 를 문 이유는 계측이지만 그 모듈의 imports 에는 CAS 어댑터도 들어 있다.
        // batch 는 그걸 주입받는 곳이 없고 V1 에는 Redis 자체가 없다.
        // 값 전체를 훑지 않는다 — 무관한 키에 적힌 같은 문자열은 아무 일도 하지 않는데
        // 그것까지 통과시킨다(실측: exclude 를 지우고 다른 키로 옮겨도 초록불이었다).
        // 클래스명도 옮겨 적지 않는다 — 리네임되면 yml 의 제외는 조용히 무효가 되지만
        // Boot 는 없는 이름을 예외 없이 무시한다. 타입을 참조해 컴파일에서 깨지게 둔다.
        assertThat(excludedAutoConfigurations())
            .as("빼지 않으면 쓰이지도 않는 RedisRuntimeConfigStore 빈이 batch 컨텍스트에 생긴다")
            .contains(RuntimeConfigRedisAutoConfiguration.class.getName());
    }

    private static List<String> excludedAutoConfigurations() {
        Properties template = yaml(new ClassPathResource("application.yml.example"));
        List<String> excludes = new ArrayList<>();
        // 목록 표기와 콤마 스칼라 표기를 Boot 는 같은 뜻으로 바인딩한다. 인덱스 키만 읽으면
        // 스칼라로 바꿨을 때 제외가 실제로 동작하는데도 이 테스트가 거짓 실패한다.
        String scalar = template.getProperty("spring.autoconfigure.exclude");
        if (scalar != null) {
            excludes.addAll(normalizeCsv(scalar));
        }
        for (int i = 0; ; i++) {
            String value = template.getProperty("spring.autoconfigure.exclude[" + i + "]");
            if (value == null) {
                return excludes;
            }
            excludes.add(value);
        }
    }

    private static List<String> orderingNames() {
        AutoConfiguration declaration = RedisLatencyAutoConfiguration.class.getAnnotation(AutoConfiguration.class);
        assertThat(declaration).isNotNull();

        List<String> names = new ArrayList<>(List.of(declaration.afterName()));
        names.addAll(List.of(declaration.beforeName()));
        assertThat(names).as("순서 선언이 통째로 사라지면 이 테스트가 빈 채로 통과한다").isNotEmpty();
        return names;
    }

    private static Properties managementTemplate() {
        return yaml(new ClassPathResource("management.yml.example"));
    }

}
