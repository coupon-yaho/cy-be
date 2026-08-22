package com.kafkick.storage.db.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

/**
 * 템플릿에 적힌 값을 검증한다. 실제 {@code storage.yml} 은 커밋하지 않으므로 여기서 읽으면
 * 신규 클론과 CI 에서 깨진다 — CY-09 가 관리 포트에서 내린 결론과 같은 이유다.
 *
 * <p>값이 빈까지 흘러가는지는 {@link ObservationDataSourceConfigTest} 가 본다. 여기는
 * "템플릿이 그 값을 그 키에 담고 있는가" 만 본다. 둘이 합쳐져야 한 줄이 이어진다.
 */
class StorageYamlTemplateTest {

    private static final String OBS = "observation.datasource.";

    private final Properties template = template();

    @Test
    @DisplayName("관측 풀은 2개다 — 집계가 커넥션을 오래 잡는 동안 폴링이 굶지 않을 최소치")
    void observationPoolIsSmallButNotOne() {
        assertThat(template.getProperty(OBS + "hikari.maximum-pool-size")).isEqualTo("2");
    }

    @Test
    @DisplayName("관측 풀은 읽기 전용이다 — 마이그레이션·검증 배치가 흘러들어오면 여기서 막힌다")
    void observationPoolIsReadOnly() {
        assertThat(template.getProperty(OBS + "hikari.read-only")).isEqualTo("true");
    }

    @Test
    @DisplayName("커넥션을 못 얻으면 대기가 아니라 실패다 — 대기하면 수집 주기가 밀린다")
    void observationPoolFailsFastOnAcquire() {
        assertThat(template.getProperty(OBS + "hikari.connection-timeout")).isEqualTo("2000");
    }

    /**
     * 클라이언트 타임아웃만 두면 커넥션만 회수되고 MySQL 은 그 쿼리를 끝까지 돌린다.
     * 버퍼풀을 갈아엎는 건 그쪽이라, 서버 상한이 방어의 나머지 절반이다.
     */
    @Test
    @DisplayName("타임아웃은 두 겹이다 — JDBC 와 서버 세션 양쪽")
    void observationQueryTimeoutIsEnforcedOnBothSides() {
        assertThat(template.getProperty(OBS + "query-timeout")).isEqualTo("3s");
        assertThat(template.getProperty(OBS + "hikari.connection-init-sql"))
            .isEqualTo("SET SESSION max_execution_time = 3000");
    }

    @Test
    @DisplayName("풀 이름은 JVM 별로 바꿀 수 있다 — pool 태그가 섞이면 사용량을 못 가른다")
    void observationPoolNameIsOverridable() {
        assertThat(template.getProperty(OBS + "hikari.pool-name")).isEqualTo("${OBS_POOL_NAME:obs-pool}");
    }

    /**
     * 운영 계정으로 폴백하면 관측이 read-write 로 붙는다. hikari 의 read-only 는 세션 속성이라
     * 권한 분리가 아니다. 빈 기본값이어야 @NotBlank 가 기동 시점에 막는다 —
     * 기본값 자체를 지우면 미해결 플레이스홀더가 문자열로 바인딩돼 그대로 뜬다.
     */
    @Test
    @DisplayName("관측 계정은 운영 계정으로 폴백하지 않는다")
    void observationAccountNeverFallsBackToMain() {
        assertThat(template.getProperty(OBS + "username")).isEqualTo("${DB_OBS_USERNAME:}");
        assertThat(template.getProperty(OBS + "password")).isEqualTo("${DB_OBS_PASSWORD:}");
    }

    /**
     * 이게 없으면 MySQL 이 응답을 멈췄을 때 커넥션이 무한 대기하고 풀 전체가 묶인다.
     * 관측 풀만 3초에 끊기고 발급 경로가 무방비인 상태를 막는 값이다.
     */
    @Test
    @DisplayName("운영 URL 에 접속·소켓 타임아웃이 있다")
    void mainUrlBoundsHungConnections() {
        String url = template.getProperty("spring.datasource.url");

        assertThat(url).contains("connectTimeout=${DB_CONNECT_TIMEOUT_MS:3000}");
        assertThat(url).contains("socketTimeout=${DB_SOCKET_TIMEOUT_MS:60000}");
    }

    /**
     * v1 은 락 대기가 정상 동작이다. socketTimeout 이 innodb_lock_wait_timeout(기본 50s)보다 짧으면
     * 정상 대기가 통신 오류로 끊긴다. 관측은 락을 기다릴 일이 없어 훨씬 짧게 잡는다.
     */
    @Test
    @DisplayName("관측 소켓 타임아웃이 운영보다 짧다 — 락 대기를 기다릴 이유가 없다")
    void observationUrlFailsFasterThanMain() {
        assertThat(template.getProperty(OBS + "url"))
            .contains("connectTimeout=${OBS_CONNECT_TIMEOUT_MS:2000}")
            .contains("socketTimeout=${OBS_SOCKET_TIMEOUT_MS:5000}");
        assertThat(defaultOf(template.getProperty(OBS + "url"), "socketTimeout"))
            .isLessThan(defaultOf(template.getProperty("spring.datasource.url"), "socketTimeout"));
    }

    @Test
    @DisplayName("운영 풀은 관측 설정에 오염되지 않는다")
    void mainPoolStaysWritable() {
        assertThat(template.getProperty("spring.datasource.hikari.read-only")).isNull();
        assertThat(template.getProperty("spring.datasource.hikari.connection-init-sql")).isNull();
    }

    /**
     * 이 파일은 {@code spring.config.import} 로 들어간다. import 로 들어온 문서는 그것을 선언한
     * 문서보다 <b>우선</b>하므로, 여기 적힌 키는 각 모듈이 자기 파일에 적은 값을 에러도 경고도 없이
     * 이긴다 — batch 의 {@code maximum-pool-size: 4} 와 {@code flyway.enabled: false} 가 실제로
     * 그렇게 무시되고 10 · true 로 떴다(실측).
     *
     * <p>그래서 <b>모듈마다 값이 달라야 하는 키는 이 파일에 두지 않는다.</b> 여기에 다시 추가되면
     * 같은 회귀가 재발하므로 없음을 고정한다. 누가 대신 선언하는지는 api 의
     * {@code ConnectionBudgetOwnershipTest} 가 본다 — 둘이 합쳐져야 한 줄이 이어진다.
     */
    @Test
    @DisplayName("모듈마다 달라야 하는 키는 공유 파일에 없다 — 있으면 모듈 선언을 조용히 이긴다")
    void perModuleKeysAreNotDeclaredHere() {
        assertThat(template.getProperty("spring.datasource.hikari.maximum-pool-size")).isNull();
        assertThat(template.getProperty("spring.flyway.enabled")).isNull();
    }

    /** enabled 만 모듈이 정한다. 나머지 Flyway 설정은 모듈이 달라질 이유가 없어 여기 남는다. */
    @Test
    @DisplayName("Flyway 의 나머지 설정은 공유다")
    void flywaySettingsOtherThanEnabledStayShared() {
        assertThat(template.getProperty("spring.flyway.locations")).isEqualTo("classpath:db/migration");
        assertThat(template.getProperty("spring.flyway.clean-disabled")).isEqualTo("true");
    }

    /** {@code socketTimeout=${VAR:12345}} 에서 기본값 12345 만 뽑는다. */
    private static int defaultOf(String url, String parameter) {
        Matcher matcher = Pattern.compile(parameter + "=\\$\\{[^:]+:(\\d+)}").matcher(url);
        assertThat(matcher.find()).isTrue();
        return Integer.parseInt(matcher.group(1));
    }

    private static Properties template() {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("storage.yml.example"));
        return yaml.getObject();
    }
}
