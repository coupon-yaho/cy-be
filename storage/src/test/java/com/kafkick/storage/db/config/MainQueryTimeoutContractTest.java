package com.kafkick.storage.db.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;

import com.zaxxer.hikari.HikariDataSource;

/**
 * [CY-488] "운영 풀 질의 타임아웃은 값을 두지 않는다" 는 판단이 <b>두 파일에 걸쳐</b> 있다 —
 * {@code storage.yml.example} 이 키를 안 적는 것과, {@link MainDataSourceConfig} 가 값이 없으면
 * 손잡이를 안 거는 것. 둘을 각각 보는 테스트로는 계약이 안 지켜진다: 템플릿에만 키가 생겨도,
 * 코드만 기본값을 박아도 각자는 초록불이다.
 *
 * <p>그래서 여기서는 <b>템플릿을 읽어 그대로 컨텍스트에 먹이고, 빈에서 결과를 확인한다.</b>
 * 실제 {@code storage.yml} 은 커밋하지 않으므로 템플릿을 읽는다 —
 * {@link StorageYamlTemplateTest} 와 같은 이유다.
 *
 * <p>근거(실측 수치 포함)는 {@link MainDataSourceConfig#jdbcTemplate} 의 주석에 있다.
 * 값을 넣으려는 사람은 그것을 읽고 이 테스트를 함께 고쳐야 한다.
 */
class MainQueryTimeoutContractTest {

    /** JdbcTemplate 이 setQueryTimeout 을 한 번도 안 받았을 때의 값. 즉 "우리가 안 걸었다". */
    private static final int NOT_APPLIED = -1;

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(MainDataSourceConfig.class)
        .withPropertyValues(templateAsPropertyValues());

    @Test
    @DisplayName("템플릿대로 뜨면 운영 템플릿에 질의 타임아웃이 안 걸린다 — 값을 두지 않기로 한 판단")
    void mainTemplateCarriesNoQueryTimeoutAsShipped() {
        runner.run(context -> assertThat(
            context.getBean("jdbcTemplate", JdbcTemplate.class).getQueryTimeout()).isEqualTo(NOT_APPLIED));
    }

    /**
     * 서버 쪽 상한은 운영 풀이 하는 일(쓰기)을 못 막고 락 대기만 끊는다 — 실측표는 코드 주석에 있다.
     * 관측 풀과 형태를 맞추려고 여기에 같은 줄을 붙이면 v1 의 정상 발급이 죽는다.
     */
    @Test
    @DisplayName("운영 풀에는 서버 쪽 실행시간 상한도 없다")
    void mainPoolCarriesNoServerSideCap() {
        runner.run(context -> assertThat(
            context.getBean("mainDataSource", HikariDataSource.class).getConnectionInitSql()).isNull());
    }

    /**
     * JDBC 에서 0 은 '제한 없음' 이다(실측: setQueryTimeout(0) + SELECT SLEEP(3) → 3.0s 완주).
     * 순정 Boot 처럼 내리면 조이려던 설정이 푸는 설정으로 뒤집힌다.
     */
    @Test
    @DisplayName("1초 미만 타임아웃은 0(무제한)이 아니라 1초로 올라간다")
    void subSecondTimeoutRoundsUpInsteadOfDisabling() {
        runner.withPropertyValues("spring.jdbc.template.query-timeout=500ms")
            .run(context -> assertThat(
                context.getBean("jdbcTemplate", JdbcTemplate.class).getQueryTimeout()).isEqualTo(1));
    }

    /** 초로 안 떨어지는 값도 내리지 않는다 — 설정한 사람이 모르는 사이에 더 조이면 안 된다. */
    @Test
    @DisplayName("2500ms 는 2초가 아니라 3초다")
    void fractionalSecondTimeoutRoundsUp() {
        runner.withPropertyValues("spring.jdbc.template.query-timeout=2500ms")
            .run(context -> assertThat(
                context.getBean("jdbcTemplate", JdbcTemplate.class).getQueryTimeout()).isEqualTo(3));
    }

    private static String[] templateAsPropertyValues() {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new Resource[] { new ClassPathResource("storage.yml.example") });
        Properties properties = yaml.getObject();
        assertThat(properties).isNotNull();
        return properties.stringPropertyNames().stream()
            .map(key -> key + "=" + properties.getProperty(key))
            .toArray(String[]::new);
    }
}
