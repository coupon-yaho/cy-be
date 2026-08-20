package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import com.kafkick.storage.db.config.JpaAuditConfig;

/**
 * 커밋되는 설정 템플릿이 도메인 Gauge 의 전제를 유지하는지 본다. 스위치 하나가 빠지면 앱은
 * 정상 기동하고 로그도 없이 지표만 사라진다.
 */
class DomainGaugeConfigContractTest {

    @Test
    @DisplayName("도메인 Gauge 를 켜면 관측 전용 풀도 함께 켜져 있어야 한다")
    void domainGaugeRequiresObservationPool() {
        Properties template = applicationTemplate();

        assertThat(template.getProperty("observation.domain-gauge.enabled"))
            .isEqualTo("${DOMAIN_GAUGE_ENABLED:true}");
        assertThat(template.getProperty("observation.datasource.enabled"))
            .as("관측 계정이 없는 환경은 이 스위치를 꺼야 batch 가 뜬다 — 고정값이면 끌 방법이 없다")
            .isEqualTo("${OBSERVATION_DATASOURCE_ENABLED:true}");
    }

    @Test
    @DisplayName("관측 풀을 켜려면 batch 에서 JPA 층을 빼야 한다 — 엔티티가 0개라 기동이 멈춘다")
    void batchStaysJpaFree() {
        Properties template = applicationTemplate();

        assertThat(template.getProperty("spring.autoconfigure.exclude[0]"))
            .isEqualTo("org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration");
        assertThat(template.getProperty("spring.autoconfigure.exclude[1]"))
            .isEqualTo("org.springframework.boot.data.jpa.autoconfigure.DataJpaRepositoriesAutoConfiguration");
        // 키 문자열을 옮겨 적지 않는다 — storage 에서 이름을 바꾸면 여기서 컴파일이 깨져야 한다.
        assertThat(template.getProperty(JpaAuditConfig.AUDITING_ENABLED_PROPERTY)).isEqualTo("false");
    }

    @Test
    @DisplayName("무거운 집계는 재고보다 느린 주기로 돈다")
    void aggregateRunsSlowerThanStock() {
        Properties template = applicationTemplate();

        assertThat(template.getProperty("observation.domain-gauge.interval-ms"))
            .isEqualTo("${DOMAIN_GAUGE_INTERVAL_MS:1000}");
        // 실측: 한 회차 300만 행 집계가 약 1.5초다. 주기가 촘촘하면 관측이 v1 측정을 오염시킨다.
        assertThat(defaultOf(template.getProperty("observation.domain-gauge.aggregate-interval-ms")))
            .as("집계 주기는 재고 주기보다 훨씬 느려야 한다")
            .isGreaterThanOrEqualTo(10 * defaultOf(template.getProperty("observation.domain-gauge.interval-ms")));
        assertThat(template.getProperty("observation.domain-gauge.coupon-id"))
            .as("회차를 고정할 손잡이가 있어야 부하 중 새 회차로 시계열이 점프하지 않는다")
            .isEqualTo("${DOMAIN_GAUGE_COUPON_ID:}");
    }

    @Test
    @DisplayName("설정으로 조일 수 있어야 하는 값들이 템플릿에 드러나 있다")
    void tunableThresholdsAreVisibleInTheTemplate() {
        Properties template = applicationTemplate();

        assertThat(template.getProperty("observation.domain-gauge.consecutive-failure-alarm"))
            .as("코드에 박으면 부하 중에 재기동 없이 못 바꾼다")
            .isEqualTo("${DOMAIN_GAUGE_FAILURE_ALARM:3}");
        assertThat(template.getProperty("observation.consistency.severity.warn-threshold"))
            .as("키가 어디에도 없으면 운영자가 코드를 뒤져야 한다")
            .isNotBlank();
        assertThat(template.getProperty("observation.consistency.severity.critical-threshold"))
            .isNotBlank();
    }

    @Test
    @DisplayName("Redis 접속·타임아웃과 스케줄러 풀이 템플릿에 있다")
    void redisAndSchedulerAreConfigured() {
        Properties template = applicationTemplate();

        assertThat(template.getProperty("spring.data.redis.timeout"))
            .as("없으면 Lettuce 기본 60초가 걸려 수집 스레드를 그동안 점유한다")
            .isEqualTo("${REDIS_TIMEOUT_MS:800}ms");
        assertThat(template.getProperty("spring.data.redis.connect-timeout")).isNotBlank();
        assertThat(defaultOf(template.getProperty("spring.task.scheduling.pool.size")))
            .as("풀이 1이면 느린 집계가 1초 주기 재고 갱신을 막는다")
            .isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("Redis 헬스 기여자를 끈다 — V1 에는 Redis 가 없는데 스타터만으로 붙는다")
    void redisHealthContributorIsDisabled() {
        assertThat(managementTemplate().getProperty("management.health.redis.enabled"))
            .isEqualTo("false");
    }

    @Test
    @DisplayName("obs 그룹은 두되 멤버십 검증은 끈다 — 안 그러면 관측 스위치가 탈출구가 못 된다")
    void observationGroupDoesNotBlockTheEscapeHatch() {
        Properties template = managementTemplate();

        // storage 의 ObservationHealthStatusOrderCheck 가 관측을 켠 JVM 에 이 그룹을 요구한다.
        assertThat(template.getProperty("management.endpoint.health.group.obs.include"))
            .isEqualTo("obsDb");
        // 그런데 관측을 끄면 그 기여자가 사라진다. 검증이 켜져 있으면 그 순간 기동이 실패한다.
        assertThat(template.getProperty("management.endpoint.health.validate-group-membership"))
            .isEqualTo("false");
    }

    /** {@code ${VAR:1000}} 표기에서 기본값만 뽑는다. */
    private static int defaultOf(String placeholder) {
        return Integer.parseInt(placeholder.replaceAll(".*:(\\d+)}", "$1"));
    }

    private static Properties applicationTemplate() {
        return load("application.yml.example");
    }

    private static Properties managementTemplate() {
        return load("management.yml.example");
    }

    private static Properties load(String name) {
        YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
        factory.setResources(new ClassPathResource(name));
        return factory.getObject();
    }
}
