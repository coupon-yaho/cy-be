package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
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

    /**
     * 계약이 뒤집혔다. 예전에는 "batch 에서 JPA 층을 뺀다" 였고 근거는 엔티티가 0개라는 것이었다.
     * CY-245 계보가 들어오면서 그 전제가 사라졌고, 만료·회차 경로가 storage 의 JPA 어댑터를 탄다.
     *
     * <p>이제 지키는 것은 <b>반쪽만 켜지지 않는 것</b>이다. 셋 중 하나만 어긋나면 증상이 서로 다른
     * 자리에서 나온다 — 자동설정만 빼면 리포지토리는 생기는데 EntityManagerFactory 가 없어 기동이
     * 죽고, auditing 만 끄면 기동은 되는데 쓰기 시점에 created_at 이 비어 실패한다.
     */
    @Test
    @DisplayName("JPA 를 반쪽만 켜지 않는다 — 자동설정과 auditing 은 한 쌍이다")
    void batchDoesNotHalfEnableJpa() {
        Properties template = applicationTemplate();

        for (int index = 0; template.getProperty("spring.autoconfigure.exclude[" + index + "]") != null;
                index++) {
            assertThat(template.getProperty("spring.autoconfigure.exclude[" + index + "]"))
                .as("JPA 자동설정을 빼면 storage 의 @EnableJpaRepositories 가 만든 리포지토리가"
                        + " EntityManagerFactory 를 못 찾는다. @Enable... 은 자동설정이 아니라"
                        + " 이 목록으로 막히지 않는다")
                .doesNotContain("Jpa");
        }

        // 키 문자열을 옮겨 적지 않는다 — storage 에서 이름을 바꾸면 여기서 컴파일이 깨져야 한다.
        assertThat(template.getProperty(JpaAuditConfig.AUDITING_ENABLED_PROPERTY))
            .as("엔티티가 @CreatedDate 와 AuditingEntityListener 를 쓴다. 여기서 auditing 을 끄면"
                    + " 기동은 되고 쓰기만 실패한다 — 기동 로그에는 아무것도 안 남는다")
            .isNull();
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

    /**
     * <b>Redis 타임아웃의 정본은 {@code redis.yml} 이다</b>(CY-781). {@code spring.config.import}
     * 로 들어온 문서가 선언 문서를 이기므로, {@code application.yml} 에 같은 키를 적으면
     * 에러도 경고도 없이 죽는다 — 실제로 이 자리에 있던 {@code 800ms} 가 Sentinel import 합류
     * 뒤로 죽어 있었고, 최종 바인딩은 {@code 500ms} 였다. 그래서 <b>정본에서 값을 확인하고,
     * 선언 문서에는 그 키가 없다는 것</b>을 함께 고정한다.
     */
    @Test
    @DisplayName("Redis 접속·타임아웃은 redis.yml 이 갖고 application.yml 은 다시 적지 않는다")
    void redisAndSchedulerAreConfigured() {
        Properties template = applicationTemplate();
        Properties redis = load("redis.yml.example");

        assertThat(redis.getProperty("spring.data.redis.timeout"))
            .as("없으면 Lettuce 기본 60초가 걸려 수집 스레드를 그동안 점유한다")
            .isEqualTo("${REDIS_COMMAND_TIMEOUT:500ms}");
        assertThat(redis.getProperty("spring.data.redis.connect-timeout")).isNotBlank();
        assertThat(template.getProperty("spring.data.redis.timeout"))
            .as("여기 적으면 import 에 져서 조용히 죽는다 — 바꿀 값은 redis.yml 로 간다")
            .isNull();
        assertThat(template.getProperty("spring.data.redis.connect-timeout")).isNull();
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

    /**
     * 관측 풀을 켠 JVM 은 합산 상태 순서까지 함께 정해야 한다.
     *
     * <p>순서를 안 적으면 목록에 없는 상태가 "가장 안 심각" 으로 취급되어, 관측 풀 장애가
     * {@code /actuator/health} 를 503 으로 끌어내리고 배치가 죽은 것처럼 보인다. 반대로
     * {@code OBSERVATION_DOWN} 을 {@code UP} 앞에 두면 관측 풀 하나 때문에 인스턴스가
     * 로드밸런서에서 빠진다.
     *
     * <p>이 단언은 원래 api 의 {@code PrometheusScrapeConfigContractTest} 에 있었다. batch 파일의
     * 계약을 남의 모듈이 보고 있었고, 스위치가 {@code ${VAR:기본값}} 표기로 바뀌자 그쪽만 값을
     * 못 읽어 빨간불이 됐다. 계약은 파일을 소유한 모듈이 지킨다.
     */
    @Test
    @DisplayName("관측 풀을 켰으면 합산 상태 순서를 함께 정한다 — 안 그러면 배치가 죽은 것처럼 보인다")
    void observationPoolComesWithAStatusOrder() {
        Properties template = managementTemplate();

        assertThat(enabledDefaultOf(applicationTemplate().getProperty("observation.datasource.enabled")))
            .as("이 테스트는 관측 풀이 켜진 전제다. 끄기로 했다면 group.obs 도 함께 빠져야 한다")
            .isTrue();

        List<String> order = List.of(template
            .getProperty("management.endpoint.health.status.order")
            .split("\\s*,\\s*"));
        int up = order.indexOf("UP");
        assertThat(up)
            .as("status.order 에 UP 이 있어야 나머지 순서를 판정할 수 있다")
            .isNotNegative();
        assertThat(order.indexOf("OBSERVATION_DOWN"))
            .as("OBSERVATION_DOWN 이 UP 보다 뒤여야 관측 풀 장애만으로 인스턴스가"
                + " 로드밸런서에서 빠지지 않는다")
            .isGreaterThan(up);
        assertThat(order.indexOf("DOWN"))
            .as("반대로 DOWN 이 UP 보다 앞이어야 진짜 장애가 200 에 묻히지 않는다")
            .isBetween(0, up - 1);
    }

    /** {@code ${VAR:true}} 표기에서 기본값만 뽑는다. 리터럴 {@code true} 도 받는다. */
    private static boolean enabledDefaultOf(String placeholder) {
        return Boolean.parseBoolean(placeholder.replaceAll("^\\$\\{[^:}]+:([^}]*)\\}$", "$1"));
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
