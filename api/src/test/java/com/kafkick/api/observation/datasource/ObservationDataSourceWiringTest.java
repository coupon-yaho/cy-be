package com.kafkick.api.observation.datasource;

import static org.assertj.core.api.Assertions.assertThat;

import javax.sql.DataSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * api 모듈 클래스패스에서 관측 빈 한 벌이 통째로 올라오는지 본다 — A 가 쓸 주입 형태 그대로다.
 *
 * <p><b>배선만 본다.</b> 풀 크기·read-only·pool-name 같은 값은 storage 의
 * {@code ObservationDataSourceConfigTest} 가 본다. 여기서 같이 보려면 api 가 HikariDataSource 를
 * 컴파일 타임에 참조해야 하는데, 그러면 "어댑터는 런타임에만"(api/build.gradle) 이 무너진다.
 * 이 테스트가 통과하고 값이 틀리는 경우는 storage 테스트가 잡는다.
 *
 * <p><b>storage 타입을 하나도 참조하지 않는다.</b> 설정 클래스를 {@code @Import} 로 지목할 수도
 * 있지만, 운영에서는 컴포넌트 스캔으로 올라온다. 패키지 이름(문자열)만 스캔하면 <b>검증 대상이
 * 운영과 같은 경로</b>가 되고, 덤으로 이 클래스는 storage 타입을 몰라도 된다.
 *
 * <p>storage 를 봐야만 하는 검증은 {@code ObservationHealthContractTest} 하나로 몰아 뒀다.
 *
 * <p>값은 프로퍼티로 직접 준다. 실제 {@code storage.yml} 은 커밋하지 않으므로 그 파일에 기대면
 * 신규 클론과 CI 에서 깨진다. 템플릿에 적힌 값은 storage 의 StorageYamlTemplateTest 가 본다.
 *
 * <p>자동설정은 켜지 않는다. 엔티티가 0개라 JpaAuditConfig(@EnableJpaAuditing)가
 * "JPA metamodel must not be empty" 로 죽고, 이 검증에는 DB 도 필요 없다.
 */
@SpringBootTest(classes = ObservationDataSourceWiringTest.TestApp.class, properties = {
    "spring.datasource.url=jdbc:mysql://localhost:3306/app",
    "spring.datasource.username=app",
    "spring.datasource.password=app",
    "observation.datasource.enabled=true",
    "observation.datasource.url=jdbc:mysql://localhost:3306/app",
    "observation.datasource.username=obs",
    "observation.datasource.password=obs",
})
class ObservationDataSourceWiringTest {

    @Test
    @DisplayName("obs 한정자로 DataSource · JdbcTemplate · 이름 바인딩 · 트랜잭션 매니저가 모두 잡힌다")
    void observationBeansAreAvailableInApi(
        @Qualifier("obs") DataSource observationDataSource,
        @Qualifier("obs") JdbcTemplate observationJdbcTemplate,
        @Qualifier("obs") NamedParameterJdbcTemplate observationNamedParameterJdbcTemplate,
        @Qualifier("obs") PlatformTransactionManager observationTransactionManager,
        @Qualifier("observationDataSource") DataSource byName
    ) {
        assertThat(observationDataSource).isSameAs(byName);
        assertThat(observationJdbcTemplate.getDataSource()).isSameAs(observationDataSource);
        assertThat(observationNamedParameterJdbcTemplate.getJdbcTemplate()).isSameAs(observationJdbcTemplate);
        assertThat(observationTransactionManager).isNotNull();
    }

    /**
     * 한정자를 빠뜨린 주입이 관측 풀로 가면 안 된다. 그 반대 — 관측 조회가 운영 풀로 새는 것 — 은
     * 에러 없이 일어나므로 여기가 유일한 그물이다.
     */
    @Test
    @DisplayName("타입만으로 주입하면 운영 풀이 잡힌다 — @Primary 가 빠지면 여기서 걸린다")
    void primaryStaysOnMainPool(
        @Autowired DataSource byType,
        @Autowired JdbcTemplate jdbcTemplateByType,
        @Qualifier("mainDataSource") DataSource mainDataSource,
        @Qualifier("obs") DataSource observationDataSource
    ) {
        assertThat(byType).isSameAs(mainDataSource).isNotSameAs(observationDataSource);
        assertThat(jdbcTemplateByType.getDataSource()).isSameAs(mainDataSource);
    }

    /**
     * 같은 패키지의 JPA 설정은 제외한다 — 이 검증은 DataSource 배선만 확인하며 엔티티나
     * EntityManagerFactory를 올리지 않는다. 타입이 아니라 이름 패턴으로 거르는 것은 storage 에
     * 대한 컴파일 의존을 만들지 않기 위해서다.
     */
    @SpringBootConfiguration
    @ComponentScan(
        basePackages = "com.kafkick.storage.db.config",
        excludeFilters = @Filter(
            type = FilterType.REGEX,
            pattern = ".*(JpaAuditConfig|CouponStorageConfig)"))
    static class TestApp {
    }
}
