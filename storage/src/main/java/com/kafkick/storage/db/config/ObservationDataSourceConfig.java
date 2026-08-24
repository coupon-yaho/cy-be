package com.kafkick.storage.db.config;

import javax.sql.DataSource;

import jakarta.persistence.EntityManagerFactory;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails;
import org.springframework.boot.jdbc.autoconfigure.JdbcProperties;
import org.springframework.boot.transaction.autoconfigure.TransactionManagerCustomizers;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.StringUtils;

import com.zaxxer.hikari.HikariDataSource;

/**
 * 대시보드가 읽는 집계 조회를 운영 풀에서 떼어 낸다. 검증 배치(asof_state 생성·V1~V6)는 대상이 아니다.
 *
 * <p>운영 풀 10개 중 관측이 상시 점유분을 가져가면 그만큼 사라지는데, 하필 그 풀이 v1(DB Lock)의
 * 병목이라 관측 도구가 측정 대상을 바꿔 버린다.
 *
 * <p>다만 풀만 분리했을 뿐 MySQL 의 CPU·I/O·버퍼풀은 여전히 공유다. 이 설정은 절반의 방어이고,
 * 나머지 절반은 관측 쿼리 자체가 무겁지 않은 것(OBS-16)이다.
 *
 * <h2>켠 모듈에서만 동작한다</h2>
 *
 * storage 를 얹은 모듈은 이 설정을 자동으로 물려받는다. 그대로 두면 관측을 쓰지 않는 batch 도
 * 관측 풀을 만들고, {@link ObservationDbProperties} 의 {@code @NotBlank} 때문에 관측 계정 없이는
 * <b>기동조차 못 한다.</b> 관측을 위한 설정이 관측과 무관한 모듈을 멈춰 세우는 셈이라, 이 티켓이
 * 피하려던 것과 같은 사고다.
 *
 * <p>그래서 {@code observation.datasource.enabled} 로 <b>옵트인</b>한다. 대시보드를 그리는 api 만
 * 켠다. 끈 모듈에서는 이 설정이 통째로 건너뛰어지고 Boot 자동설정이 원래 하던 일을 그대로 한다.
 *
 * <p><b>관측 빈만이 아니라 이 클래스 전체가 조건부인 이유</b> — 아래 빈들은 관측용만이 아니다.
 * DataSource 를 직접 정의하는 순간 Boot 가 만들어 주던 JdbcTemplate · NamedParameterJdbcTemplate ·
 * TransactionManager 가 물러나므로 운영용까지 여기서 만들고 있다. 관측을 안 쓰는 모듈에서는
 * 그 대체 자체가 불필요하다 — 거기서는 DataSource 가 하나뿐이라 자동설정으로 충분하다.
 *
 * <p><b>반대 방향 실패</b> — 켜야 할 모듈이 스위치를 빠뜨리면 관측 빈이 통째로 없다. api 는
 * {@code management.yml} 의 {@code group.obs} 가 {@code obsDb} 기여자를 찾지 못해 기동에서
 * 걸리지만, 그 그룹을 쓰지 않는 모듈에는 그런 그물이 없다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
@EnableConfigurationProperties({ MainDbProperties.class, ObservationDbProperties.class, JdbcProperties.class })
public class ObservationDataSourceConfig {

    /**
     * @Primary 가 없으면 JPA·Flyway 자동설정이 DataSource 후보 둘을 만나 기동에 실패한다.
     *
     * <p>관측 쪽과 달리 {@code @NotBlank} 를 쓰지 않는다 — 운영 풀은 접속 정보 자동 주입
     * (Testcontainers·Compose)을 <b>따르는 게 정상</b>이라, 프로퍼티만 보고 검증하면 컨테이너로 뜨는
     * 테스트가 전부 죽는다. 그래서 자동 주입까지 반영한 <b>최종 값</b>을 여기서 확인한다.
     */
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource.hikari")
    public HikariDataSource mainDataSource(
        MainDbProperties properties,
        ObjectProvider<JdbcConnectionDetails> connectionDetails
    ) {
        JdbcConnectionDetails details = connectionDetails.getIfAvailable();
        HikariDataSource dataSource = hikari(
            details != null ? details.getDriverClassName() : properties.driverClassName(),
            requireResolved(details != null ? details.getJdbcUrl() : properties.url(), "url"));
        dataSource.setUsername(
            requireResolved(details != null ? details.getUsername() : properties.username(), "username"));
        dataSource.setPassword(details != null ? details.getPassword() : properties.password());
        return dataSource;
    }

    /**
     * storage.yml 은 커밋하지 않는다. 낡은 파일을 받아 키가 비어 있으면 Hikari 는 첫 조회까지 풀을 열지
     * 않으므로, 기동은 성공하고 첫 요청에서야 죽는다. 관측 풀({@code @NotBlank})과 같은 시점에 죽인다.
     */
    private static String requireResolved(String value, String key) {
        if (!StringUtils.hasText(value)) {
            throw new IllegalStateException(
                "spring.datasource." + key + " 가 비어 있다. storage.yml 의 datasource 블록을 확인한다.");
        }
        return value;
    }

    /**
     * 풀 크기 · read-only · 짧은 타임아웃은 storage.yml 이 정한다. 여기서 기본값을 박으면 두 곳이 어긋난다.
     *
     * <p>접속 정보는 {@code observation.datasource} 값<b>만</b> 쓴다 — URL 도 계정도 자동 주입을
     * 따르지 않는다. 자동 주입에 맡기면 컨테이너·Compose 의 슈퍼유저로 붙어 버려서 "관측은 SELECT
     * 전용 계정" 이 조용히 무효가 된다. 셋 다 {@code @NotBlank} 라 비면 기동에서 죽는다.
     *
     * <p>컨테이너를 쓰는 테스트는 값을 <b>명시적으로</b> 꽂는다 —
     * {@code MySqlContainerConfig} 의 {@code DynamicPropertyRegistrar} 가 그 자리다.
     */
    @Bean
    @Qualifier("obs")
    @ConfigurationProperties("observation.datasource.hikari")
    public HikariDataSource observationDataSource(ObservationDbProperties properties) {
        HikariDataSource dataSource = hikari(properties.driverClassName(), properties.url());
        dataSource.setUsername(properties.username());
        dataSource.setPassword(properties.password());
        return dataSource;
    }

    /**
     * JdbcOperations 빈이 하나라도 있으면 자동설정의 JdbcTemplate 이 물러난다. 운영용까지 여기서
     * 같이 정의해야 기존 주입 지점이 그대로 산다.
     *
     * <p>{@code spring.jdbc.template.*} 를 직접 적용하는 것도 자동설정이 하던 일이다. 안 하면
     * 그 키들이 조용히 무시된다 — 나중에 운영 풀 타임아웃을 설정으로 조이려는 사람이 값을 적어도
     * 아무 일도 일어나지 않는다. 자동설정이 하던 것과 같은 순서로 적용한다.
     */
    @Bean
    @Primary
    public JdbcTemplate jdbcTemplate(DataSource dataSource, JdbcProperties jdbcProperties) {
        JdbcProperties.Template template = jdbcProperties.getTemplate();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setFetchSize(template.getFetchSize());
        jdbcTemplate.setMaxRows(template.getMaxRows());
        if (template.getQueryTimeout() != null) {
            jdbcTemplate.setQueryTimeout((int) template.getQueryTimeout().toSeconds());
        }
        return jdbcTemplate;
    }

    /**
     * 관측 쿼리는 오래 걸리면 대기가 아니라 실패여야 한다. 서버 쪽 {@code max_execution_time} 은
     * storage.yml 의 connection-init-sql 이 건다 — 클라이언트 타임아웃만 두면 커넥션만 놓고 MySQL 은
     * 그 쿼리를 끝까지 돌린다. 버퍼풀을 갈아엎는 건 그쪽이다.
     *
     * <p>JDBC API 가 초 단위라 올림한다. {@code 2500ms} 를 내림하면 설정보다 빨리 끊겨서, 설정한
     * 사람이 모르는 사이에 더 조인다. 그래서 두 겹의 상한은 값이 다를 수 있다 —
     * 정확한 ms 제어는 서버 쪽이 하고, JDBC 는 그보다 느슨한 안전망이다.
     */
    @Bean
    @Qualifier("obs")
    public JdbcTemplate observationJdbcTemplate(
        @Qualifier("obs") DataSource observationDataSource,
        ObservationDbProperties properties
    ) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(observationDataSource);
        jdbcTemplate.setQueryTimeout((int) Math.ceil(properties.queryTimeout().toMillis() / 1000.0));
        return jdbcTemplate;
    }

    /**
     * NamedParameterJdbcOperations 빈이 하나라도 있으면 자동설정이 물러난다. 관측용만 정의하면
     * 운영용이 사라지므로 둘 다 여기서 만든다.
     *
     * <p>관측용이 없으면 이름 바인딩을 쓰는 관측 쿼리는 한정자를 붙일 대상 자체가 없어
     * 조용히 운영 풀로 나간다.
     */
    @Bean
    @Primary
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate(JdbcTemplate jdbcTemplate) {
        return new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    @Bean
    @Qualifier("obs")
    public NamedParameterJdbcTemplate observationNamedParameterJdbcTemplate(
        @Qualifier("obs") JdbcTemplate observationJdbcTemplate
    ) {
        return new NamedParameterJdbcTemplate(observationJdbcTemplate);
    }

    /**
     * TransactionManager 를 하나라도 직접 정의하면 JPA 자동설정의 매니저가 물러난다. JdbcTemplate 과
     * 같은 구조라 운영용을 여기서 함께 정의한다.
     *
     * <p>엔티티가 생기기 전에는 EntityManagerFactory 가 없다. @ConditionalOnBean 은 사용자 설정에서
     * 자동설정 빈을 대상으로 하면 평가 순서 때문에 신뢰할 수 없어 런타임에 직접 고른다.
     *
     * <p>커스터마이저를 직접 적용하는 것도 자동설정이 하던 일이다. 안 하면
     * {@code spring.transaction.default-timeout} 이 조용히 무시된다 — 트랜잭션 전체를 시간으로
     * 묶는 유일한 손잡이라, 무한 대기를 막으려는 사람이 값을 적어도 아무 일도 일어나지 않는다.
     */
    @Bean
    @Primary
    public PlatformTransactionManager transactionManager(
        DataSource dataSource,
        ObjectProvider<EntityManagerFactory> entityManagerFactory,
        ObjectProvider<TransactionManagerCustomizers> customizers
    ) {
        EntityManagerFactory factory = entityManagerFactory.getIfAvailable();
        PlatformTransactionManager transactionManager = factory != null
            ? new JpaTransactionManager(factory) : new JdbcTransactionManager(dataSource);
        customizers.ifAvailable(it -> it.customize(transactionManager));
        return transactionManager;
    }

    /**
     * 관측 조회를 한 트랜잭션으로 묶으려면 이 매니저를 이름으로 지목해야 한다 —
     * {@code @Transactional(transactionManager = "observationTransactionManager", readOnly = true)}.
     *
     * <p>이름을 빠뜨리면 @Primary(운영 풀)에 트랜잭션이 열리고 관측 쿼리는 그 밖에서 문장마다
     * 따로 나간다. 운영 풀을 점유하면서 3축이 서로 다른 시점이 되는데 에러는 나지 않는다.
     */
    @Bean
    @Qualifier("obs")
    public PlatformTransactionManager observationTransactionManager(
        @Qualifier("obs") DataSource observationDataSource,
        ObjectProvider<TransactionManagerCustomizers> customizers
    ) {
        JdbcTransactionManager transactionManager = new JdbcTransactionManager(observationDataSource);
        customizers.ifAvailable(it -> it.customize(transactionManager));
        return transactionManager;
    }

    /**
     * DataSource 빈을 직접 정의하면 접속 정보를 덮어써 주던 Boot 의 BeanPostProcessor 까지 물러난다.
     * <b>운영 풀</b>은 그 자리를 대신하려고 {@code JdbcConnectionDetails} 를 직접 반영한다 —
     * Testcontainers 의 {@code @ServiceConnection} 과 Compose 연동이 그대로 살아 있어야 하기 때문이다.
     *
     * <p>관측 풀은 반대다. 자동 주입을 <b>따르지 않고</b> 설정 값만 쓴다.
     */
    private static HikariDataSource hikari(String driverClassName, String url) {
        HikariDataSource dataSource = new HikariDataSource();
        if (driverClassName != null) {
            dataSource.setDriverClassName(driverClassName);
        }
        dataSource.setJdbcUrl(url);
        return dataSource;
    }
}
