// 운영 DB 풀과 그에 딸린 기본 빈들입니다.
package com.kafkick.storage.db.config;

import javax.sql.DataSource;

import jakarta.persistence.EntityManagerFactory;

import org.springframework.beans.factory.ObjectProvider;
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
 * 운영 풀과, 그것을 직접 정의했기 때문에 우리가 대신 만들어야 하는 기본 빈들.
 *
 * <h2>왜 {@link ObservationDataSourceConfig} 에서 떼어 냈는가</h2>
 *
 * <p>예전에는 이 빈들이 관측 설정 안에 있었고, 그 설정 전체가
 * {@code @ConditionalOnProperty("observation.datasource.enabled")} 였다. 그래서 <b>관측을 끄면
 * 운영 풀 정의까지 함께 사라졌다.</b>
 *
 * <p>그 스위치는 사고 스위치가 아니라 {@code application.yml.example} 이
 * <i>"유휴 시간처럼 관측이 필요 없는 구간을 위한 스위치"</i> 라고 적어 둔 <b>정상 운영 스위치</b>다.
 * 그런데 batch 쪽 {@code BatchJobRepositoryConfig} 가 이 풀을 이름으로 물고 있어, 끄는 순간
 * Spring Batch 가 {@code ResourcelessJobRepository} 로 떨어졌다 — {@code JOB_INST_UN} 유니크
 * 제약에 INSERT 가 가지 않으니 <b>두 노드가 같은 파라미터로 각자 잡을 시작할 수 있는 상태</b>가
 * 되고, 만료 배치라면 같은 회차를 두 번 훑는다. <b>예외도 로그도 없다.</b>
 *
 * <p>즉 관측이라는 <b>부가 기능의 스위치에 배치의 실행 1회 불변식이 걸려</b> 있었다. 결이 다른
 * 두 관심사라 파일을 갈랐다. 관측 빈은 여전히 옵트인이고, 운영 풀은 <b>항상</b> 있다.
 * 그 분리를 {@code batch} 의 {@code BatchMetadataWithoutObservationTest} 가 고정한다.
 *
 * <h2>왜 자동설정에 맡기지 않는가</h2>
 *
 * <p>DataSource 를 직접 정의하는 순간 Boot 가 만들어 주던 {@code JdbcTemplate} ·
 * {@code NamedParameterJdbcTemplate} · {@code TransactionManager} 가 전부 물러난다. 그래서 그
 * 셋을 여기서 함께 만든다 — 자동설정이 하던 것과 <b>같은 순서로</b> 설정을 적용해야
 * {@code spring.jdbc.template.*} · {@code spring.transaction.default-timeout} 이 조용히
 * 무시되지 않는다.
 *
 * <p><b>대가</b> — 관측을 안 쓰는 모듈에서도 이 대체가 일어난다. 예전 주석은 그것을 "불필요" 라고
 * 적고 조건부로 두었는데, 그 대가가 위의 배치 사고였다. 자동설정과 같은 일을 하는 코드를 항상
 * 도는 쪽이, 스위치 하나로 배선이 통째로 갈리는 쪽보다 낫다.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({ MainDbProperties.class, JdbcProperties.class })
public class MainDataSourceConfig {

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
     * DataSource 빈을 직접 정의하면 접속 정보를 덮어써 주던 Boot 의 BeanPostProcessor 까지 물러난다.
     * <b>운영 풀</b>은 그 자리를 대신하려고 {@code JdbcConnectionDetails} 를 직접 반영한다 —
     * Testcontainers 의 {@code @ServiceConnection} 과 Compose 연동이 그대로 살아 있어야 하기 때문이다.
     *
     * <p>관측 풀은 반대다. 자동 주입을 <b>따르지 않고</b> 설정 값만 쓴다.
     */
    static HikariDataSource hikari(String driverClassName, String url) {
        HikariDataSource dataSource = new HikariDataSource();
        if (driverClassName != null) {
            dataSource.setDriverClassName(driverClassName);
        }
        dataSource.setJdbcUrl(url);
        return dataSource;
    }
}
