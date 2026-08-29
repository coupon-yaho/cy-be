package com.kafkick.storage.db.config;

import javax.sql.DataSource;


import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.transaction.autoconfigure.TransactionManagerCustomizers;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

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
 * 켠다. 끈 모듈에서는 <b>관측 빈만</b> 건너뛰어진다.
 *
 * <p><b>[CY-338] 예전에는 이 자리에 "Boot 자동설정이 원래 하던 일을 그대로 한다" 고 적혀
 * 있었는데 이제 사실이 아니다.</b> 운영 풀을 {@link MainDataSourceConfig} 로 떼면서 그쪽이
 * 조건 없이 항상 붙고, 그래서 Boot 의 DataSource·JdbcTemplate·TransactionManager 자동설정은
 * <b>스위치와 무관하게 늘 물러난다.</b> 그 대가를 알고 고른 것이다 — 이유는 그 파일에 적었다.
 *
 * <p><b>[CY-338] 운영 풀은 여기 없다.</b> 예전에는 운영 {@code DataSource} · {@code JdbcTemplate} ·
 * {@code TransactionManager} 까지 이 파일이 갖고 있었고, 그래서 <b>관측을 끄면 운영 풀 정의가
 * 함께 사라졌다.</b> batch 의 {@code BatchJobRepositoryConfig} 가 그 풀을 물고 있어서, 정상
 * 운영 스위치를 끄는 것만으로 Spring Batch 가 {@code ResourcelessJobRepository} 로 떨어져
 * <b>잡 중복 실행 방지가 통째로 꺼졌다.</b> 그 셋은 {@link MainDataSourceConfig} 로 옮겼고
 * 조건 없이 항상 만들어진다. 여기 남은 것은 <b>관측 전용 빈뿐</b>이다.
 *
 * <p><b>반대 방향 실패</b> — 켜야 할 모듈이 스위치를 빠뜨리면 관측 빈이 통째로 없다. api 는
 * {@code management.yml} 의 {@code group.obs} 가 {@code obsDb} 기여자를 찾지 못해 기동에서
 * 걸리지만, 그 그룹을 쓰지 않는 모듈에는 그런 그물이 없다.
 */
@Configuration(proxyBeanMethods = false)
// 조건은 저장소가 통일한 havingValue 명시 형태를 따른다 — 관측 빈 여섯이 전부 그 형태이고
// ApiApplicationTests 가 그 일치를 단언한다. 속성 목록은 MainDbProperties·JdbcProperties 가
// MainDataSourceConfig 로 옮겨가 관측 것만 남는다.
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
@EnableConfigurationProperties({ ObservationDbProperties.class })
public class ObservationDataSourceConfig {



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
        // 풀 생성 헬퍼는 MainDataSourceConfig 가 갖는다. 두 곳에 같은 6줄을 두면
        // 한쪽만 고치는 실수를 아무것도 막지 못한다.
        HikariDataSource dataSource =
            MainDataSourceConfig.hikari(properties.driverClassName(), properties.url());
        dataSource.setUsername(properties.username());
        dataSource.setPassword(properties.password());
        return dataSource;
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


    @Bean
    @Qualifier("obs")
    public NamedParameterJdbcTemplate observationNamedParameterJdbcTemplate(
        @Qualifier("obs") JdbcTemplate observationJdbcTemplate
    ) {
        return new NamedParameterJdbcTemplate(observationJdbcTemplate);
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

}
