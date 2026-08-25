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
     *
     * <h2>[CY-488] 운영 풀 query-timeout 은 값을 두지 않는다 — 그 판단의 근거</h2>
     *
     * <p><b>이 손잡이가 닿는 범위는 운영 <i>풀</i> 이 아니라 이 {@code JdbcTemplate} 하나다.</b>
     * Flyway 와 Spring Batch 의 {@code JobRepository} 는 {@code DataSource} 를 직접 물어 이 값과
     * 무관하고, 발급의 재고 차감은 JPA({@code CouponStockJpaRepository} 의
     * {@code @Lock(PESSIMISTIC_WRITE)})라 역시 이 템플릿을 안 탄다.
     *
     * <p><b>다만 {@code IdempotencyRepositoryImpl} 이 이것을 문다 — 발급 경로다.</b> 요청마다
     * 재고를 건드리기 전에 도는 멱등키 선점({@code INSERT idempotency_records})이 여기 실린다.
     * 그래서 이 값은 <b>정상 발급을 죽일 수 있는 자리에 있다.</b> 나머지 주입 지점은
     * {@code JdbcBenchmarkRunRepository} 의 쓰기 전이와 {@code JdbcRunTimeseriesArchiveStore} 다.
     *
     * <p>실측(2026-08-25, MySQL 8.4.11 · Connector/J 9.7.0, 유휴 DB). 이 템플릿이 실제로 실행하는
     * 가장 무거운 문장은 아카이브의 500행 청크 INSERT 이고, 10000행 3회 반복에서:
     *
     * <pre>
     *   FOR UPDATE 0~11ms · DELETE 1~42ms · 가장 느린 500행 청크 12~49ms
     * </pre>
     *
     * <p>여기서 값을 정하려면 <b>부하 중</b> 수치가 필요한데 그것이 없다. 유휴 49ms 에 임의의
     * 배수를 곱해 적는 것은 근거가 아니라 추측이고, 그 추측한 값은 위 멱등키 선점을 함께 끊는다 —
     * 증상은 타임아웃이 아니라 <b>발급 실패</b>로 나타난다. 아래 실측대로 이 손잡이는 v1 비관적
     * 락의 <b>정상 대기까지 끊으므로</b>, 락 경합이 몰리는 구간일수록 더 많이 끊는다.
     * 무한 대기는 URL 의 {@code socketTimeout=60000} 이 이미 막고 있으므로, 지금 비워 두는 대가는
     * "60초 상한이 그대로 남는다" 뿐이다. 그래서 <b>비워 두는 쪽을 골랐다.</b>
     *
     * <p><b>서버 쪽 두 번째 겹({@code SET SESSION max_execution_time})도 걸지 않는다.</b>
     * 관측 풀과 달리 운영 풀에서는 방어가 아니라 손해다. 같은 날 같은 서버 실측:
     *
     * <pre>
     *                                   max_execution_time=1000   setQueryTimeout(1)
     *   SELECT SLEEP(5)                 1.1s 에 잘림 (ER 3024)     1.03s 에 잘림
     *   UPDATE / INSERT .. SLEEP(5)     안 잘림 (5.0s 완주)         1.18s 에 잘림
     *   락 걸린 행 SELECT .. FOR UPDATE   1.0s 에 잘림 (ER 3024)     1.02s 에 잘림
     *   락 걸린 행 UPDATE                안 잘림                    1.03s 에 잘림
     * </pre>
     *
     * <p>즉 서버 상한은 운영 풀이 하는 일(쓰기)은 못 막고 <b>락 대기만 끊는다.</b>
     * {@code storage.yml} 이 socketTimeout 에 대해 경고한 함정과 같은 자리다.
     *
     * <p>값을 정하려면 부하 중 이 두 문장의 소요를 재야 한다. TODO(후속 티켓): v1 부하 측정에서
     * 아카이브·전이 문장의 p99 를 확보한 뒤 이 키를 다시 본다.
     *
     * <h2>1초 미만은 올림한다 — 자동설정과 다르다</h2>
     *
     * <p>Boot 4.1 의 {@code JdbcTemplateConfiguration} 은 {@code (int) getSeconds()} 로 <b>내린다.</b>
     * 그러면 {@code 500ms} 가 {@code 0} 이 되고, JDBC 에서 {@code 0} 은 <b>제한 없음</b>이다
     * (실측: {@code setQueryTimeout(0)} + {@code SELECT SLEEP(3)} → 3.0s 완주).
     * 조이려던 설정이 푸는 설정으로 뒤집힌다. 관측 템플릿이 같은 이유로 올림하고 있어 맞춘다.
     *
     * <p><b>반대 방향 실패</b> — {@code 1200ms} 를 적으면 2초가 된다. 순정 Boot 보다 <b>느슨하다.</b>
     * Boot 문서를 보고 값을 적은 사람은 이 차이를 모른다. 그래도 "모르는 사이에 더 조인다" 와
     * "모르는 사이에 통째로 풀린다" 중 후자가 더 나쁘다고 보고 올림을 택했다.
     */
    @Bean
    @Primary
    public JdbcTemplate jdbcTemplate(DataSource dataSource, JdbcProperties jdbcProperties) {
        JdbcProperties.Template template = jdbcProperties.getTemplate();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setFetchSize(template.getFetchSize());
        jdbcTemplate.setMaxRows(template.getMaxRows());
        if (template.getQueryTimeout() != null) {
            jdbcTemplate.setQueryTimeout(
                (int) Math.ceil(template.getQueryTimeout().toMillis() / 1000.0));
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
