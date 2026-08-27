package com.kafkick.batch.analytics;

import javax.sql.DataSource;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.transaction.autoconfigure.TransactionManagerCustomizers;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.support.TimeProvider;

/**
 * 브랜드 분석 집계 배선.
 *
 * <p><b>관측 풀 스위치를 문다.</b> 재계수는 관측 풀로 나가므로 그 풀이 없으면 배선할 대상이 없다.
 * {@code DomainObservationConfig} 와 같은 이유로 조건을 붙인다 — 없는 풀을 찾다가 기동에서 죽는 대신
 * 배선을 통째로 접는다.
 *
 * <p>⚠️ 반대 방향 실패 — 관측을 끄면 집계가 <b>조용히</b> 사라진다. 기동도 로그도 정상이고 화면은
 * 3시간 뒤 STALE 이 된다. 이것을 배포 시점에 잡으려면
 * {@code AnalyticsWiringTest} 의 WithoutObservation 이 고정하는 조합을 봐야 한다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
@EnableConfigurationProperties(AnalyticsAggregationProperties.class)
public class AnalyticsConfiguration {

    @Bean
    public AnalyticsAggregateReader analyticsAggregateReader(
            @Qualifier("obs") DataSource observationDataSource,
            AnalyticsAggregationProperties properties
    ) {
        return new AnalyticsAggregateReader(observationDataSource, properties);
    }

    /**
     * 트랜잭션 매니저를 빈으로 올리지 않고 여기서 만든다.
     *
     * <p>@Primary 매니저는 엔티티가 있으면 {@code JpaTransactionManager} 인데, 그것은
     * {@code MainDataSourceConfig} 에서 {@code setDataSource} 없이 만들어져 <b>JDBC 커넥션을
     * 트랜잭션에 묶어 주지 않는다.</b> 그 매니저로 묶으면 축 행 쓰기와 축 상태 갱신이 서로 다른
     * 커넥션으로 나가 원자성이 조용히 사라진다. 운영 풀 위의 JDBC 매니저를 직접 만들어 그 짝을 맞춘다.
     *
     * <p>빈으로 올리지 않는 이유 — {@code PlatformTransactionManager} 후보를 하나 더 만들면
     * 이 배치와 무관한 주입 지점의 해석이 달라질 수 있다. 쓰는 곳이 여기뿐이라 여기 가둔다.
     */
    @Bean
    public AnalyticsRunStore analyticsRunStore(
            JdbcTemplate jdbcTemplate,
            DataSource dataSource,
            ObjectProvider<TransactionManagerCustomizers> customizers
    ) {
        JdbcTransactionManager transactionManager = new JdbcTransactionManager(dataSource);
        // 매니저를 직접 만들면 자동설정이 하던 커스터마이저 적용이 함께 물러난다. 안 하면
        // spring.transaction.default-timeout 이 이 트랜잭션에서만 조용히 무시된다.
        customizers.ifAvailable(it -> it.customize(transactionManager));
        return new AnalyticsRunStore(jdbcTemplate, new TransactionTemplate(transactionManager));
    }

    @Bean
    public AnalyticsAggregationRunner analyticsAggregationRunner(
            AnalyticsAggregateReader reader,
            AnalyticsRunStore store,
            AnalyticsAggregationProperties properties,
            TimeProvider timeProvider
    ) {
        return new AnalyticsAggregationRunner(reader, store, properties, timeProvider);
    }

    /**
     * 스케줄러를 {@code @Component} 가 아니라 여기서 조립하는 이유.
     *
     * <p>coupon 계열 스케줄러는 {@code @Component} + 클래스 조건이다. 이쪽은 조건이 <b>두 개</b>다 —
     * 관측 풀(이 설정 전체의 조건)과 쓰기 동결 스위치. {@code @Component} 로 두면 관측 조건은
     * 클래스에, 동결 조건은 또 클래스에 붙어 <b>두 조건이 서로 다른 파일에 흩어진다.</b>
     * 배선 전체가 관측 풀에 매여 있다는 사실이 이 파일 하나로 읽혀야 해서 여기 모은다
     * (observation 계열 {@code DomainObservationConfig} 도 같은 형태다).
     */
    @Bean
    @ConditionalOnProperty(prefix = "batch.scheduling", name = "enabled", havingValue = "true")
    public AnalyticsAggregationScheduler analyticsAggregationScheduler(
            AnalyticsAggregationRunner runner
    ) {
        return new AnalyticsAggregationScheduler(runner);
    }
}
