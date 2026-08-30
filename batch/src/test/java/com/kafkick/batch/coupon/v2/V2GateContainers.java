package com.kafkick.batch.coupon.v2;

import java.sql.Driver;

import org.flywaydb.core.Flyway;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * 게이트를 여는 두 경로의 테스트가 <b>같은</b> MySQL·Redis 를 쓴다.
 *
 * <p>클래스마다 컨테이너를 따로 띄우면 그만큼 기동이 반복된다. 이 저장소의 CI 는
 * {@code org.gradle.parallel=true} 로 {@code :batch:test} 와 {@code :infra:mq:test} 를 함께
 * 돌리는데, mq 쪽은 Kafka 브로커 셋 위에서 <b>20초 벽시계</b>({@code SETTLE})로 컨슈머 재시도를
 * 기다린다. 2코어 러너에서 그 창에 컨테이너 기동을 더 얹으면 남의 테스트가 타임아웃으로
 * 깨진다 — 코드가 틀려서가 아니라 그 순간 CPU 가 없어서다.
 *
 * <p>그래서 <b>JVM 하나에 한 벌</b>이다. 정적 초기화는 클래스 로딩 때 한 번만 돌고, 종료는
 * Testcontainers 의 Ryuk 이 맡는다 — {@code @AfterAll} 에서 멈추면 다음 클래스가 다시 띄운다.
 *
 * <p><b>상태 격리는 각 테스트가 진다.</b> 두 클래스 모두 {@code @BeforeEach} 에서 표를 비우고
 * {@code FLUSHALL} 한다. 같은 JVM 안에서 클래스는 순차 실행이므로(모듈에
 * {@code maxParallelForks} 를 주지 않았다) 그것으로 충분하다.
 */
final class V2GateContainers {

    @SuppressWarnings("rawtypes")
    private static final MySQLContainer MYSQL;
    private static final GenericContainer<?> REDIS;

    private static final JdbcTemplate JDBC;
    private static final TransactionTemplate TRANSACTIONS;
    private static final StringRedisTemplate REDIS_TEMPLATE;

    static {
        MYSQL = new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                .withDatabaseName("app")
                // ⚠️ 저장된 값이 UTC 벽시계라는 전제(CouponRoundGateJdbc.utc)를 컨테이너에도 건다.
                .withCommand("--default-time-zone=+00:00");
        MYSQL.start();
        Flyway.configure()
                .dataSource(MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();

        SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        try {
            dataSource.setDriverClass(
                    Class.forName(MYSQL.getDriverClassName()).asSubclass(Driver.class));
        } catch (ClassNotFoundException notFound) {
            throw new IllegalStateException("MySQL 드라이버를 못 찾았습니다.", notFound);
        }
        dataSource.setUrl(MYSQL.getJdbcUrl());
        dataSource.setUsername(MYSQL.getUsername());
        dataSource.setPassword(MYSQL.getPassword());
        JDBC = new JdbcTemplate(dataSource);
        TRANSACTIONS = new TransactionTemplate(new JdbcTransactionManager(dataSource));

        REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
                .withExposedPorts(6379);
        REDIS.start();
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getFirstMappedPort()));
        factory.afterPropertiesSet();
        factory.start();
        REDIS_TEMPLATE = new StringRedisTemplate(factory);
    }

    private V2GateContainers() {
    }

    static JdbcTemplate jdbc() {
        return JDBC;
    }

    static TransactionTemplate transactions() {
        return TRANSACTIONS;
    }

    static StringRedisTemplate redis() {
        return REDIS_TEMPLATE;
    }
}
