package com.kafkick.storage.db.config;

import static org.assertj.core.api.Assertions.assertThat;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails;
import org.springframework.boot.transaction.autoconfigure.TransactionManagerCustomizationAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import com.zaxxer.hikari.HikariDataSource;

/**
 * 컨텍스트 전체를 띄우지 않는다. 엔티티가 0개라 JpaAuditConfig 가 "JPA metamodel must not be empty"
 * 로 죽고, 실제 접속도 필요 없기 때문이다. Hikari 는 첫 getConnection 까지 풀을 만들지 않는다.
 */
class ObservationDataSourceConfigTest {

    private static final String[] STORAGE_YML = {
        "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
        "spring.datasource.url=jdbc:mysql://localhost:3306/app",
        "spring.datasource.username=app",
        "spring.datasource.password=app",
        "spring.datasource.hikari.maximum-pool-size=10",
        "spring.datasource.hikari.connection-timeout=3000",
        "observation.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
        "observation.datasource.url=jdbc:mysql://localhost:3306/app",
        "observation.datasource.username=obs",
        "observation.datasource.password=obs",
        "observation.datasource.hikari.maximum-pool-size=2",
        "observation.datasource.hikari.connection-timeout=2000",
        "observation.datasource.hikari.read-only=true",
        "observation.datasource.hikari.pool-name=obs-pool",
        "observation.datasource.hikari.connection-init-sql=SET SESSION max_execution_time = 3000",
    };

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(ObservationDataSourceConfig.class)
        .withPropertyValues(STORAGE_YML);

    @Test
    void 관측_풀은_작게_고정되고_읽기_전용이다() {
        runner.run(context -> {
            HikariDataSource obs = context.getBean("observationDataSource", HikariDataSource.class);

            assertThat(obs.getMaximumPoolSize()).isEqualTo(2);
            assertThat(obs.getConnectionTimeout()).isEqualTo(2000);
            assertThat(obs.isReadOnly()).isTrue();
            assertThat(obs.getPoolName()).isEqualTo("obs-pool");
            assertThat(obs.getUsername()).isEqualTo("obs");
        });
    }

    @Test
    void 운영_풀은_기존_hikari_설정을_그대로_받는다() {
        runner.run(context -> {
            HikariDataSource main = context.getBean("mainDataSource", HikariDataSource.class);

            assertThat(main.getMaximumPoolSize()).isEqualTo(10);
            assertThat(main.getConnectionTimeout()).isEqualTo(3000);
            assertThat(main.isReadOnly()).isFalse();
            assertThat(main.getUsername()).isEqualTo("app");
        });
    }

    /** @Primary 가 빠지면 JPA·Flyway 자동설정이 후보 둘을 만나 기동에 실패한다. */
    @Test
    void 타입만으로_주입하면_운영_풀이_선택된다() {
        runner.run(context -> {
            assertThat(context.getBeanNamesForType(DataSource.class)).hasSize(2);
            assertThat(context.getBean(DataSource.class)).isSameAs(context.getBean("mainDataSource"));
            assertThat(context.getBean(JdbcTemplate.class))
                .isSameAs(context.getBean("jdbcTemplate"));
        });
    }

    @Test
    void obs_한정자로_주입하면_관측_풀이_선택된다() {
        runner.withUserConfiguration(ObsConsumer.class).run(context -> {
            ObsConsumer consumer = context.getBean(ObsConsumer.class);

            assertThat(consumer.dataSource).isSameAs(context.getBean("observationDataSource"));
            assertThat(consumer.jdbcTemplate).isSameAs(context.getBean("observationJdbcTemplate"));
            assertThat(consumer.jdbcTemplate.getDataSource()).isSameAs(consumer.dataSource);
        });
    }

    /**
     * 컨테이너·Compose 의 접속 정보가 관측 풀까지 덮어쓰면 관측이 슈퍼유저로 붙는다.
     * "관측은 SELECT 전용 계정" 이 에러 없이 무효가 되므로 여기서 못을 박는다.
     *
     * <p>URL 도 마찬가지다 — 관측은 자동 주입을 <b>전혀</b> 따르지 않는다. 폴백 분기를 두면
     * 관측 설정을 빠뜨린 채로도 뜨는 경로가 생겨서, 기동에서 죽인다는 결정이 무의미해진다.
     */
    @Test
    void 접속_정보_자동_주입이_관측_풀을_덮어쓰지_않는다() {
        runner.withBean(JdbcConnectionDetails.class, ObservationDataSourceConfigTest::superuserDetails)
            .run(context -> {
                HikariDataSource obs = context.getBean("observationDataSource", HikariDataSource.class);
                HikariDataSource main = context.getBean("mainDataSource", HikariDataSource.class);

                assertThat(obs.getUsername()).isEqualTo("obs");
                assertThat(obs.getJdbcUrl()).isEqualTo("jdbc:mysql://localhost:3306/app");
                // 운영 풀은 반대로 자동 주입을 따라야 @ServiceConnection 을 쓰는 테스트가 산다.
                assertThat(main.getUsername()).isEqualTo("root");
                assertThat(main.getJdbcUrl()).isEqualTo("jdbc:mysql://localhost:33333/app");
            });
    }

    /** 관측용이 없으면 이름 바인딩 쿼리는 한정자 붙일 대상이 없어 조용히 운영 풀로 나간다. */
    @Test
    void 이름_바인딩_템플릿도_운영과_관측_둘_다_있다() {
        runner.run(context -> {
            NamedParameterJdbcTemplate obs = context.getBean(
                "observationNamedParameterJdbcTemplate", NamedParameterJdbcTemplate.class);

            assertThat(obs.getJdbcTemplate()).isSameAs(context.getBean("observationJdbcTemplate"));
            assertThat(context.getBean(NamedParameterJdbcTemplate.class))
                .isSameAs(context.getBean("namedParameterJdbcTemplate"));
        });
    }

    /** 타임아웃이 없으면 무거운 관측 쿼리가 끝날 때까지 커넥션을 물고 있는다. */
    @Test
    void 관측_쿼리에는_타임아웃이_걸린다() {
        runner.run(context -> {
            JdbcTemplate obs = context.getBean("observationJdbcTemplate", JdbcTemplate.class);

            assertThat(obs.getQueryTimeout()).isEqualTo(3);
        });
    }

    /**
     * 운영 풀의 타임아웃은 이 티켓이 정하지 않는다 — 값은 부하 측정 뒤에 정할 일이라 별도 티켓이다.
     * 다만 <b>설정으로 조일 수 있는 경로</b>는 살아 있어야 한다. 자동설정을 대체하면서
     * spring.jdbc.template.* 가 조용히 무시되면, 나중에 값을 적는 사람이 그 사실을 모른다.
     */
    @Test
    void 운영_템플릿은_spring_jdbc_설정을_따른다() {
        runner.withPropertyValues("spring.jdbc.template.query-timeout=5s", "spring.jdbc.template.max-rows=100")
            .run(context -> {
                JdbcTemplate main = context.getBean("jdbcTemplate", JdbcTemplate.class);

                assertThat(main.getQueryTimeout()).isEqualTo(5);
                assertThat(main.getMaxRows()).isEqualTo(100);
            });
    }

    /**
     * 클라이언트 타임아웃만 두면 커넥션만 회수되고 MySQL 은 쿼리를 끝까지 돌린다.
     * 서버 세션 상한이 방어의 나머지 절반이라, 값이 빈까지 실렸는지 확인한다.
     */
    @Test
    void 서버_쪽_실행시간_상한이_커넥션마다_걸린다() {
        runner.run(context -> {
            HikariDataSource obs = context.getBean("observationDataSource", HikariDataSource.class);

            assertThat(obs.getConnectionInitSql()).isEqualTo("SET SESSION max_execution_time = 3000");
            assertThat(context.getBean("mainDataSource", HikariDataSource.class).getConnectionInitSql()).isNull();
        });
    }

    /** JDBC 는 초 단위라 내림하면 설정보다 빨리 끊긴다. 설정한 사람이 모르는 사이에 더 조이면 안 된다. */
    @Test
    void 초로_안_떨어지는_타임아웃은_올림된다() {
        runner.withPropertyValues("observation.datasource.query-timeout=2500ms").run(context -> {
            JdbcTemplate obs = context.getBean("observationJdbcTemplate", JdbcTemplate.class);

            assertThat(obs.getQueryTimeout()).isEqualTo(3);
        });
    }

    /** 1초 미만은 JDBC 에서 0(제한 없음)으로 잘린다. 조이려던 설정이 푸는 설정으로 뒤집힌다. */
    @Test
    void 일초_미만_타임아웃은_기동을_막는다() {
        runner.withPropertyValues("observation.datasource.query-timeout=500ms")
            .run(context -> assertThat(context).hasFailed());
    }

    /** 이름을 지목하지 않은 @Transactional 이 어느 풀에 열리는지가 여기서 갈린다. */
    @Test
    void 관측_트랜잭션_매니저는_관측_풀_위에_열린다() {
        runner.run(context -> {
            JdbcTransactionManager obs =
                context.getBean("observationTransactionManager", JdbcTransactionManager.class);

            assertThat(obs.getDataSource()).isSameAs(context.getBean("observationDataSource"));
            assertThat(context.getBean(PlatformTransactionManager.class))
                .isSameAs(context.getBean("transactionManager"));
        });
    }

    /**
     * 트랜잭션 전체를 시간으로 묶는 유일한 손잡이다. 자동설정을 대체하면서 커스터마이저 적용을
     * 빠뜨리면 spring.transaction.default-timeout 이 조용히 무시된다.
     */
    @Test
    void 트랜잭션_기본_타임아웃_설정이_두_매니저에_모두_적용된다() {
        runner.withConfiguration(AutoConfigurations.of(TransactionManagerCustomizationAutoConfiguration.class))
            .withPropertyValues("spring.transaction.default-timeout=7s")
            .run(context -> {
                assertThat(context.getBean("transactionManager", JdbcTransactionManager.class).getDefaultTimeout())
                    .isEqualTo(7);
                assertThat(context.getBean("observationTransactionManager", JdbcTransactionManager.class)
                    .getDefaultTimeout()).isEqualTo(7);
            });
    }

    /** EntityManagerFactory 가 없는 지금은 JDBC 매니저로 떨어져야 한다. 엔티티가 생기면 JPA 매니저다. */
    @Test
    void 운영_트랜잭션_매니저는_운영_풀_위에_열린다() {
        runner.run(context -> {
            JdbcTransactionManager main =
                context.getBean("transactionManager", JdbcTransactionManager.class);

            assertThat(main.getDataSource()).isSameAs(context.getBean("mainDataSource"));
        });
    }

    /**
     * URL 하나만 빠져도 막아야 한다. 블록 전체가 없는 경우는 계정 검증에도 걸려서, 그 테스트만으로는
     * <b>URL 의 @NotBlank 가 지워져도 통과한다</b> — 실제로 지워 보고 확인했다.
     *
     * <p>관측 URL 은 자동 주입 폴백이 없으므로, 비면 jdbcUrl 이 null 인 풀이 만들어지고 대시보드
     * 첫 조회에서야 죽는다.
     */
    @Test
    void 관측_URL_만_빠져도_기동에_실패한다() {
        new ApplicationContextRunner()
            .withUserConfiguration(ObservationDataSourceConfig.class)
            .withBean(JdbcConnectionDetails.class, ObservationDataSourceConfigTest::superuserDetails)
            .withPropertyValues(
                "spring.datasource.url=jdbc:mysql://localhost:3306/app",
                "spring.datasource.username=app",
                "observation.datasource.username=obs",
                "observation.datasource.password=obs")
            .run(context -> assertThat(context).hasFailed());
    }

    /** storage.yml 은 커밋하지 않는다. 블록이 없는 채로 뜨면 대시보드 첫 요청에서야 죽는다. */
    @Test
    void 관측_블록이_없으면_기동에_실패한다() {
        new ApplicationContextRunner()
            .withUserConfiguration(ObservationDataSourceConfig.class)
            .withPropertyValues(
                "spring.datasource.url=jdbc:mysql://localhost:3306/app",
                "spring.datasource.username=app",
                "spring.datasource.password=app")
            .run(context -> assertThat(context).hasFailed());
    }

    /**
     * 운영 블록이 비면 Hikari 는 첫 조회까지 풀을 안 열어서 기동은 성공하고 첫 요청에서 죽는다.
     * 관측 블록(@NotBlank)과 같은 시점에 죽여야 한다.
     */
    @Test
    void 운영_접속_정보가_비면_기동에_실패한다() {
        new ApplicationContextRunner()
            .withUserConfiguration(ObservationDataSourceConfig.class)
            .withPropertyValues(
                "observation.datasource.url=jdbc:mysql://localhost:3306/app",
                "observation.datasource.username=obs",
                "observation.datasource.password=obs")
            .run(context -> assertThat(context).hasFailed());
    }

    /** 접속 정보 자동 주입이 채워 주면 storage.yml 에 값이 없어도 정상이다 — 컨테이너 테스트가 그 경로다. */
    @Test
    void 운영_접속_정보는_자동_주입으로_채워도_된다() {
        new ApplicationContextRunner()
            .withUserConfiguration(ObservationDataSourceConfig.class)
            .withBean(JdbcConnectionDetails.class, ObservationDataSourceConfigTest::superuserDetails)
            .withPropertyValues(
                "observation.datasource.url=jdbc:mysql://localhost:3306/app",
                "observation.datasource.username=obs",
                "observation.datasource.password=obs")
            .run(context -> assertThat(context).hasNotFailed());
    }

    private static JdbcConnectionDetails superuserDetails() {
        return new JdbcConnectionDetails() {

            @Override
            public String getUsername() {
                return "root";
            }

            @Override
            public String getPassword() {
                return "root";
            }

            @Override
            public String getJdbcUrl() {
                return "jdbc:mysql://localhost:33333/app";
            }
        };
    }

    /** A 가 실제로 쓰게 될 주입 형태 그대로다. */
    static class ObsConsumer {

        private final DataSource dataSource;

        private final JdbcTemplate jdbcTemplate;

        ObsConsumer(@Qualifier("obs") DataSource dataSource, @Qualifier("obs") JdbcTemplate jdbcTemplate) {
            this.dataSource = dataSource;
            this.jdbcTemplate = jdbcTemplate;
        }
    }
}
