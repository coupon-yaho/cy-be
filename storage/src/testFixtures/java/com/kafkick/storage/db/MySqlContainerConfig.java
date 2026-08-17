package com.kafkick.storage.db;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * @ServiceConnection 이 컨테이너에서 접속 정보를 읽어 storage.yml 의 url/계정/드라이버를 덮어쓴다.
 * 컨테이너 수명은 스프링 테스트 컨텍스트 캐싱에 맡긴다.
 *
 * <p>관측 풀은 @ServiceConnection 을 따르지 않는다 — URL 도 계정도. 자동 주입에 맡기면
 * "관측은 SELECT 전용 계정" 이 조용히 무효가 되기 때문이다. 그래서 여기서 명시적으로 꽂아 준다.
 * 관측 접속 정보는 @NotBlank 라, 이 등록을 빠뜨리면 컨테이너를 쓰는 테스트가 기동에서 죽는다.
 */
@TestConfiguration(proxyBeanMethods = false)
public class MySqlContainerConfig {

    /** latest 는 도커 허브가 가리키는 대상이 바뀌면 커밋이 그대로여도 테스트 결과가 달라진다. */
    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:latest");

    @Bean
    @ServiceConnection
    MySQLContainer mySqlContainer() {
        return new MySQLContainer(IMAGE)
                .withDatabaseName("app")
                .withUrlParam("serverTimezone", "UTC")
                .withUrlParam("characterEncoding", "UTF-8")
                .withUrlParam("useUnicode", "true")
                .withUrlParam("rewriteBatchedStatements", "true")
                // 운영 MySQL 서버 설정(my.cnf) 중 쿼리 결과에 영향을 주는 항목만 옮겼다.
                // 메모리·로깅·binlog 는 테스트에 불필요하므로 제외. 서버 설정이 바뀌면 여기도 같이 본다.
                .withCommand(
                        "--default-time-zone=+00:00",
                        "--character-set-server=utf8mb4",
                        "--collation-server=utf8mb4_0900_ai_ci",
                        "--default-storage-engine=InnoDB",
                        "--sql-mode=STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
                                + "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
                        "--local-infile=0");
    }

    /** 관측 풀 설정은 커밋되지 않는 storage.yml 에 있다. 컨테이너를 쓰는 테스트는 여기서 받는다. */
    @Bean
    DynamicPropertyRegistrar observationDataSourceProperties(MySQLContainer mySqlContainer) {
        return registry -> {
            registry.add("observation.datasource.url", mySqlContainer::getJdbcUrl);
            registry.add("observation.datasource.username", mySqlContainer::getUsername);
            registry.add("observation.datasource.password", mySqlContainer::getPassword);
        };
    }
}
