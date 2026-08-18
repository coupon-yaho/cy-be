package com.kafkick.storage.db;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * @ServiceConnection 이 컨테이너에서 접속 정보를 읽어 storage.yml 의 url/계정/드라이버를 덮어쓴다.
 * 컨테이너 수명은 스프링 테스트 컨텍스트 캐싱에 맡긴다.
 */
@TestConfiguration(proxyBeanMethods = false)
public class MySqlContainerConfig {

    /** CHECK 제약을 실제로 적용하는 MySQL 8.0.16 이상의 고정 버전으로 검증합니다. */
    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.4.6");

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
}
