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

    /**
     * <b>시드 저장소가 게이트 데이터셋을 만든 버전에 맞춘다.</b> {@code cy-seed} 의 README 가
     * "MySQL 8.0.35 컨테이너" 로 실측을 기록하고 {@code docs/07-operations.md} 도 {@code mysql:8.0} 을 쓴다.
     * 검증 대상 데이터가 그 버전에서 만들어지는데 검증기만 다른 버전에서 돌 이유가 없다.
     *
     * <p><b>{@code latest} 를 쓰면 안 된다.</b> 커밋이 그대로여도 도커 허브가 가리키는 대상이
     * 바뀌면 테스트 결과가 달라진다 — 실제로 이 저장소에서 {@code latest} 가 8.0 에서
     * <b>26.7 로 넘어간 것을 확인했다.</b> {@code sql-mode} 기본값·{@code CHECK} 강제·
     * {@code DROP CHECK} 문법이 전부 버전에 묶여 있어, 아래 설정이 "기본값과 같다" 는 주장도
     * 버전이 고정돼야만 성립한다.
     */
    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.0.35");

    @Bean
    @ServiceConnection
    MySQLContainer mySqlContainer() {
        return new MySQLContainer(IMAGE)
                .withDatabaseName("app")
                .withUrlParam("serverTimezone", "UTC")
                .withUrlParam("characterEncoding", "UTF-8")
                .withUrlParam("useUnicode", "true")
                .withUrlParam("rewriteBatchedStatements", "true")
                // UPDATE 반환값을 matched rows 로 고정한다. 기본값이지만 명시한다 —
                // VerificationRunJdbcAdapter 가 "0행 = 실행 행이 없다" 로 해석하는데,
                // 누가 UPSERT 반환값(삽입 1/갱신 2)을 쓰려고 useAffectedRows=true 를 붙이면
                // 값이 같은 UPDATE 가 0행이 되어 멀쩡한 행에 RUN_ROW_VANISHED 가 난다.
                .withUrlParam("useAffectedRows", "false")
                // 운영 MySQL 서버 설정(my.cnf) 중 쿼리 결과에 영향을 주는 항목만 옮겼다.
                // 메모리·로깅·binlog 는 테스트에 불필요하므로 제외. 서버 설정이 바뀌면 여기도 같이 본다.
                .withCommand(
                        "--default-time-zone=+00:00",
                        "--character-set-server=utf8mb4",
                        "--collation-server=utf8mb4_0900_ai_ci",
                        "--default-storage-engine=InnoDB",
                        // ONLY_FULL_GROUP_BY 는 MySQL 8 기본값이다. 빼 두면 그룹 키를 잘못 좁힌
                        // GROUP BY 를 서버가 거부하지 않고 임의 값을 골라 줘서,
                        // 테스트가 운영보다 느슨한 모드에서 돈다.
                        "--sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
                                + "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
                        "--local-infile=0",
                        // 위 주석대로 binlog 는 테스트에 불필요한데 이미지 기본값이 ON 이었다.
                        // 켜져 있으면 SUPER 없는 계정이 트리거를 못 만들어(오류 1419),
                        // "실행 중에 데이터가 바뀐다" 를 재현하는 테스트를 쓸 수 없다.
                        "--skip-log-bin");
    }
}
