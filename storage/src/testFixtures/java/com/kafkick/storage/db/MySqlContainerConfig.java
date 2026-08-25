package com.kafkick.storage.db;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * @ServiceConnection 이 컨테이너에서 접속 정보를 읽어 storage.yml 의 url/계정/드라이버를 덮어쓴다.
 * 컨테이너 수명은 스프링 테스트 컨텍스트 캐싱에 맡긴다.
 */
@TestConfiguration(proxyBeanMethods = false)
public class MySqlContainerConfig {

    /**
     * <b>팀 협의로 {@code latest} 를 쓴다.</b> 앞서 {@code 8.0.35} 로 고정했던 것을 되돌린 것이고,
     * 되돌리면서 생기는 성질을 여기 적어 둔다 — 지우지 마라.
     *
     * <p><b>커밋이 그대로여도 결과가 달라질 수 있다.</b> 도커 허브가 가리키는 대상이 바뀌면
     * 같은 코드가 다른 서버에서 돈다. 실제로 이 태그가 8.0 에서 <b>26.7 로 넘어간 것을
     * 확인했다.</b> {@code sql-mode} 기본값·{@code CHECK} 강제·{@code DROP CHECK} 문법·
     * 기본 collation 이 전부 버전에 묶여 있고, 아래 설정이 <i>"운영 기본값과 같다"</i> 는 주장도
     * 그렇다. 파리티 테스트가 컬럼 collation 까지 대조하므로 이 축이 흔들리면 거기서 먼저 운다.
     *
     * <p><b>{@code V2026082509__issuance_status_check.sql} 의 <i>"시드·테스트·CI 는 8.0.35 로
     * 고정돼 있다"</i> 는 문장은 이 결정으로 낡았다.</b> 그 파일은 이미 적용된 마이그레이션이라
     * 주석만 고쳐도 Flyway 체크섬이 깨지므로 손대지 않는다 — 정정을 여기 둔다.
     *
     * <p><b>그래서 빨간불이 코드 탓이 아닐 수 있다.</b> 손댄 것이 없는데 갑자기 깨지면
     * {@code docker run --rm mysql:latest mysqld --version} 을 먼저 찍어 보고,
     * 시드({@code cy-seed})·compose 가 쓰는 버전과 갈렸는지 확인해라.
     * 검증 대상 데이터를 만드는 쪽과 검증하는 쪽이 다른 서버면 판정의 뜻이 약해진다.
     */
    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:latest");

    @Bean
    @ServiceConnection
    MySQLContainer mySqlContainer() {
        return new MySQLContainer(IMAGE)
                .withDatabaseName("app")
                .withUsername("test")
                // 락 범위를 재는 테스트가 performance_schema 를 읽는다. 파일 안에 이유를 적었다.
                // initdb.d 는 root 로 도는 자리라 여기서만 권한을 줄 수 있다.
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("db/testcontainers/grant-process.sql"),
                        "/docker-entrypoint-initdb.d/10-grant-process.sql")
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
