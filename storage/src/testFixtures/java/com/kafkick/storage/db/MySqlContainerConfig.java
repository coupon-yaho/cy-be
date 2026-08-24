package com.kafkick.storage.db;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * @ServiceConnection 이 컨테이너에서 접속 정보를 읽어 storage.yml 의 url/계정/드라이버를 덮어쓴다.
 * 컨테이너 수명은 스프링 테스트 컨텍스트 캐싱에 맡긴다.
 *
 * <p>관측 풀은 @ServiceConnection 을 따르지 않는다 — URL 도 계정도. 자동 주입에 맡기면
 * "관측은 SELECT 전용 계정" 이 조용히 무효가 되기 때문이다. 그래서 여기서 명시적으로 꽂아 준다.
 * 관측 접속 정보는 @NotBlank 라, 이 등록을 빠뜨리면 컨테이너를 쓰는 테스트가 기동에서 죽는다.
 */
@TestConfiguration
public class MySqlContainerConfig {

    /** {@code infra/mysql/initdb/20-obs-account.sh} 에 env 로 건네는 값이다. */
    private static final String OBSERVATION_USERNAME = "obs";

    /**
     * <b>일부러 까다로운 값이다.</b> 초기화 스크립트가 이 비밀번호를 root 로 도는 SQL 문에
     * 리터럴로 박으므로, 이스케이프가 한 문자라도 빠지면 <b>컨테이너가 아예 안 뜬다</b> —
     * 그러면 컨테이너를 쓰는 모든 테스트가 그 자리에서 빨간불이 된다.
     *
     * <p>두 문자가 각각 다른 실패를 만든다.
     * <ul>
     *   <li>{@code '} — 문자열을 조기에 닫는다. 배가({@code ''})로 막는다</li>
     *   <li>{@code \} — MySQL 은 {@code NO_BACKSLASH_ESCAPES} 가 꺼진 기본값에서 이 문자를
     *       이스케이프 문자로 읽는다. 값이 이것으로 <b>끝나면</b> 닫는 따옴표가 escape 되어
     *       문자열이 다음 줄까지 삼킨다. 실측하면 {@code ERROR 1064} 로 죽는다</li>
     * </ul>
     *
     * <p>그래서 <b>백슬래시가 마지막 문자다.</b> 가운데 두면 그 실패가 재현되지 않아
     * 이스케이프를 지워도 테스트가 통과한다 — 실제로 그 상태였고, 변이로 확인했다.
     *
     * <p>평범한 값(예: {@code "obs"})으로 두면 이스케이프 경로를 아예 타지 않아
     * <b>이 저장소에 그 회귀를 잡는 그물이 하나도 없게 된다.</b>
     */
    private static final String OBSERVATION_PASSWORD = "o'bs\\";

    /** CHECK 제약을 실제로 적용하며 커밋마다 동일한 결과를 내도록 버전을 고정한다. */
    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.4.6");

    @Bean
    @ServiceConnection
    MySQLContainer mySqlContainer() {
        return new MySQLContainer(IMAGE)
                .withDatabaseName("app")
                // grant-process.sql 이 이 이름으로 GRANT 한다. Testcontainers 기본값과 같지만
                // 명시한다 — 기본값이 바뀌면 GRANT 대상이 없는 계정을 가리켜 컨테이너가 안 뜨고,
                // 그때 원인이 SQL 파일 안에 있다는 것을 알기 어렵다.
                .withUsername("test")
                // initdb.d 는 root 로 도는 유일한 자리라 권한은 여기서만 줄 수 있다.
                // 두 파일은 서로 독립이다 — 번호가 실행 순서이고, 하나를 지우면 그 파일에
                // 기대는 테스트만 죽는다.
                //
                // 10 · 락 범위 측정이 performance_schema.data_locks 를 읽는다(feature/CY-15).
                //      이 브랜치에는 그 테스트가 없지만, 합류 때 이 호출이 사라지지 않도록
                //      미리 맞춰 둔다.
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("db/testcontainers/grant-process.sql"),
                        "/docker-entrypoint-initdb.d/10-grant-process.sql")
                // 20 · 관측 전용 계정. **compose 가 마운트하는 것과 같은 파일**이라
                //      테스트에서 도는 권한이 곧 로컬에서 도는 권한이다.
                // ⚠️ 클래스패스가 아니라 **저장소 경로**에서 읽는다. 이 파일은 compose 가
                //    호스트 경로로 마운트하는 것이라 배포 jar 에 실릴 이유가 없다 —
                //    main/resources 에 뒀더니 storage jar 안에 들어갔다(실측: 3,767 bytes).
                //
                //    모드 0755 도 명시한다. entrypoint 는 .sh 를 **실행**하므로 실행 비트가
                //    없으면 "bad interpreter: Permission denied" 로 컨테이너가 아예 안 뜬다(실측).
                .withCopyFileToContainer(
                        MountableFile.forHostPath(
                                repoRoot().resolve("infra/mysql/initdb/20-obs-account.sh"), 0755),
                        "/docker-entrypoint-initdb.d/20-obs-account.sh")
                // 그 스크립트가 읽는 값. compose 는 .env 로 준다.
                .withEnv("DB_OBS_USERNAME", OBSERVATION_USERNAME)
                .withEnv("DB_OBS_PASSWORD", OBSERVATION_PASSWORD)
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

    /**
     * 관측 풀 설정은 커밋되지 않는 storage.yml 에 있다. 컨테이너를 쓰는 테스트는 여기서 받는다.
     *
     * <p><b>[CY-338] 계정을 컨테이너 슈퍼유저에서 진짜 {@code obs} 로 바꿨다.</b> 예전에는
     * {@code mySqlContainer.getUsername()} 을 그대로 꽂아서, "관측은 SELECT 전용" 이라는 계약을
     * <b>어떤 테스트도 검증하지 못했다</b> — 권한이 모자라도 프로덕션에서만 드러났다.
     * 계정과 권한은 {@code infra/mysql/initdb/20-obs-account.sh} 가 만든다 — compose 가 마운트하는 것과 같은 파일이다.
     *
     * <p>그 대가 — 관측 풀로 쓰기를 시도하는 코드가 있으면 이제 <b>테스트에서 깨진다.</b>
     * 그것이 이 변경의 목적이다.
     */
    @Bean
    DynamicPropertyRegistrar observationDataSourceProperties(MySQLContainer mySqlContainer) {
        return registry -> {
            registry.add("observation.datasource.url", mySqlContainer::getJdbcUrl);
            registry.add("observation.datasource.username", () -> OBSERVATION_USERNAME);
            registry.add("observation.datasource.password", () -> OBSERVATION_PASSWORD);
        };
    }

    /**
     * 저장소 루트. 실행 디렉터리가 모듈마다 달라 위로 올라가며 {@code settings.gradle} 로 찾는다 —
     * api 의 {@code ConfigContractFixture} 가 같은 방식을 쓴다. 상대 경로를 박으면 다른 모듈에서
     * 돌릴 때 파일을 못 찾아 컨테이너가 안 뜬다.
     */
    private static java.nio.file.Path repoRoot() {
        java.nio.file.Path candidate = java.nio.file.Path.of("").toAbsolutePath();
        while (candidate != null) {
            if (java.nio.file.Files.isRegularFile(candidate.resolve("settings.gradle"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        throw new IllegalStateException(
                "저장소 루트를 찾지 못했다. 실행 디렉터리: " + java.nio.file.Path.of("").toAbsolutePath());
    }
}
