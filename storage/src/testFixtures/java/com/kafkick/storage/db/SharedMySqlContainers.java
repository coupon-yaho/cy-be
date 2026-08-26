// 테스트용 MySQL 컨테이너를 만드는 자리입니다. 수명의 주인은 JVM 입니다.
package com.kafkick.storage.db;

import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * <b>만들기만 한다. 띄우지 않는다.</b>
 *
 * <h2>왜 별도 클래스인가</h2>
 *
 * <p>한때 이 팩토리가 {@link MySqlContainerConfig} 안에 있었고,
 * {@link CorruptMySqlContainerConfig} 가 그것을 불렀다. 그런데 {@code static} 메서드 호출은
 * <b>그 클래스의 정적 초기화를 강제한다</b>(JLS §12.4.1) — CLEAN 설정의
 * {@code static { CONTAINER.start(); }} 가 함께 돌아서, <b>CORRUPT 만 쓰는 실행도 CLEAN
 * 컨테이너를 띄웠다.</b>
 *
 * <p>실측: {@code ./gradlew :storage:test --tests '*Corrupt*'} 가 컨테이너를 <b>2회</b> 띄웠다.
 * <i>"스키마 종류마다 하나"</i> 라고 적어 놓고 실제로는 <i>"CORRUPT 를 건드리면 무조건 둘"</i>
 * 이었다. 팩토리를 밖으로 빼면 그 연쇄가 끊긴다.
 *
 * <h2>기동은 {@code @Bean} 에서 한다</h2>
 *
 * <p>정적 초기화자에서 띄우면 안 된다. 기동이 실패한 클래스는 {@code erroneous} 로 표시되어
 * <b>이후 모든 접근이 {@code NoClassDefFoundError} 만 낸다</b>(JLS §12.4.2) — 원인 스택이
 * 첫 실패 하나에만 붙고, 리포트에는 수백 건의 {@code Could not initialize class} 가 남는다.
 * 이 저장소는 이미 <i>"컨테이너 기동 실패로 보여서 원인까지 가는 길이 멀다"</i> 로 시간을
 * 태운 적이 있다. 그 거리를 더 늘리지 않는다.
 */
final class SharedMySqlContainers {

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

    /** 연결 상한의 기본값. {@code build.gradle} 이 컨텍스트 캐시 상한에서 계산해 넘긴다. */
    private static final String MAX_CONNECTIONS_PROPERTY = "test.mysql.maxConnections";

    private SharedMySqlContainers() {
    }

    /**
     * <b>{@code stop()} 을 받지 않는 컨테이너.</b> 수명의 주인이 JVM 이라는 결정을 타입으로 박는다.
     *
     * <p>스프링이 컨텍스트를 닫을 때마다 {@code stop()} 을 부르는데, 싱글턴에 그것이 먹히면
     * <b>먼저 닫힌 컨텍스트가 남의 컨테이너를 끈다.</b> 그 뒤 {@code start()} 는 같은 객체에
     * <b>새 컨테이너를 새로 띄운다</b> — 그래서 막기 전에는 44회가 39회로만 줄었다.
     *
     * <p><b>{@code @Bean(destroyMethod = "")} 로는 못 막는다.</b> 그것은 스프링의 표준 소멸
     * 훅이고, 부트의 {@code TestcontainersLifecycleBeanPostProcessor} 는
     * {@code DestructionAwareBeanPostProcessor} 라 그 속성과 무관하게 {@code Startable.stop()}
     * 을 부른다.
     *
     * <p>정리는 Testcontainers 가 한다 — Ryuk 사이드카가 기본이고, 그것을 끄면
     * {@code JVMHookResourceReaper} 가 Docker API 로 직접 지운다. 둘 다 {@code stop()} 을
     * 안 타므로 이 오버라이드가 회수를 막지 않는다 — {@link #warnIfRyukDisabled()} 참고.
     */
    private static final class SharedMySqlContainer extends MySQLContainer {

        private SharedMySqlContainer(DockerImageName image) {
            super(image);
        }

        @Override
        public void stop() {
            // 일부러 비운다. 위 javadoc 참고.
        }

        @Override
        public void close() {
            // try-with-resources 로도 안 닫힌다. Startable.close() 가 stop() 을 부른다.
        }
    }

    /**
     * <b>Ryuk 이 꺼져 있으면 무엇이 달라지는지 알린다. 죽이지는 않는다.</b>
     *
     * <p>한때 여기서 예외를 던졌다. 근거는 <i>"{@code stop()}·{@code close()} 를 비운 순간부터
     * 회수 경로가 Ryuk 하나뿐"</i> 이었는데 <b>그 전제가 틀렸다.</b> 실측(testcontainers 2.0.5
     * 바이트코드):
     *
     * <ul>
     *   <li>{@code ResourceReaper.instance()} 가 {@code TESTCONTAINERS_RYUK_DISABLED} 를 읽어
     *       참이면 경고를 찍고 {@code JVMHookResourceReaper} 를 쓴다.</li>
     *   <li>그 구현은 {@code performCleanup()} 에서 <b>Docker API 로 직접 지운다</b>
     *       ({@code removeContainerCmd}) — {@code GenericContainer.stop()} 을 안 탄다.
     *       즉 이 클래스의 오버라이드가 그것을 무력화하지 않는다.</li>
     * </ul>
     *
     * <p>그래서 rootless Podman·Bitbucket 처럼 Ryuk 을 못 쓰는 환경에서도 <b>정상 종료면
     * 걷힌다.</b> 거기서 빌드를 통째로 막는 것은 과하다.
     *
     * <p><b>다만 같지는 않다.</b> JVM 훅은 <b>비정상 종료에 안 돈다</b> — {@code kill -9},
     * OOM 킬, 러너 강제 종료면 컨테이너가 남는다. Ryuk 은 사이드카라 그 경우에도 걷는다.
     * 그 차이를 알고 쓰라고 한 줄 남긴다.
     */
    private static void warnIfRyukDisabled() {
        if (!Boolean.parseBoolean(System.getenv("TESTCONTAINERS_RYUK_DISABLED"))) {
            return;
        }
        System.err.println(
                "[cy-be] TESTCONTAINERS_RYUK_DISABLED 가 켜져 있습니다. "
                        + "JVMHookResourceReaper 가 정상 종료에서 컨테이너를 걷지만, "
                        + "kill -9·OOM 킬 같은 비정상 종료에는 안 돕니다 — "
                        + "이 컨테이너는 stop() 을 무시하므로 그때는 손으로 지워야 합니다.");
    }

    /**
     * 컨테이너를 만든다. <b>띄우지는 않는다</b> — 부르는 쪽이 {@code @Bean} 안에서 띄운다.
     */
    static MySQLContainer create() {
        warnIfRyukDisabled();
        return new SharedMySqlContainer(IMAGE)
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
                        // **컨테이너를 공유하면서 제약이 옮겨 갔다.** 예전에는 컨텍스트마다
                        // mysqld 가 따로라 연결이 흩어졌는데, 지금은 캐시된 컨텍스트 전부가
                        // 각자 Hikari 풀을 들고 **한 서버**에 붙는다. 기본값 151 로는
                        // maxSize=32 에서 "Too many connections" 가 282번 났다(실측).
                        //
                        // **값은 build.gradle 이 컨텍스트 캐시 상한에서 계산해 넘긴다.**
                        // 한쪽만 바꾸면 증상이 원인과 안 닮기 때문이다 — Too many connections 를
                        // 받은 사람은 Hikari 를 의심하지 mysqld 인자를 의심하지 않는다.
                        "--max-connections=" + System.getProperty(MAX_CONNECTIONS_PROPERTY, "1000"),
                        // 위 주석대로 binlog 는 테스트에 불필요한데 이미지 기본값이 ON 이었다.
                        // 켜져 있으면 SUPER 없는 계정이 트리거를 못 만들어(오류 1419),
                        // "실행 중에 데이터가 바뀐다" 를 재현하는 테스트를 쓸 수 없다.
                        "--skip-log-bin");
    }
}
