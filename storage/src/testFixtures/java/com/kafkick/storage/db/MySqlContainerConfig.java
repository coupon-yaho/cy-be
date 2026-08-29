// CLEAN 스키마용 MySQL 컨테이너입니다. JVM 이 하나만 띄웁니다.
package com.kafkick.storage.db;

import java.io.IOException;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.mysql.MySQLContainer;

/**
 * <b>컨테이너를 스프링 컨텍스트가 아니라 JVM 이 소유한다.</b>
 *
 * <h2>왜 바꿨나</h2>
 *
 * <p>한때 이 클래스가 {@code @Bean} 으로 컨테이너를 <b>새로</b> 만들었고, 수명을
 * <i>"스프링 테스트 컨텍스트 캐싱에 맡긴다"</i> 고 적어 뒀다. 그 결과 <b>컨텍스트마다 mysqld 가
 * 하나씩</b> 떴다 — 실측: {@code :batch:test} 가 컨텍스트 30개인데 <b>컨테이너를 44회</b>
 * 띄웠고, 동시에 18개가 살아 약 8GB 를 썼다(하나에 450MB). 개발 기기 Docker VM 이 7.65GB 라
 * 그 선을 넘는 순간 컨테이너가 죽고 다음 컨텍스트가 {@code Connection refused} 로 실패했다 —
 * CI 에서 실제로 세 번 깨졌다(CY-392).
 *
 * <p>그때의 대응은 {@code spring.test.context.cache.maxSize=4} 로 <b>컨텍스트를 줄이는</b>
 * 것이었다. 증상은 멎지만 대가가 재생성 비용이고, 그것이 곧 컨테이너 44회다.
 *
 * <p><b>이제 컨테이너가 컨텍스트에 안 묶인다.</b> 컨텍스트가 밀려나도 mysqld 는 그대로 산다.
 *
 * <h2>어떻게 수명을 뺏나</h2>
 *
 * <p><b>{@code @Bean} 으로 내주되 수명만 뺏는다.</b> {@code @ServiceConnection} 이 컨테이너에서
 * 접속 정보를 읽어 {@code storage.yml} 의 url/계정/드라이버를 덮어써야 하므로 빈으로 내주는
 * 것 자체는 필요하다. 대신 컨테이너가 {@code stop()}·{@code close()} 를 무시한다 —
 * {@code SharedMySqlContainers} 에 이유가 있다.
 *
 * <p><b>{@code @DynamicPropertySource} 로 접속 정보만 넘기는 길은 막혀 있다.</b> 그 애노테이션은
 * 테스트 클래스나 {@code @ContextConfiguration} 에 등록된 클래스에서만 스캔되고,
 * {@code @Import} 된 {@code @TestConfiguration} 은 안 본다 — 실측: 컨텍스트가
 * <i>"Failed to determine a suitable driver class"</i> 로 죽었다.
 *
 * <h2>CLEAN 과 CORRUPT</h2>
 *
 * <p>둘의 차이는 <b>Flyway 로케이션 하나</b>다({@link CorruptSchema}). 그래서 <b>스키마
 * 종류마다 컨테이너 하나</b>다 — CLEAN 은 이 클래스, CORRUPT 는
 * {@link CorruptMySqlContainerConfig}.
 *
 * <p><b>같은 컨테이너의 다른 데이터베이스로 가르는 방법은 안 된다.</b>
 * {@code @ServiceConnection} 이 컨테이너에서 읽은 접속 정보가 <b>인라인 프로퍼티를 이겨서</b>
 * {@code spring.datasource.url} 을 덮어써도 무시되고, CORRUPT 마이그레이션이 CLEAN DB 에
 * 떨어졌다 — {@code CleanSchemaGuard} 가 13건 울었다.
 *
 * <h2>격리는 어디서 오나</h2>
 *
 * <p>컨테이너가 공유되므로 <b>컨텍스트가 곧 빈 DB</b>이던 성질이 사라진다. 대신 규율이 그
 * 자리를 진다 — 데이터를 읽는 테스트는 {@link VerificationSeed#clear()} 로, 잡을 돌리는
 * 테스트는 {@link BatchMetadata#clear} 로 스스로 비운다.
 *
 * <h2>관측 전용 계정</h2>
 *
 * <p><b>관측 풀은 {@code @ServiceConnection} 을 안 따른다</b> — URL 도 계정도. 자동 주입에
 * 맡기면 <i>"관측은 SELECT 전용 계정"</i> 이 조용히 무효가 된다. 그래서 아래
 * {@code observationDataSourceProperties} 가 명시적으로 꽂는다. 그 값은 {@code @NotBlank} 라
 * 등록을 빠뜨리면 컨테이너를 쓰는 테스트가 기동에서 죽는다.
 *
 * <p>계정·권한은 {@code SharedMySqlContainers} 가 올린 {@code 20-obs-account.sh} 와
 * {@code obs-grants/apply.sh} 가 만든다 — <b>compose 가 마운트하는 것과 같은 파일</b>이라
 * 테스트에서 도는 권한이 곧 로컬에서 도는 권한이다.
 *
 * <p><b>새로 쓰는 테스트가 데이터를 읽는다면 반드시 비우고 시작해라.</b> 안 비우면 앞
 * 테스트가 남긴 행을 보고, 실패가 실행 순서에 따라 달라진다.
 */
@TestConfiguration(proxyBeanMethods = false)
public class MySqlContainerConfig {

    private static final MySQLContainer CONTAINER = SharedMySqlContainers.create();

    /**
     * <b>테스트가 이 컨테이너의 정체성을 재려고 쓴다</b>({@code SharedMySqlContainerTest}).
     * 새로 만들지 않고 <b>실제로 쓰이는 것</b>을 봐야 계약을 재는 것이 된다 —
     * 테스트가 컨테이너를 더 띄우면 이 티켓이 줄인 수를 도로 늘린다.
     */
    static MySQLContainer sharedContainer() {
        return CONTAINER;
    }

    /**
     * <b>기동을 여기서 한다.</b> 정적 초기화자에서 띄우면 실패했을 때 이후 접근이 전부
     * {@code NoClassDefFoundError} 가 되어 원인이 사라진다 — {@code SharedMySqlContainers} 참고.
     *
     * <p><b>{@code start()} 의 판정 기준은 "도는가" 가 아니라 {@code containerId != null} 이다</b>
     * (바이트코드 확인). 그래서 아래 {@code isRunning()} 검사는 <b>최초 1회</b> 말고는 분기를
     * 못 만든다 — 죽은 컨테이너를 되살리지는 못한다. 되살아나려면 {@code stop()} 이 먼저
     * {@code containerId} 를 비워야 하고, 그 경로는 {@code SharedMySqlContainers} 가 지킨다.
     */
    @Bean(destroyMethod = "")  // 효과 없음. 의도 표시용 — 실제로 막는 것은 SharedMySqlContainer 다
    @ServiceConnection
    MySQLContainer mySqlContainer() {
        if (!CONTAINER.isRunning()) {
            CONTAINER.start();
        }
        return CONTAINER;
    }

    /**
     * <b>CLEAN 컨테이너에 CORRUPT 로케이션이 얹히는 것을 막는다.</b>
     *
     * <p>이 컨테이너는 여러 컨텍스트가 공유한다. 여기에 {@code db/corrupt} 가 얹히면
     * {@code uk_coupon_member}·{@code uk_coupon_code}·{@code ck_stock_range} 가 떨어져
     * <b>남의 테스트가 깨진다</b> — 그것도 이 테스트가 아니라 <i>나중에 도는</i> 테스트가.
     *
     * <p>원인 자리에서 잡는다. {@link CorruptMySqlContainerConfig} 가 반대 방향을 막는다.
     *
     * <p><b>둘을 함께 {@code @Import} 하는 것도 이 쌍이 막는다.</b> 그러면
     * {@code MySQLContainer} 타입 {@code @ServiceConnection} 빈이 둘이 되어 접속 정보 결정이
     * 모호해지는데 — 로케이션에 {@code db/corrupt} 가 있으면 이 가드가, 없으면 저쪽 가드가
     * 반드시 하나는 운다. 조건이 서로의 부정이라 <b>두 설정이 공존할 수 있는 환경이 없다.</b>
     */
    @Bean
    static BeanFactoryPostProcessor cleanSchemaLocationsGuard(Environment environment) {
        if (CorruptSchema.isCorrupt(environment)) {
            throw new IllegalStateException(
                    "MySqlContainerConfig(CLEAN 컨테이너)를 쓰면서 spring.flyway.locations 에 "
                            + "db/corrupt 가 들어 있습니다. 이 컨테이너는 여러 컨텍스트가 "
                            + "공유하므로 제약을 떨어뜨리면 남의 테스트가 깨집니다 — "
                            + "CorruptMySqlContainerConfig 를 쓰십시오. 현재값="
                            + CorruptSchema.locationsOf(environment));
        }
        return beanFactory -> { };
    }

    /**
     * <b>단일 출처는 {@link SharedMySqlContainers} 다.</b> 그쪽이 컨테이너 계정을 만들고
     * 이쪽이 접속 정보를 꽂으므로, 값을 두 곳에 적으면 한쪽만 고치는 날 컨테이너를 쓰는
     * 모든 테스트가 {@code Access denied} 로 죽는다. 여기서는 참조만 한다.
     */
    private static final String OBSERVATION_USERNAME = SharedMySqlContainers.OBSERVATION_USERNAME;

    private static final String OBSERVATION_PASSWORD = SharedMySqlContainers.OBSERVATION_PASSWORD;

    /** 컨테이너 안에서 양성 목록과 적용 스크립트가 놓이는 자리. */
    private static final String OBS_GRANTS_DIR = "/obs-grants";

    /** 컨텍스트가 뜰 때 앱 표를 비운다. 근거·목록·전제는 {@link AppTableCleaner} 에 있다. */
    @Bean
    static SmartInitializingSingleton appTableCleaner(MySQLContainer mySqlContainer) {
        return AppTableCleaner.of(mySqlContainer);
    }

    /**
     * <b>관측 계정에 양성 목록의 테이블만 SELECT 를 준다</b> — compose 의 {@code obs-grants}
     * 일회성 서비스와 <b>같은 스크립트·같은 목록</b>이다. 그래서 테스트에서 도는 권한이 곧
     * 로컬에서 도는 권한이다.
     *
     * <p><b>왜 {@code SmartInitializingSingleton} 인가.</b> 테이블 단위 GRANT 는 그 테이블이
     * 이미 있어야 한다. Flyway 마이그레이션과 Spring Batch 스키마 초기화는 각자의 빈이 초기화될
     * 때 돌므로, 그 뒤에 확실히 놓이는 자리가 필요하다 — 이 콜백은 <b>모든 싱글턴이 만들어진
     * 뒤</b> 한 번 불린다. {@code @PostConstruct} 나 평범한 {@code @Bean} 으로 두면 순서가
     * 빈 그래프에 따라 달라져, 어떤 컨텍스트에서는 ERROR 1146 으로 죽고 어떤 컨텍스트에서는
     * 통과하는 상태가 된다.
     *
     * <p><b>이미 열린 커넥션은 어떻게 되나.</b> 관측 풀은 이 시점에 이미 접속해 있을 수 있다.
     * MySQL 은 <b>테이블 단위</b> 권한을 문장 실행 시점에 메모리 grant 구조에서 다시 보므로,
     * 접속 뒤에 준 GRANT 가 기존 세션에도 즉시 적용된다.
     *
     * <p><b>스키마 GRANT 를 걷는 경로도 여기서 실제로 돈다(실측).</b> {@code 20-obs-account.sh} 에
     * {@code GRANT SELECT ON app.*} 를 되살려 놓고 돌려 봤더니, 이 스크립트의
     * {@code REVOKE IF EXISTS} 가 그것을 걷어내 {@code ObservationAccountPrivilegeTest} 의
     * members 단언이 그대로 통과했다. 즉 기존 볼륨을 쓰는 환경에서 재부여가 하는 일이
     * 테스트에서도 한 번 실행된다 — 신규 컨테이너에만 도는 죽은 분기가 아니다.
     *
     * <p>실패하면 여기서 던진다. 조용히 넘어가면 권한이 없는 채로 테스트가 돌아
     * "관측이 못 읽는다" 를 계약 위반이 아니라 환경 문제로 오해하게 된다.
     */
    @Bean
    SmartInitializingSingleton observationGrantApplier(MySQLContainer mySqlContainer) {
        return () -> applyObservationGrants(mySqlContainer);
    }

    /**
     * 재부여 스크립트를 컨테이너 안에서 한 번 돌린다.
     *
     * <p><b>{@code public} 인 이유</b> — 재부여의 <b>반대 구성</b>(레거시 과다 권한이 남아 있는
     * 상태)을 단언하는 테스트가 같은 호출을 다시 써야 한다. 그 테스트가 {@code execInContainer}
     * 를 따로 적으면 호출 형태가 둘이 되어, 여기만 고쳐도 테스트는 옛 형태를 계속 돌린다.
     */
    public static void applyObservationGrants(MySQLContainer mySqlContainer) {
        ExecResult result;
        try {
            result = mySqlContainer.execInContainer(
                    "env",
                    // Testcontainers 는 root 비밀번호를 테스트 계정과 같은 값으로 넣는다.
                    "MYSQL_ROOT_PASSWORD=" + mySqlContainer.getPassword(),
                    "MYSQL_DATABASE=" + mySqlContainer.getDatabaseName(),
                    "DB_OBS_USERNAME=" + OBSERVATION_USERNAME,
                    "sh", OBS_GRANTS_DIR + "/apply.sh");
        } catch (IOException e) {
            throw new IllegalStateException("관측 계정 재부여 스크립트를 실행하지 못했다", e);
        } catch (InterruptedException e) {
            // 인터럽트 플래그는 **정말 인터럽트됐을 때만** 되살린다. IOException 과 한 덩이로
            // 잡으면 단순 입출력 실패에도 스레드를 인터럽트 상태로 만들어, 그 뒤의 대기가
            // 엉뚱하게 즉시 깨진다.
            Thread.currentThread().interrupt();
            throw new IllegalStateException("관측 계정 재부여 스크립트 실행이 중단됐다", e);
        }
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                    "관측 계정 재부여 실패(exit=" + result.getExitCode() + ")\n"
                            + result.getStdout() + result.getStderr());
        }
    }

    /**
     * root 로 SQL 한 덩이를 돌리고 표준출력을 돌려준다. 권한 상태를 <b>서버가 보는 그대로</b>
     * 읽기 위한 통로다 — 풀 커넥션으로 읽으면 접속 시점에 캐시된 스키마·전역 권한이 섞인다.
     */
    public static String executeAsRoot(MySQLContainer mySqlContainer, String sql) {
        try {
            ExecResult result = mySqlContainer.execInContainer(
                    "env", "MYSQL_PWD=" + mySqlContainer.getPassword(),
                    "mysql", "-uroot", "-N", "-e", sql);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException(
                        "root SQL 실패(exit=" + result.getExitCode() + "): " + result.getStderr());
            }
            return result.getStdout();
        } catch (IOException e) {
            throw new IllegalStateException("root SQL 을 실행하지 못했다", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("root SQL 실행이 중단됐다", e);
        }
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

}
