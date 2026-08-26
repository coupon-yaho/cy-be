// CLEAN 스키마용 MySQL 컨테이너입니다. JVM 이 하나만 띄웁니다.
package com.kafkick.storage.db;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
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
     * <p>{@code start()} 는 이미 도는 컨테이너에 멱등이다. {@code isRunning()} 을 명시하는 것은
     * 읽는 사람을 위해서다.
     */
    @Bean(destroyMethod = "")
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
        String locations = environment.getProperty("spring.flyway.locations", "");
        if (locations.contains("db/corrupt")) {
            throw new IllegalStateException(
                    "MySqlContainerConfig(CLEAN 컨테이너)를 쓰면서 spring.flyway.locations 에 "
                            + "db/corrupt 가 들어 있습니다. 이 컨테이너는 여러 컨텍스트가 "
                            + "공유하므로 제약을 떨어뜨리면 남의 테스트가 깨집니다 — "
                            + "CorruptMySqlContainerConfig 를 쓰십시오. 현재값=" + locations);
        }
        return beanFactory -> { };
    }
}
