// CORRUPT 스키마 전용 MySQL 컨테이너입니다. JVM 이 하나만 띄웁니다.
package com.kafkick.storage.db;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.testcontainers.mysql.MySQLContainer;

/**
 * <b>{@link MySqlContainerConfig} 와 설정이 같고, 컨테이너만 별개다.</b>
 *
 * <h2>왜 컨테이너를 나누나</h2>
 *
 * <p>CLEAN 과 CORRUPT 는 <b>Flyway 로케이션 하나</b>만 다르다({@link CorruptSchema}) — 스키마
 * 모양이 다를 뿐이라 <i>같은 mysqld 안의 다른 데이터베이스</i>면 될 것 같다. <b>그런데 안 된다.</b>
 *
 * <p>{@code @ServiceConnection} 이 컨테이너에서 읽은 접속 정보가 <b>인라인 프로퍼티를 이긴다.</b>
 * {@code spring.datasource.url} 을 초기화자로 덮어써도 무시되고, CORRUPT 마이그레이션이
 * <b>CLEAN DB 에 떨어진다</b> — 실측: {@code CleanSchemaGuard} 가
 * <i>"dataset=CLEAN 인데 uk_coupon_member 가 없습니다"</i> 로 13건 울었다.
 *
 * <p>그래서 <b>스키마 종류마다 컨테이너 하나</b>로 간다. {@code docs/13} §7 이 예고한 모양이다 —
 * <i>"단순 싱글턴이 안 된다. 스키마 종류별로 갈린 JVM 싱글턴이 필요하다."</i>
 *
 * <h2>CLEAN 컨테이너를 딸려 띄우지 않는다</h2>
 *
 * <p>한때 이 클래스가 {@code MySqlContainerConfig.createContainer()} 를 불렀다. {@code static}
 * 메서드 호출은 <b>그 클래스의 정적 초기화를 강제하므로</b>(JLS §12.4.1) CLEAN 컨테이너까지
 * 함께 떴다 — 실측: CORRUPT 테스트만 골라 돌려도 <b>컨테이너가 2회</b> 떴다. 지금은 팩토리가
 * {@code SharedMySqlContainers} 로 빠져 그 연쇄가 없다.
 *
 * <h2>쓰는 쪽 — 둘을 함께 줘야 한다</h2>
 *
 * <p>{@link CorruptRepositoryTest} 는 메타 애노테이션이라 <b>둘이 원자적으로 붙는다.</b>
 * 잡 테스트는 애노테이션을 공유할 수 없어({@code @DataJpaTest} 대 {@code @SpringBootTest})
 * 각자 {@code @Import} 와 {@link CorruptSchema#FLYWAY_LOCATIONS} 를 나열한다.
 *
 * <p><b>하나만 주면 조용히 틀린다</b> — 그래서 아래 가드가 기동 시점에 죽인다.
 * 로케이션만 주면 CLEAN 컨테이너에 제약을 떨어뜨려 <i>남의 테스트</i>가 깨지고
 * ({@link MySqlContainerConfig} 쪽 가드가 잡는다), 이 설정만 주면 제약이 남아 위반 INSERT 가
 * 튕겨 <i>"검출 0건"</i> 이 초록으로 나온다 — 후자가 더 나쁘다. 그 테스트가 세는 것이 검출
 * 건수라, 심을 대상이 애초에 안 생겨도 <b>기대와 실제가 둘 다 0</b>이 되기 때문이다.
 */
@TestConfiguration(proxyBeanMethods = false)
public class CorruptMySqlContainerConfig {

    private static final MySQLContainer CONTAINER = SharedMySqlContainers.create();

    /**
     * <b>테스트가 이 컨테이너의 정체성을 재려고 쓴다</b>({@code SharedMySqlContainerTest}).
     * 새로 만들지 않고 <b>실제로 쓰이는 것</b>을 봐야 계약을 재는 것이 된다 —
     * 테스트가 컨테이너를 더 띄우면 이 티켓이 줄인 수를 도로 늘린다.
     */
    static MySQLContainer sharedContainer() {
        return CONTAINER;
    }

    /** 기동을 여기서 하는 이유는 {@link MySqlContainerConfig#mySqlContainer()} 와 같다. */
    @Bean(destroyMethod = "")
    @ServiceConnection
    MySQLContainer corruptMySqlContainer() {
        if (!CONTAINER.isRunning()) {
            CONTAINER.start();
        }
        return CONTAINER;
    }

    /**
     * <b>로케이션을 함께 주지 않으면 여기서 죽는다.</b> 위 "쓰는 쪽" 참고 — 이것이 없으면
     * 실패가 <i>"검출 0건"</i> 이라는 <b>초록</b>으로 나타난다.
     */
    @Bean
    static BeanFactoryPostProcessor corruptLocationsGuard(Environment environment) {
        String locations = environment.getProperty("spring.flyway.locations", "");
        if (!locations.contains("db/corrupt")) {
            throw new IllegalStateException(
                    "CorruptMySqlContainerConfig 를 @Import 했는데 spring.flyway.locations 에 "
                            + "db/corrupt 가 없습니다. CLEAN 제약이 남아 있어 위반 INSERT 가 튕기고, "
                            + "그 테스트는 '검출 0건'으로 초록이 됩니다 — "
                            + "CorruptSchema.FLYWAY_LOCATIONS 를 함께 주십시오. 현재값=" + locations);
        }
        return beanFactory -> { };
    }
}
