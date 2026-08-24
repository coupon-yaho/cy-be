package com.kafkick.api.observation.resource;

import static org.assertj.core.api.Assertions.assertThat;

import javax.sql.DataSource;

import com.kafkick.ApiApplication;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestConstructor;

@SpringBootTest(classes = ApiApplication.class, properties = {
    "observation.datasource.enabled=true",
    "coupon.idempotency.wait-timeout=1s",
    "coupon.idempotency.poll-interval=50ms",
    "coupon.idempotency.stale-after=30s",
    "coupon.round-generation.schedule-zone=Asia/Seoul",
    "coupon.round-generation.max-days=30",
})
@Import(MySqlContainerConfig.class)
// 생성자 주입을 쓰려면 이 선언이 있어야 한다 — 없으면 JUnit 이 파라미터를 못 풀고
// ParameterResolutionException 으로 죽는다.
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
class ResourceProviderMySqlIntegrationTest {

    private final DataSource dataSource;

    private final ResourceProvider resourceProvider;

    /**
     * @param dataSource ResourceProvider 가 재는 것과 같은 풀이어야 한다. 여기서 딴 풀의
     *                   커넥션을 열면 아래 total >= 1 단언이 무엇을 검증하는지 알 수 없어진다.
     */
    ResourceProviderMySqlIntegrationTest(
            @Qualifier("mainDataSource") DataSource dataSource,
            ResourceProvider resourceProvider
    ) {
        this.dataSource = dataSource;
        this.resourceProvider = resourceProvider;
    }

    @Test
    @DisplayName("실제 MySQL에 기동한 HikariPoolMXBean의 active · total · awaiting을 읽는다")
    void readsTheRealHikariPoolMxBean() throws Exception {
        // 풀과 MXBean을 실제 원천에 붙여 기동한다. Provider 자체는 커넥션을 얻지 않는다.
        dataSource.getConnection().close();
        ResourceSnapshot.DbPool pool = resourceProvider.snapshot().dbPool();

        assertThat(pool.active().state()).isEqualTo(SourceStatus.VALID);
        assertThat(pool.total().state()).isEqualTo(SourceStatus.VALID);
        assertThat(pool.total().value()).isGreaterThanOrEqualTo(1.0);
        assertThat(pool.awaiting().state()).isEqualTo(SourceStatus.VALID);
        assertThat(pool.awaiting().value()).isZero();
    }
}
