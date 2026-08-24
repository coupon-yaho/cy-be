package com.kafkick.api.admin.batch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.kafkick.core.batch.BatchExecutionRepository;
import com.kafkick.storage.db.MySqlContainerConfig;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * <b>이력 조회가 관측 전용 풀로 나가는지</b>를 풀 이름 미터로 확인한다.
 *
 * <p>이 방향의 실수는 <b>에러가 나지 않는다.</b> 운영 풀로 새어도 값은 정상적으로 돌아오고,
 * 부하 회차 중에 커넥션을 물어 측정만 오염시킨다. 그래서 배선을 눈으로 보는 것으로는 부족하고
 * 실제로 질의를 한 번 태워 <b>어느 풀이 일했는지</b>를 센다.
 *
 * <p><b>{@code pool-name} 을 프로퍼티로 직접 준다.</b> 실제 값은 {@code storage.yml} 에 있는데
 * 그 파일은 커밋하지 않는다 — 거기 기대면 신규 클론과 CI 에서 깨진다. 템플릿에 그 값이
 * 적혀 있는지는 storage 의 {@code StorageYamlTemplateTest} 가 본다.
 *
 * <p><b>좁힌 컨텍스트가 아니라 api 앱을 통째로 띄운다.</b> 배치 메타 테이블이 있어야 질의가
 * 살고, 그 마이그레이션의 소유자가 api 다(batch 는 {@code spring.flyway.enabled: false}).
 * 자동설정을 골라 담으면 실제 기동 경로와 다른 것을 검증하게 되고, Flyway 는 api 에서
 * {@code runtimeOnly} 라 컴파일 타임에 지목할 수도 없다.
 *
 * <p><b>{@code classes} 를 명시한다.</b> 안 적으면 스프링이 패키지를 거슬러 올라가며
 * {@code @SpringBootConfiguration} 을 찾는데, {@code com.kafkick.api} 에 있는 다른 테스트의
 * {@code TestApp} 이 먼저 잡혀 <b>엉뚱한 컨텍스트가 뜬다</b>(실제로 그렇게 죽었다 —
 * 헬스 그룹이 지목하는 기여자가 없다는 오류였다).
 */
@SpringBootTest(classes = com.kafkick.ApiApplication.class, properties = {
        "spring.datasource.hikari.pool-name=main-pool",
        "observation.datasource.hikari.pool-name=obs-pool",
})
@Import(MySqlContainerConfig.class)
class BatchHistoryObservationPoolTest {

    private static final String ACQUIRE = "hikaricp.connections.acquire";

    @Autowired
    BatchExecutionRepository repository;
    @Autowired
    MeterRegistry registry;

    private long acquireCount(String poolName) {
        var timer = registry.find(ACQUIRE).tag("pool", poolName).timer();
        return timer == null ? 0 : timer.count();
    }

    @Test
    @DisplayName("이력 조회는 관측 풀에서만 커넥션을 얻는다")
    void historyQueryUsesObservationPool() {
        long obsBefore = acquireCount("obs-pool");
        long mainBefore = acquireCount("main-pool");

        // 행이 없어도 된다. 세는 것은 결과가 아니라 어느 풀이 커넥션을 내줬는가다.
        repository.findRecent(10);
        repository.findRecentByJobName("expireJob", 10);

        assertThat(acquireCount("obs-pool"))
                .as("관측 풀이 일했어야 한다")
                .isGreaterThan(obsBefore);
        assertThat(acquireCount("main-pool"))
                .as("운영 풀은 이 조회에 관여하면 안 된다 — 새어도 에러가 안 나는 방향이다")
                .isEqualTo(mainBefore);
    }

}
