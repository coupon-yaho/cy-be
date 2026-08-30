// 부하 중 정지 스위치가 스케줄러를 실제로 없애는지 확인합니다.
package com.kafkick.batch.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>이 잡은 재고를 쓴다.</b> 부하 측정 중에 돌면 같은 DB 를 때려 측정값이 흔들리고,
 * 검증이 도는 중에 돌면 {@code sum(active_count)} 가 바뀌어 <b>판정이 데이터 변동으로 죽는다.</b>
 *
 * <p>그래서 끄는 수단이 있어야 하는데, <b>있다는 것만으로는 부족하다.</b> 여기서는 그 스위치가
 * <b>빈 자체를 없애는지</b>를 본다 — 메서드 안에서 검사하고 빠져나오는 방식이면
 * 그 검사를 안 넣은 스케줄러가 나중에 하나 생기는 순간 조용히 돈다.
 *
 * <p><b>세 번째 경우가 실제로 제일 위험하다 — 아무 값도 안 준 채 뜨는 것.</b> 검증용 DB 를
 * 보게 띄우면서 환경변수를 빠뜨리는 상황이 그것이고, 그때 돌면 검증 셋을 고쳐 버린다.
 * CORRUPT 셋은 일부러 심은 오염까지 건드려 expected 집합과 어긋나는데,
 * <b>셋을 다시 만드는 것 말고 되돌릴 방법이 없다.</b> 그래서 모를 때는 안 도는 쪽으로 정했고,
 * 그 결정을 지키는 것이 {@link WhenUnset} 이다.
 */
class ExpireSchedulerTest {

    @Nested
    @SpringBootTest(properties = {
            "spring.batch.job.enabled=false",
            "batch.scheduling.enabled=false"
    })
    @Import(MySqlContainerConfig.class)
    @DisplayName("꺼져 있을 때")
    class WhenDisabled {

        @Autowired
        private ApplicationContext context;

        @Test
        @DisplayName("스케줄러 빈이 아예 만들어지지 않는다")
        void schedulerBeanIsAbsent() {
            assertThat(context.getBeanNamesForType(ExpireScheduler.class))
                    .as("빈이 있으면 크론이 살아 있다")
                    .isEmpty();
        }
    }

    @Nested
    @SpringBootTest(properties = {
            "spring.batch.job.enabled=false",
            "batch.scheduling.enabled=true",
            // 크론을 먼 미래로 밀어 테스트 중에 실제로 돌지 않게 한다. 빈 존재만 본다.
            "batch.schedule.expire-cron=0 0 0 1 1 *",
        // 이 클래스는 스케줄러 빈이 만들어지는지만 본다 — 그래서 플래그를 켜 둬야 한다.
        // 발화는 크론을 1월 1일로 밀어 막는다. **그 시각에 도는 CI 는 이 잡을 한 번
        // 실행한다** — 연 1회 1초짜리 창이고, 스케줄러를 끄면 이 클래스가 재려는
        // 축이 사라지므로 그 창을 남긴다. 없애려면 크론이 아니라 트리거를 갈아야 한다.
        //
        // 연 단위 크론으로는 어떤 SLA 도 못 맞춰 기동 가드가 거절하므로,
        // 그 검사가 여기서 뜻이 없다는 것을 값으로 명시한다.
        // 정리 크론도 함께 민다. 그러지 않으면 04:30 UTC(13:30 KST)를 지나며 도는 CI 에서
        // 진짜 정리가 발화해 asof_state · verification_findings · 통계 세 테이블을 지운다 —
        // MySqlContainerConfig 는 컨테이너를 공유하므로 무관한 검증 테스트가 그날만 빨개진다.
        // 연 1회 크론은 CleanupScheduler 의 SLA 가드에 걸리므로 SLA 도 함께 올린다.
        "batch.schedule.cleanup-cron=0 0 0 1 1 *",
        "batch.metrics.cleanup-sla-seconds=999999999",
        "batch.metrics.expire-sla-seconds=999999999",
        // 검증 크론도 함께 민다(CY-470). 기본값 05:00 UTC 를 그대로 두면
        // 그 시각을 지나며 도는 CI 에서 진짜 검증이 발화해, 공유 컨테이너의
        // asof_state 를 300만 행까지 채우고 다른 테스트의 전제를 바꾼다 —
        // 위 정리 크론을 민 것과 같은 이유다. 연 1회는 SLA 가드에 걸려 SLA 도 올린다.
        "batch.schedule.verify-cron=0 0 0 1 1 *",
        "batch.metrics.verify-sla-seconds=999999999"
    })
    @Import(MySqlContainerConfig.class)
    @DisplayName("켜져 있을 때")
    class WhenEnabled {

        @Autowired
        private ApplicationContext context;

        @Test
        @DisplayName("스케줄러 빈이 있다")
        void schedulerBeanExists() {
            assertThat(context.getBeanNamesForType(ExpireScheduler.class))
                    .as("꺼진 쪽만 확인하면 '항상 없다' 로도 통과한다")
                    .hasSize(1);
        }
    }

    /**
     * <b>아무 값도 주지 않은 채 뜬 경우.</b> 실제 설정 파일도 안 읽으므로 속성 자체가 없다 —
     * {@code matchIfMissing} 만 남는 상황이고, 그 값이 이 결정의 전부다.
     *
     * <p>되돌리면 여기가 빨개진다. 꺼진 채 운영을 보는 반대편 사고는 나중에 돌려 따라잡을 수 있고
     * {@code ExpireNotSucceeding} 알림도 잡아 주므로, 무게가 같지 않다.
     */
    @Nested
    @SpringBootTest(properties = "spring.batch.job.enabled=false")
    @Import(MySqlContainerConfig.class)
    @DisplayName("아무 값도 안 줬을 때")
    class WhenUnset {

        @Autowired
        private ApplicationContext context;

        @Test
        @DisplayName("스케줄러 빈이 만들어지지 않는다 — 모를 때는 안 돈다")
        void schedulerBeanIsAbsent() {
            assertThat(context.getBeanNamesForType(ExpireScheduler.class))
                    .as("속성이 없을 때 도는 쪽이면, 검증용으로 띄우다 환경변수를 빠뜨린 날 "
                            + "검증 셋이 오염된다")
                    .isEmpty();
        }
    }
}
