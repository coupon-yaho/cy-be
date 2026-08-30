// 컨테이너 소유권 계약을 기계로 지킵니다. CY-621 이 세운 성질입니다.
package com.kafkick.storage.db;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;

/**
 * <b>CY-621 이 세운 성질을 잰다.</b> 이 티켓의 존재 이유(컨테이너 44회 → 4회)가 여기 걸려
 * 있고, 회귀가 나도 <b>다른 어떤 테스트도 원인을 안 짚어 준다.</b>
 *
 * <p><b>스프링 컨텍스트를 안 띄운다.</b> 두 설정을 한 컨텍스트에 물면 가드가 먼저 죽인다 —
 * 재려는 것은 그것이 아니라 <b>싱글턴의 수명</b>이다.
 *
 * <p><b>컨테이너를 새로 안 띄운다.</b> 두 설정이 실제로 쓰는 그 객체를 받아 {@code start()}
 * 한다. 다른 테스트가 이미 띄웠으면 멱등이라, 이 테스트 때문에 늘어나는 컨테이너는 없다.
 *
 * <p><b>CLEAN 컨테이너에 제약이 살아 있는지는 여기서 안 잰다.</b>
 * {@code CleanSchemaConstraintTest} 가 실제 위반 INSERT 로 {@code uk_coupon_member}·
 * {@code uk_coupon_code}·{@code ck_stock_range} 를 이미 확인한다 — 같은 것을 두 군데서
 * 관리하지 않는다. 한때 여기에 그 이름을 단 테스트를 뒀는데 <b>실제로는 DB 이름만 봤다.</b>
 * 오염돼도 이름은 안 바뀌므로 잡을 수 없는 테스트였다.
 */
class SharedMySqlContainerTest {

    /**
     * <b>{@code stop()} 오버라이드가 사라지면 여기서 잡힌다.</b> 그것이 없으면 컨텍스트가
     * 닫힐 때 컨테이너가 죽고, 다음 {@code start()} 가 <b>새 컨테이너를 띄운다</b> —
     * 44회가 39회로만 줄던 그 상태다.
     */
    @Test
    @DisplayName("기동한 뒤에는 stop() 을 삼킨다 — 수명의 주인은 JVM 이다")
    void swallowsStopAfterStart() {
        MySQLContainer container = MySqlContainerConfig.sharedContainer();
        container.start();
        String id = container.getContainerId();
        int delegatedBefore = SharedMySqlContainers.delegatedStopCount(container);

        container.stop();
        container.close();

        assertThat(SharedMySqlContainers.delegatedStopCount(container))
                .as("기동한 뒤의 stop() 은 super 로 내려가면 안 된다 — 내려가면 남의 컨테이너가 꺼진다")
                .isEqualTo(delegatedBefore);
        assertThat(container.isRunning()).isTrue();
        assertThat(container.getContainerId())
                .as("같은 컨테이너여야 한다 — id 가 바뀌면 새로 띄운 것이다")
                .isEqualTo(id);
    }

    /**
     * <b>이것이 blocker 의 회귀 테스트다.</b>
     *
     * <p>{@code stop()} 을 <b>무조건</b> 비우면 Testcontainers 자신의 기동 실패 정리가 죽는다 —
     * {@code tryStart} 의 실패 갈래가 {@code stop()} 을 부르는데, 그것이 {@code containerId} 를
     * 비우는 유일한 자리이기 때문이다. 막히면 죽은 id 가 고정되고 {@code start()} 가 즉시
     * return 해, <b>JVM 안의 모든 뒤 컨텍스트가 죽은 컨테이너를 받는다.</b>
     *
     * <p><b>위임 횟수를 재는 것이 핵심이다.</b> 한때 여기서 {@code startedOnce} 만 다시 봤는데,
     * 그 값은 삼키든 위임하든 안 바뀐다 — <b>옛 무조건 no-op 에서도 통과하는 무력한 테스트</b>
     * 였다. 기동 전 {@code stop()} 은 {@code containerId} 가 null 이라 {@code super.stop()} 도
     * 즉시 돌아오므로, 바깥에서 결과가 같아 보인다. 카운터만이 그 둘을 가른다.
     */
    @Test
    @DisplayName("기동 전에는 stop() 이 super 로 내려간다 — 안 그러면 기동 실패가 죽은 id 를 고정한다")
    void delegatesStopBeforeFirstStart() {
        MySQLContainer fresh = SharedMySqlContainers.create();

        assertThat(SharedMySqlContainers.hasStartedOnce(fresh))
                .as("만들기만 한 컨테이너는 아직 기동 전이다")
                .isFalse();
        assertThat(SharedMySqlContainers.delegatedStopCount(fresh)).isZero();

        fresh.stop();

        assertThat(SharedMySqlContainers.delegatedStopCount(fresh))
                .as("무조건 no-op 이면 0 이다 — 그러면 tryStart 의 실패 정리가 죽는다")
                .isEqualTo(1);
        assertThat(SharedMySqlContainers.hasStartedOnce(fresh))
                .as("stop() 이 상태를 바꾸면 안 된다")
                .isFalse();
    }

    /**
     * {@code Startable.close()} 의 기본 구현이 {@code this.stop()} 한 줄이라 같은 조건을 탄다.
     * <b>{@code close()} 를 따로 비우면 조건을 건너뛰어</b> 기동 실패 정리가 다시 죽는다.
     */
    @Test
    @DisplayName("close() 도 같은 조건을 탄다 — 따로 비우면 정리가 다시 죽는다")
    void closeFollowsTheSameCondition() {
        MySQLContainer fresh = SharedMySqlContainers.create();

        fresh.close();

        assertThat(SharedMySqlContainers.delegatedStopCount(fresh))
                .as("close() 가 stop() 을 타야 한다. 오버라이드로 비우면 여기가 0 이 된다")
                .isEqualTo(1);
    }
}
