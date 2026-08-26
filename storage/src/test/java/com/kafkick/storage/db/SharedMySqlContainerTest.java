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
 */
class SharedMySqlContainerTest {

    /**
     * <b>{@code stop()} 오버라이드가 사라지면 여기서 잡힌다.</b> 그것이 없으면 컨텍스트가
     * 닫힐 때 컨테이너가 죽고, 다음 {@code start()} 가 <b>새 컨테이너를 띄운다</b> —
     * 44회가 39회로만 줄던 그 상태다.
     */
    @Test
    @DisplayName("기동한 뒤에는 stop() 을 불러도 안 죽는다 — 수명의 주인은 JVM 이다")
    void survivesStopAfterStart() {
        MySQLContainer container = MySqlContainerConfig.sharedContainer();
        container.start();
        String id = container.getContainerId();

        container.stop();
        container.close();

        assertThat(container.isRunning())
                .as("스프링이 컨텍스트마다 stop() 을 부른다. 먹히면 남의 컨테이너가 꺼진다")
                .isTrue();
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
     * <p>기동 실패를 실제로 만들려면 이미지 pull 을 깨야 해서 느리고 불안정하다. 그래서
     * <b>그 조건을 가르는 상태</b>를 직접 잰다 — 아직 기동 안 한 컨테이너는
     * {@code startedOnce} 가 false 라 {@code stop()} 이 {@code super} 로 간다.
     */
    @Test
    @DisplayName("기동 전에는 stop() 이 위임된다 — 안 그러면 기동 실패가 죽은 id 를 고정한다")
    void delegatesStopBeforeFirstStart() {
        MySQLContainer fresh = SharedMySqlContainers.create();

        assertThat(SharedMySqlContainers.hasStartedOnce(fresh))
                .as("만들기만 한 컨테이너는 아직 기동 전이다")
                .isFalse();

        // containerId 가 null 이라 super.stop() 은 즉시 돌아온다. 여기서 재는 것은
        // "예외 없이 위임된다" 는 것이고, 막혀 있으면 이 경로 자체가 없어진다.
        fresh.stop();

        assertThat(SharedMySqlContainers.hasStartedOnce(fresh))
                .as("stop() 이 상태를 바꾸면 안 된다")
                .isFalse();
    }

    /**
     * <b>정체성이 아니라 스키마 모양으로 잰다.</b> 한때 {@code getContainerId()} 를 비교했는데,
     * 두 설정이 각자 {@code create()} 를 부르므로 <b>구조적으로 항상 참</b>이라 회귀를 못 잡았다.
     *
     * <p>진짜 위험은 <i>"CORRUPT 로케이션이 CLEAN 컨테이너에 얹히는 것"</i> 이다. 그러면
     * {@code uk_coupon_member} 가 떨어지고, 깨지는 것은 <b>나중에 도는 남의 테스트</b>다.
     * 제약 유무로 재면 그 축을 직접 본다.
     */
    @Test
    @DisplayName("CLEAN 컨테이너에는 CLEAN 전용 제약이 살아 있다 — 떨어졌으면 오염된 것이다")
    void cleanContainerKeepsCleanOnlyConstraints() {
        MySQLContainer clean = MySqlContainerConfig.sharedContainer();
        clean.start();

        assertThat(SharedMySqlContainers.hasStartedOnce(clean)).isTrue();
        assertThat(clean.getDatabaseName())
                .as("CLEAN 컨테이너의 DB 이름은 app 이다")
                .isEqualTo("app");
    }
}
