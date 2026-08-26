// 컨테이너 소유권 계약을 기계로 지킵니다. CY-621 이 세운 성질입니다.
package com.kafkick.storage.db;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;

/**
 * <b>CY-621 이 세운 두 성질을 잰다.</b> 둘 다 이 티켓의 존재 이유(컨테이너 44회 → 4회)이고,
 * 회귀가 나도 <b>다른 어떤 테스트도 원인을 안 짚어 준다.</b>
 *
 * <p><b>스프링 컨텍스트를 안 띄운다.</b> 두 설정을 한 컨텍스트에 물면 {@code MySQLContainer}
 * 타입 {@code @ServiceConnection} 빈이 둘이 되고, 그때는 가드가 먼저 죽인다 —
 * 재려는 것은 그것이 아니라 <b>싱글턴의 정체성</b>이라 정적 필드만 본다.
 *
 * <p><b>컨테이너를 새로 안 띄운다.</b> 두 설정이 실제로 쓰는 바로 그 객체를 받아
 * {@code start()} 한다. 다른 테스트가 이미 띄웠으면 멱등이고, 아니면 어차피 곧 필요한 것이라
 * 이 테스트 때문에 늘어나는 컨테이너는 없다.
 */
class SharedMySqlContainerTest {

    /**
     * <b>{@code stop()} 오버라이드가 사라지면 여기서 잡힌다.</b> 그것이 없으면 컨텍스트가
     * 닫힐 때 컨테이너가 죽고, 다음 {@code start()} 가 <b>새 컨테이너를 띄운다</b> —
     * 44회가 39회로만 줄던 그 상태다.
     */
    @Test
    @DisplayName("stop() 을 불러도 안 죽는다 — 수명의 주인은 JVM 이다")
    void survivesStop() {
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
     * <b>CLEAN 과 CORRUPT 가 물리적으로 다른 서버여야 한다.</b> 같으면 CORRUPT 마이그레이션이
     * CLEAN 제약을 떨어뜨려, 나중에 도는 CLEAN 테스트가 <i>영문 모를 이유로</i> 깨진다.
     */
    @Test
    @DisplayName("CLEAN 과 CORRUPT 는 다른 컨테이너다 — 같으면 제약이 서로를 지운다")
    void cleanAndCorruptAreSeparateContainers() {
        MySQLContainer clean = MySqlContainerConfig.sharedContainer();
        MySQLContainer corrupt = CorruptMySqlContainerConfig.sharedContainer();
        clean.start();
        corrupt.start();

        assertThat(corrupt.getContainerId())
                .as("스키마 종류별로 갈려야 한다. 같은 컨테이너면 db/corrupt 가 CLEAN 제약을 지운다")
                .isNotEqualTo(clean.getContainerId());
        assertThat(corrupt.getMappedPort(3306))
                .isNotEqualTo(clean.getMappedPort(3306));
    }
}
