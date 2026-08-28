// JVM 기본 존 가드를 컨테이너 없이 잽니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.time.ZoneId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>스위치로 껐다는 것이 "검사를 안 한다" 가 되면 안 된다.</b> 테스트 JVM 은 일부러
 * {@code Asia/Seoul} 이라 테스트 설정이 이 가드를 꺼 두는데, 그러면 스프링 컨텍스트를 띄우는
 * 어떤 테스트도 이 가드를 안 지난다. 그래서 여기서 <b>생성자를 직접</b> 부른다.
 *
 * <p>판정 함수는 따로 잰다 — 생성자는 <b>실제 JVM 존</b>에 매여 있어 CI 와 로컬이 다르면
 * 결과가 갈린다. {@link DefaultZoneGuard#isUtc} 는 존을 인자로 받아 그 결합이 없다.
 */
class DefaultZoneGuardTest {

    @Test
    @DisplayName("이름이 달라도 오프셋이 0 인 고정 존이면 통과한다")
    void acceptsEveryFixedZeroOffsetZone() {
        assertThat(DefaultZoneGuard.isUtc(ZoneId.of("UTC")))
                .as("이름을 비교하면 멀쩡한 배포가 거절된다")
                .isTrue();
        assertThat(DefaultZoneGuard.isUtc(ZoneId.of("Etc/UTC"))).isTrue();
        assertThat(DefaultZoneGuard.isUtc(ZoneId.of("Z"))).isTrue();
        assertThat(DefaultZoneGuard.isUtc(ZoneId.of("GMT"))).isTrue();
    }

    @Test
    @DisplayName("KST 는 거절한다 — 배치 메타가 아홉 시간 어긋난다")
    void rejectsSeoul() {
        assertThat(DefaultZoneGuard.isUtc(ZoneId.of("Asia/Seoul"))).isFalse();
    }

    /**
     * <b>지금 0 이라고 통과시키면 안 된다.</b> {@code Europe/London} 은 겨울에 오프셋이 0
     * 이지만 여름에 +1 이 된다 — 서머타임이 시작하는 날 <b>조용히</b> 어긋난다.
     */
    @Test
    @DisplayName("전이가 있는 존은 지금 0 이어도 거절한다")
    void rejectsAZoneThatDrifts() {
        assertThat(DefaultZoneGuard.isUtc(ZoneId.of("Europe/London")))
                .as("서머타임이 시작하는 날 조용히 어긋난다")
                .isFalse();
    }

    /**
     * 생성자 갈래는 실제 JVM 존에 매여 있다. 테스트 JVM 이 {@code Asia/Seoul} 인 것을
     * 전제로 <b>거절과 탈출구</b>를 잰다 — 그 전제 자체를 먼저 단언해 CI 가 바뀌면
     * 이 테스트가 조용히 무의미해지지 않게 한다.
     */
    @Test
    @DisplayName("이 JVM 은 UTC 가 아니다 — 아래 두 단언의 전제다")
    void thisJvmIsNotUtc() {
        assertThat(DefaultZoneGuard.isUtc(ZoneId.systemDefault()))
                .as("batch/build.gradle 이 테스트 JVM 을 Asia/Seoul 로 고정한다(CY-392)")
                .isFalse();
    }

    /**
     * <b>전제가 안 서면 실패가 아니라 건너뛴다.</b> 전제를 <i>지키는</i> 것은
     * {@link #thisJvmIsNotUtc} 하나가 지고, 그것 하나만 크게 운다 — UTC JVM 에서 돌리면
     * <i>"내 JVM 이 CI 와 다르다"</i> 는 신호 하나만 뜨고, 그 전제에 얹힌 이 단언까지
     * 같이 붉어지지는 않는다.
     */
    @Test
    @DisplayName("켜져 있으면 거절하고, 어떻게 맞추는지와 끄는 법을 말한다")
    void rejectsAndTellsHowToFix() {
        assumeThat(DefaultZoneGuard.isUtc(ZoneId.systemDefault()))
                .as("UTC JVM 에서는 거절 자체가 안 일어난다")
                .isFalse();

        assertThatThrownBy(() -> new DefaultZoneGuard(true, new io.micrometer.core.instrument.simple.SimpleMeterRegistry()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Timestamp.valueOf")
                .as("맞추는 법을 안 말하면 사람이 어디를 고칠지 모른다")
                .hasMessageContaining("user.timezone")
                .as("끄는 법은 환경변수 이름으로 말해야 compose 에서 찾는다")
                .hasMessageContaining("TIMEZONE_GUARD_REQUIRED=false");
    }

    @Test
    @DisplayName("끄면 뜬다")
    void startsWhenEnforcementIsOff() {
        assertThatCode(() -> new DefaultZoneGuard(false, new io.micrometer.core.instrument.simple.SimpleMeterRegistry())).doesNotThrowAnyException();
    }

    /**
     * <b>스위치로 껐다는 것이 "배선도 안 잰다" 가 되면 안 된다.</b> 테스트 설정이
     * {@code batch.timezone-guard.required=false} 로 두므로 컨텍스트를 띄우는 어떤 테스트도
     * 이 가드의 거절을 안 지난다 — 그래서 {@code @Component} 를 떼거나 스캔 밖으로 옮겨도
     * <b>전 저장소가 초록</b>이다. {@code SchedulerPoolGuardTest} 가 같은 실수를 금지해 뒀고,
     * 그 선례대로 <b>가드 자신의 테스트</b>에 둔다.
     */
    @org.springframework.boot.test.context.SpringBootTest(properties = {
            "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
            "spring.batch.job.enabled=false",
            "batch.scheduling.enabled=false",
            "server.port=0",
            "management.server.port=0"})
    @org.springframework.context.annotation.Import(com.kafkick.storage.db.MySqlContainerConfig.class)
    @org.junit.jupiter.api.Nested
    class Wiring {

        @org.springframework.beans.factory.annotation.Autowired
        private org.springframework.context.ApplicationContext context;

        @Test
        @DisplayName("가드가 빈으로 등록된다 — 껐다고 배선까지 안 재면 안 된다")
        void guardIsWired() {
            assertThat(context.getBeanNamesForType(DefaultZoneGuard.class))
                    .as("스캔 밖으로 옮기면 KST 배포가 그냥 뜨고 선점문이 다시 어긋난다")
                    .hasSize(1);
        }
    }
}
