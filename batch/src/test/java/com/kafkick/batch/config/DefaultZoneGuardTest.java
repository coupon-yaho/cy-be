// JVM 기본 존 가드를 컨테이너 없이 잽니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    @DisplayName("켜져 있으면 거절하고, 어떻게 맞추는지와 끄는 법을 말한다")
    void rejectsAndTellsHowToFix() {
        assertThatThrownBy(() -> new DefaultZoneGuard(true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RunningJobProbe")
                .as("맞추는 법을 안 말하면 사람이 어디를 고칠지 모른다")
                .hasMessageContaining("user.timezone")
                .as("끄는 법은 환경변수 이름으로 말해야 compose 에서 찾는다")
                .hasMessageContaining("TIMEZONE_GUARD_REQUIRED=false");
    }

    @Test
    @DisplayName("끄면 뜬다")
    void startsWhenEnforcementIsOff() {
        assertThatCode(() -> new DefaultZoneGuard(false)).doesNotThrowAnyException();
    }
}
