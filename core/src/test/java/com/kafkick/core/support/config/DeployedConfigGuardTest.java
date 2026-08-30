// 마운트가 비었을 때 기동을 거절하는지 확인합니다.
package com.kafkick.core.support.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.Ordered;
import org.springframework.mock.env.MockEnvironment;

/**
 * <b>이 가드가 막는 것은 "설정이 틀렸다" 가 아니라 "왜 죽었는지 모른다" 다.</b>
 *
 * <p>설정이 안 실리면 앱은 결국 죽는다 — 다만 {@code observation.datasource.url must not be
 * blank} 처럼 <b>원인과 먼 자리에서</b> 죽는다. 읽는 사람은 관측 설정을 의심하며 시간을 쓴다.
 * 여기서 지키는 것은 <b>죽는다</b> 가 아니라 <b>원인을 이름으로 말한다</b> 이다.
 */
class DeployedConfigGuardTest {

    private final DeployedConfigGuard guard = new DeployedConfigGuard();

    private void run(Map<String, String> properties) {
        MockEnvironment environment = new MockEnvironment();
        properties.forEach(environment::setProperty);
        guard.postProcessEnvironment(environment, null);
    }

    @Test
    @DisplayName("켠 상태에서 마커가 비면 기동을 거절한다 — 처방까지 메시지에 싣는다")
    void rejectsWhenMarkerMissingAndRequired() {
        assertThatThrownBy(() -> run(Map.of(DeployedConfigGuard.REQUIRED, "true")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("배포 설정이 안 실렸습니다")
                // 처방이 없으면 이 가드는 원인만 바꿔 놓고 사람을 그대로 막아 세운다.
                .hasMessageContaining("cp application.yml.example application.yml")
                .hasMessageContaining("DEPLOYED_CONFIG_REQUIRED=false");
    }

    /**
     * <b>공백만 있는 값도 없는 것이다.</b> YAML 에 {@code marker:} 만 적고 값을 비우면 빈
     * 문자열이 오는데, 그것을 "있음" 으로 세면 <b>키만 있고 내용이 없는 파일</b>이 통과한다.
     */
    @Test
    @DisplayName("공백만 있는 마커도 없는 것으로 본다")
    void treatsBlankMarkerAsMissing() {
        assertThatThrownBy(() -> run(Map.of(
                DeployedConfigGuard.REQUIRED, "true",
                DeployedConfigGuard.MARKER, "   ")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("마커가 있으면 통과한다")
    void passesWhenMarkerPresent() {
        assertThatCode(() -> run(Map.of(
                DeployedConfigGuard.REQUIRED, "true",
                DeployedConfigGuard.MARKER, "root-application-yml")))
                .doesNotThrowAnyException();
    }

    /**
     * <b>기본이 꺼짐인 것이 이 가드의 전제다.</b> 배포 경로가 둘이고 설정을 마운트하는 것은
     * 한쪽뿐이라({@code base.yml + batch.yml} 의 batch 는 마운트가 없다 — 실측), 기본을
     * 켬으로 두면 그 경로와 모든 테스트가 죽는다.
     */
    @Test
    @DisplayName("안 켠 경로에서는 마커가 없어도 그냥 지나간다")
    void staysQuietWhenNotRequired() {
        assertThatCode(() -> run(Map.of())).doesNotThrowAnyException();
    }

    /**
     * <b>순서가 틀리면 정상인 배포를 거절한다.</b> 설정 파일을 읽는
     * {@code ConfigDataEnvironmentPostProcessor} 보다 앞서 돌면 마커가 아직 안 실려 있다.
     */
    @Test
    @DisplayName("설정을 읽은 뒤에 돈다 — 앞서 돌면 정상인 배포도 거절한다")
    void runsAfterConfigDataIsLoaded() {
        assertThatCode(() -> {
            if (guard.getOrder() != Ordered.LOWEST_PRECEDENCE) {
                throw new AssertionError("순서가 바뀌었다: " + guard.getOrder());
            }
        }).doesNotThrowAnyException();
    }
}
