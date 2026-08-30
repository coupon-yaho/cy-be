// 마운트가 비었을 때 기동을 거절하는지 확인합니다.
package com.kafkick.core.support.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.Ordered;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
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

    /**
     * <b>파일에서 온 마커는 통과해야 한다.</b> 위 검사만 있으면 "전부 거절" 하는 구현도
     * 통과한다 — 그 방향이 더 나쁘다(정상인 배포를 막는다).
     *
     * <p>실제 설정 파일을 읽어 얹는다. {@code MockEnvironment.setProperty} 는 Origin 이
     * 없어 이 축을 못 태운다.
     */
    @Test
    @DisplayName("설정 파일에서 온 마커는 통과한다 — 전부 거절하는 구현을 막는다")
    void passesWhenMarkerComesFromAConfigFile() throws Exception {
        Path file = Files.createTempFile("deployed-config", ".yml");
        Files.writeString(file, DeployedConfigGuard.MARKER + ": from-file\n");

        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource(
                "switch", Map.of(DeployedConfigGuard.REQUIRED, "true")));
        environment.getPropertySources().addFirst(
                new YamlPropertySourceLoader()
                        .load("mounted", new FileSystemResource(file)).get(0));
        ConfigurationPropertySources.attach(environment);

        assertThatCode(() -> guard.postProcessEnvironment(environment, null))
                .doesNotThrowAnyException();
        Files.deleteIfExists(file);
    }

    /**
     * <b>값이 있는 것만으로 통과하면 이 가드는 뜻이 없다.</b> 환경변수나 {@code -D} 로 같은
     * 키를 주면 마운트가 비어도 지나가고, 그러면 원거리 설정 오류가 그대로 돌아온다.
     *
     * <p>{@link MockEnvironment#setProperty} 는 <b>이 축을 못 태운다</b> — 값을 일반
     * 프로퍼티 소스에 넣기 때문이다. 그래서 실제 {@code systemEnvironment}·
     * {@code systemProperties} 소스를 직접 얹어 잰다.
     */
    @Test
    @DisplayName("환경변수·JVM 프로퍼티로 준 마커는 통과시키지 않는다 — 값이 아니라 출처를 본다")
    void ignoresMarkerFromOutsideConfigFiles() {
        // **이름을 안 세므로 이름을 늘려도 뜻이 없다.** 가드는 값의 Origin 을 보는데,
        // 아래 넷은 전부 그 출처가 없다 — 이름이 뭐든 통과하면 안 된다.
        // 마지막 둘이 앞선 구현(금지 목록)이 놓치던 자리다: 목록에 없는 이름이라 통과했다.
        for (String sourceName : List.of(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME,
                "defaultProperties",
                "someHandRolledSource")) {

            StandardEnvironment environment = new StandardEnvironment();
            environment.getPropertySources().addFirst(new MapPropertySource(
                    sourceName, Map.of(
                            DeployedConfigGuard.REQUIRED, "true",
                            DeployedConfigGuard.MARKER, "faked")));
            // **파사드를 붙여야 실제 기동과 같아진다.** 부트는 기동 때
            // ConfigurationPropertySources.attach 로 맨 앞에 대리 소스를 얹는데, 그것이
            // 뒤의 모든 소스를 대신 답한다. 안 붙이면 이 테스트가 그 경로를 안 태우고,
            // 파사드를 안 거르는 구현도 초록으로 통과한다 — 실제로 그 상태를 컨테이너에서만
            // 잡았다. 여기서 재현해 둔다.
            ConfigurationPropertySources.attach(environment);

            assertThatThrownBy(() -> guard.postProcessEnvironment(environment, null))
                    .as("%s 에서 온 마커가 통과했다 — 마운트가 비어도 지나간다", sourceName)
                    .isInstanceOf(IllegalStateException.class);

        }
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
