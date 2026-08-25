package com.kafkick.api.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/** 관리자 생산 배선에 공통 Fixture가 다시 들어오지 않는지 검증합니다. */
class AdminProductionSourceWiringTest {

    /** 공통 Fixture 설정과 공통 Mock 스위치는 생산 소스에 존재하면 안 됩니다. */
    @Test
    void excludesCommonFixtureConfigurationAndSwitchFromProductionSources() throws IOException {
        Path productionRoot = Path.of("src/main/java");
        List<Path> productionSources = filesUnder(productionRoot);

        assertThat(productionRoot.resolve("com/kafkick/api/admin/support/config/AdminFixtureConfig.java"))
                .doesNotExist();
        assertThat(productionSources)
                .allSatisfy(source -> assertThat(Files.readString(source))
                        .doesNotContain("admin.mock.enabled", "ADMIN_MOCK_ENABLED"));
    }

    /** 배포 예시는 공통 Mock을 제거하고 분석 Mock만 운영 기본 false로 둡니다. */
    @Test
    void keepsOnlyAnalyticsMockSettingInDeploymentExamples() throws IOException {
        Path repositoryRoot = Path.of("..").toAbsolutePath().normalize();
        String environmentExample = Files.readString(repositoryRoot.resolve(".env.example"));
        String deploymentExample = Files.readString(repositoryRoot.resolve("application.yml.example"));
        String ideExample = Files.readString(Path.of("src/main/resources/application.yml.example"));

        assertThat(environmentExample)
                .contains("ADMIN_ANALYTICS_MOCK_ENABLED=false")
                .doesNotContain("ADMIN_MOCK_ENABLED");
        assertThat(deploymentExample)
                .contains("mock-enabled: ${ADMIN_ANALYTICS_MOCK_ENABLED:false}")
                .doesNotContain("admin.mock.enabled", "ADMIN_MOCK_ENABLED");
        assertThat(ideExample)
                .contains("개발·데모 전용", "mock-enabled: ${ADMIN_ANALYTICS_MOCK_ENABLED:true}")
                .doesNotContain("admin.mock.enabled", "ADMIN_MOCK_ENABLED");
    }

    /** 생산 소스 트리를 재귀적으로 읽되 test source Fixture는 검사 대상에서 제외합니다. */
    private static List<Path> filesUnder(Path root) throws IOException {
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(Files::isRegularFile).toList();
        }
    }
}
