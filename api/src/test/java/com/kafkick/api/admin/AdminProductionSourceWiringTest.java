package com.kafkick.api.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.kafkick.api.admin.issuance.AdminIssuanceHistoryConfig;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryReader;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryService;
import com.kafkick.core.admin.issuancehistory.IssuanceCodeMasker;
import com.kafkick.core.admin.issuancehistory.IssuanceHistoryCalculator;
import com.kafkick.core.support.TimeProvider;

/** 관리자 생산 배선에 Fixture와 분석 Mock이 다시 들어오지 않는지 검증합니다. */
class AdminProductionSourceWiringTest {

    private final ApplicationContextRunner issuanceHistoryRunner = new ApplicationContextRunner()
            .withUserConfiguration(AdminIssuanceHistoryConfig.class)
            .withBean(TimeProvider.class, () -> new TimeProvider(
                    Clock.fixed(Instant.parse("2026-08-26T00:00:00Z"), ZoneOffset.UTC)));

    /** 공통 Fixture 설정과 Mock 타입·스위치는 생산 소스에 존재하면 안 됩니다. */
    @Test
    void excludesFixtureConfigurationAndMockWiringFromProductionSources() throws IOException {
        Path productionRoot = Path.of("src/main/java");
        Path repositoryRoot = Path.of("..").toAbsolutePath().normalize();
        List<Path> productionSources = filesUnder(productionRoot);

        assertThat(productionRoot.resolve("com/kafkick/api/admin/support/config/AdminFixtureConfig.java"))
                .doesNotExist();
        assertThat(repositoryRoot.resolve(
                "core/src/main/java/com/kafkick/core/admin/analytics/mock/AdminAnalyticsMockDataFactory.java"))
                .doesNotExist();
        assertThat(repositoryRoot.resolve(
                "core/src/main/java/com/kafkick/core/admin/analytics/mock/AdminAnalyticsMockSource.java"))
                .doesNotExist();
        assertThat(productionSources)
                .allSatisfy(source -> assertThat(Files.readString(source))
                        .doesNotContain(
                                "admin.mock.enabled",
                                "ADMIN_MOCK_ENABLED",
                                "admin.analytics.mock-enabled",
                                "ADMIN_ANALYTICS_MOCK_ENABLED",
                                "AdminAnalyticsMock"));
    }

    /** 배포 예시는 공통·분석 Mock 설정 없이 실제 Source와 Pending만 안내합니다. */
    @Test
    void excludesMockSettingsFromDeploymentExamples() throws IOException {
        Path repositoryRoot = Path.of("..").toAbsolutePath().normalize();
        String environmentExample = Files.readString(repositoryRoot.resolve(".env.example"));
        String deploymentExample = Files.readString(repositoryRoot.resolve("application.yml.example"));
        String ideExample = Files.readString(Path.of("src/main/resources/application.yml.example"));

        assertThat(environmentExample)
                .doesNotContain("ADMIN_MOCK_ENABLED", "ADMIN_ANALYTICS_MOCK_ENABLED");
        assertThat(deploymentExample)
                .doesNotContain(
                        "admin.mock.enabled",
                        "ADMIN_MOCK_ENABLED",
                        "mock-enabled",
                        "ADMIN_ANALYTICS_MOCK_ENABLED");
        assertThat(ideExample)
                .doesNotContain(
                        "admin.mock.enabled",
                        "ADMIN_MOCK_ENABLED",
                        "mock-enabled",
                        "ADMIN_ANALYTICS_MOCK_ENABLED");
    }

    /** 사용자 정의 이력 Service가 있으면 API 기본 조립이 중복 Bean으로 덮어쓰지 않습니다. */
    @Test
    void backsOffWhenApplicationProvidesIssuanceHistoryService() {
        AdminIssuanceHistoryReader reader = (query, snapshotAt) -> {
            throw new AssertionError("이 Context 계약에서는 Reader를 호출하지 않습니다.");
        };
        AdminIssuanceHistoryService customService = new AdminIssuanceHistoryService(
                new TimeProvider(Clock.fixed(Instant.parse("2026-08-26T00:00:00Z"), ZoneOffset.UTC)),
                reader,
                new IssuanceHistoryCalculator(new IssuanceCodeMasker()));

        issuanceHistoryRunner
                .withBean(AdminIssuanceHistoryReader.class, () -> reader)
                .withBean(AdminIssuanceHistoryService.class, () -> customService)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(AdminIssuanceHistoryService.class);
                    assertThat(context.getBean(AdminIssuanceHistoryService.class)).isSameAs(customService);
                });
    }

    /** 생산 소스 트리를 재귀적으로 읽되 test source Fixture는 검사 대상에서 제외합니다. */
    private static List<Path> filesUnder(Path root) throws IOException {
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(Files::isRegularFile).toList();
        }
    }
}
