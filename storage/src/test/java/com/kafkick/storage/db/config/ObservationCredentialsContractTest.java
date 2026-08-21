package com.kafkick.storage.db.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;

/**
 * 관측을 켠 채 계정을 안 넣으면 <b>기동에서 죽는 것이 의도</b>다.
 *
 * <p>관제는 발급 이벤트 시간대에 늘 켜져 있어야 한다. 계정이 없을 때 조용히 관측만 꺼지면
 * 화면이 빈 채로 배포가 성공하고, 발급이 시작된 뒤에야 알게 된다. 늦게 아는 것보다 배포 시점에
 * 이름을 지목하며 죽는 편이 낫다.
 *
 * <p>유휴 시간에 관측을 끄는 것은 이 실패로 하지 않는다 —
 * {@code observation.datasource.enabled=false} 라는 별도 스위치가 그 자리다.
 */
class ObservationCredentialsContractTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(ValidationAutoConfiguration.class))
        .withUserConfiguration(ObservationDataSourceConfig.class)
        .withPropertyValues(
            "observation.datasource.enabled=true",
            "spring.datasource.url=jdbc:mysql://localhost:3306/app",
            "spring.datasource.username=app",
            "observation.datasource.url=jdbc:mysql://localhost:3306/app");

    @Test
    @DisplayName("관측을 켜고 계정을 비우면 기동이 실패하고, 어느 프로퍼티가 비었는지 지목한다")
    void blankCredentialsFailFastAndNameTheProperty() {
        runner.withPropertyValues(
                "observation.datasource.username=",
                "observation.datasource.password=")
            .run(context -> {
                assertThat(context).hasFailed();
                assertThat(rootMessage(context.getStartupFailure()))
                    .as("무엇을 넣어야 하는지 모르는 실패는 조용한 실패만큼 나쁘다")
                    .contains("observation.datasource.username")
                    .contains("observation.datasource.password");
            });
    }

    @Test
    @DisplayName("계정을 넣으면 뜬다")
    void credentialsMakeItStart() {
        runner.withPropertyValues(
                "observation.datasource.username=obs",
                "observation.datasource.password=secret")
            .run(context -> assertThat(context).hasNotFailed());
    }

    private static String rootMessage(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return String.valueOf(root.getMessage());
    }
}
