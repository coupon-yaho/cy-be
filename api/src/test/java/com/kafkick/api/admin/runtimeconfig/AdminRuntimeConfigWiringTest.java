package com.kafkick.api.admin.runtimeconfig;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.ReadOnlyRuntimeConfigStore;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;

/** 관리자 Runtime Config Controller의 Core Port 조립을 검증합니다. */
class AdminRuntimeConfigWiringTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(ControllerConfiguration.class);

    /** 제공한 Core Store로 관리자 Controller가 조립되는지 검증합니다. */
    @Test
    void assemblesAdminControllerWithProvidedRuntimeConfigStore() {
        RuntimeConfigStore customStore = new ReadOnlyRuntimeConfigStore(snapshot());

        runner.withBean(RuntimeConfigStore.class, () -> customStore)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(RuntimeConfigStore.class);
                    assertThat(context).hasSingleBean(AdminRuntimeConfigController.class);
                    assertThat(context.getBean(RuntimeConfigStore.class)).isSameAs(customStore);
                });
    }

    /** Controller가 Redis 구현이 아닌 Core RuntimeConfigStore Port만 생성자·필드에 사용하는지 검증합니다. */
    @Test
    void controllerDependsOnlyOnCoreRuntimeConfigStore() {
        Constructor<?>[] constructors = AdminRuntimeConfigController.class.getDeclaredConstructors();
        Field[] fields = AdminRuntimeConfigController.class.getDeclaredFields();

        assertThat(constructors).hasSize(1);
        assertThat(constructors[0].getParameterTypes()).containsExactly(RuntimeConfigStore.class);
        assertThat(fields).extracting(Field::getType).containsExactly(RuntimeConfigStore.class);
    }

    private static RuntimeConfigSnapshot snapshot() {
        return new RuntimeConfigSnapshot(
                EngineVersion.V3,
                ReleaseStage.V3,
                QueueMode.ADAPTIVE,
                3L,
                Instant.parse("2026-08-26T00:00:00Z"),
                "812934",
                SourceStatus.VALID);
    }

    /** 관리자 Controller가 Core Store만 생성자 주입받도록 최소 컨텍스트에 등록합니다. */
    @Configuration(proxyBeanMethods = false)
    static class ControllerConfiguration {

        /** Runtime Config Store를 관리자 Controller에 직접 주입합니다. */
        @Bean
        AdminRuntimeConfigController adminRuntimeConfigController(RuntimeConfigStore runtimeConfigStore) {
            return new AdminRuntimeConfigController(runtimeConfigStore);
        }
    }
}
