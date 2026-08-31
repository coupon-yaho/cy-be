package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.task.TaskSchedulingAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * 커밋된 템플릿의 <b>문자열</b>이 아니라 그 값이 실제로 프레임워크에 <b>먹는지</b>를 본다.
 *
 * <p>문자열만 비교하면 단위를 틀린 설정이 통과한다 — {@code timeout: 800} 은 800밀리초가 아니라
 * 800초로 읽힐 수도 있는 표기이고, 그런 오타는 Redis 가 실제로 멎기 전까지 아무 신호도 없다.
 *
 * <p>실측으로 확인한 기본값(이 테스트가 지키려는 것) — Lettuce 커맨드 타임아웃 {@code PT1M},
 * 접속 대상 {@code localhost:6379}, 스케줄러 풀 {@code 1}.
 */
class DomainGaugeRuntimeConfigTest {

    @Test
    @DisplayName("Redis 타임아웃이 실제 커넥션 팩토리에 800ms 로 먹는다 — 기본값 60초를 대체한다")
    void redisTimeoutsReachTheConnectionFactory() {
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DataRedisAutoConfiguration.class))
            .withPropertyValues(committed("spring.data.redis."))
            .run(context -> {
                LettuceConnectionFactory factory = context.getBean(LettuceConnectionFactory.class);
                assertThat(factory.getClientConfiguration().getCommandTimeout())
                    .as("5초 주기 수집이 60초를 잡고 있으면 UNAVAILABLE 전환도 안 걸린다")
                    .isEqualTo(Duration.ofMillis(800))
                    .isLessThan(Duration.ofSeconds(1));
                assertThat(factory.getPort()).isEqualTo(6379);
            });
    }

    @Test
    @DisplayName("스케줄러 풀이 2 이상이라 느린 집계가 1초 주기 재고 갱신을 막지 않는다")
    void schedulerPoolIsNotSingleThreaded() {
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TaskSchedulingAutoConfiguration.class))
            .withUserConfiguration(SchedulingOn.class)
            .withPropertyValues(committed("spring.task.scheduling."))
            .run(context -> {
                ThreadPoolTaskScheduler scheduler = context.getBean(ThreadPoolTaskScheduler.class);
                assertThat(scheduler.getScheduledThreadPoolExecutor().getCorePoolSize())
                    .as("기본값 1이면 재고·정합성·기존 배치 스케줄러가 한 스레드를 나눠 쓴다")
                    .isGreaterThanOrEqualTo(2);
            });
    }

    /** 커밋된 템플릿에서 접두사가 같은 키만 골라 {@code key=value} 형태로 돌려준다. */
    private static String[] committed(String prefix) {
        YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
        factory.setResources(new ClassPathResource("application.yml.example"));
        Properties template = factory.getObject();
        return template.stringPropertyNames().stream()
            .filter(name -> name.startsWith(prefix))
            .map(name -> name + "=" + template.getProperty(name))
            .toArray(String[]::new);
    }

    @EnableScheduling
    static class SchedulingOn {
    }
}
