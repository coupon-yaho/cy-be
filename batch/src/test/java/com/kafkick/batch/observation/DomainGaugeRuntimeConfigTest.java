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

    /**
     * <b>정본 파일에서 읽는다.</b> 이 값을 {@code application.yml.example} 에서 읽으면 거짓
     * 보증이 된다 — 그 문서의 Redis 타임아웃은 {@code classpath:redis.yml} import 에 져서
     * 실행 시 적용되지 않는데, 템플릿만 단독으로 읽는 테스트는 그것을 알아채지 못한다(CY-781).
     */
    @Test
    @DisplayName("Redis 타임아웃이 실제 커넥션 팩토리에 500ms 로 먹는다 — 기본값 60초를 대체한다")
    void redisTimeoutsReachTheConnectionFactory() {
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DataRedisAutoConfiguration.class))
            .withPropertyValues(committed("redis.yml.example", "spring.data.redis."))
            .run(context -> {
                LettuceConnectionFactory factory = context.getBean(LettuceConnectionFactory.class);
                assertThat(factory.getClientConfiguration().getCommandTimeout())
                    .as("5초 주기 수집이 60초를 잡고 있으면 UNAVAILABLE 전환도 안 걸린다")
                    .isEqualTo(Duration.ofMillis(500))
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
            .withPropertyValues(committed("application.yml.example", "spring.task.scheduling."))
            .run(context -> {
                ThreadPoolTaskScheduler scheduler = context.getBean(ThreadPoolTaskScheduler.class);
                assertThat(scheduler.getScheduledThreadPoolExecutor().getCorePoolSize())
                    .as("기본값 1이면 재고·정합성·기존 배치 스케줄러가 한 스레드를 나눠 쓴다")
                    .isGreaterThanOrEqualTo(2);
            });
    }

    /**
     * 커밋된 템플릿에서 접두사가 같은 키만 골라 {@code key=value} 형태로 돌려준다.
     *
     * <p>{@code sentinel.*} 는 뺀다 — {@code redis.yml} 의 그 키들은 프로파일로만 켜지는 별도
     * 문서에 있고 기본값이 없어, 단일 모드 구성에 섞으면 해석되지 않는 자리표시자가 된다.
     */
    private static String[] committed(String resource, String prefix) {
        YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
        factory.setResources(new ClassPathResource(resource));
        Properties template = factory.getObject();
        return template.stringPropertyNames().stream()
            .filter(name -> name.startsWith(prefix))
            .filter(name -> !name.contains(".sentinel."))
            .map(name -> name + "=" + template.getProperty(name))
            .toArray(String[]::new);
    }

    @EnableScheduling
    static class SchedulingOn {
    }
}
