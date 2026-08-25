package com.kafkick.infra.mq.attempt;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * 이 티켓의 인수 조건 — <b>발급 경로에서 Redis 호출이 0회다.</b>
 *
 * <p>v1 의 전제가 "발급 경로에 Redis 미사용" 인데, 관측이 그 전제를 깨면 측정 대상이 측정 도구
 * 때문에 달라진다. 그래서 핫패스는 Kafka 를 한 번 태우고 끝이고, XADD 는 그 뒤 컨슈머
 * 스레드에서 일어난다.
 *
 * <h2>왜 실행 시점 관찰이 아니라 구조를 보는가</h2>
 *
 * "부하를 걸고 Redis 호출 수를 센다" 는 검증은 <b>그 회차에 그 코드가 안 불렸다</b>는 것만
 * 말한다. 조건 분기 하나 뒤에 숨은 호출은 그 회차에 안 걸리고, 다음 회차에 걸린다.
 * 의존 그래프는 그것과 달리 <b>가능성 자체</b>를 없앤다 — {@code infra:mq} 가 Redis 타입을
 * 컴파일 타임에 볼 수 없으면 발급 경로에 XADD 를 쓰는 코드는 애초에 컴파일되지 않는다.
 *
 * <p>그 성질을 유지하려고 {@code AttemptLiveSink} 를 core 에 뒀다. 이 테스트는 그 결정이
 * 되돌려지는 것을 막는다 — {@code infra/mq/build.gradle} 에 {@code project(':infra:redis')}
 * 한 줄을 더하면 컴파일도 테스트도 전부 초록인 채로 전제가 사라진다.
 */
class IssuePathRedisFreeTest {

    /** 발급 경로가 지나는 클래스. 컨슈머는 이 목록에 없다 — 그쪽은 Redis 를 써도 된다. */
    private static final List<String> ISSUE_PATH = List.of(
            "src/main/java/com/kafkick/infra/mq/attempt/AttemptEventPublisher.java",
            "src/main/java/com/kafkick/infra/mq/attempt/AttemptFailureCounter.java",
            "src/main/java/com/kafkick/infra/mq/config/AttemptProducerConfig.java",
            "src/main/java/com/kafkick/infra/mq/config/KafkaProducerSupport.java",
            "src/main/java/com/kafkick/infra/mq/config/PartitionKeys.java");

    private static final List<String> REDIS_MARKERS = List.of(
            "org.springframework.data.redis", "RedisTemplate", "opsForStream", "XADD",
            "io.lettuce", "redis.clients");

    @Test
    void theIssuePathSourcesNameNothingRedis() throws IOException {
        for (String relative : ISSUE_PATH) {
            Path source = Path.of(relative);
            assertThat(source).as("발급 경로 목록이 실제 파일과 어긋났다").exists();
            String text = Files.readString(source, StandardCharsets.UTF_8);
            for (String marker : REDIS_MARKERS) {
                assertThat(text).as("%s 가 Redis 를 부른다: %s", relative, marker)
                        .doesNotContain(marker);
            }
        }
    }

    /**
     * 모듈 경계 자체를 고정한다. <b>이쪽이 진짜 방어선이다</b> — 위 테스트는 파일 목록에 적힌
     * 것만 보므로, 발급 경로에 새 클래스가 생기면 그 목록 밖이라 안 걸린다.
     *
     * <p>build.gradle 문자열을 보는 검사는 주석 한 줄로 뚫린다는 것을 이 저장소가 이미 겪었다
     * (루트 build.gradle 의 {@code cy.runtimeProjects} 주석). 그래서 선언 문자열이 아니라
     * <b>Gradle 이 해석한 컴파일 클래스패스</b>에 Redis 클래스가 실제로 있는지를 본다.
     */
    @Test
    void theModuleCannotSeeRedisTypesAtCompileTime() {
        assertThat(classIsOnTheCompileClasspath("org.springframework.data.redis.core.StringRedisTemplate"))
                .as("infra:mq 가 infra:redis 나 spring-data-redis 를 물면 전제가 그래프에서 사라진다")
                .isFalse();
        assertThat(classIsOnTheCompileClasspath("com.kafkick.infra.redis.stream.AttemptLiveStream"))
                .as("컨슈머는 core 의 포트만 봐야 한다")
                .isFalse();
        // 술어가 항상 false 를 내는 상태(오타 · 로더 교체)면 위 둘이 아무것도 증명하지 못한다.
        assertThat(classIsOnTheCompileClasspath("com.kafkick.core.observation.attempt.AttemptLiveSink"))
                .as("이 검사 방법 자체가 동작하는지")
                .isTrue();
    }

    private static boolean classIsOnTheCompileClasspath(String className) {
        try {
            Class.forName(className, false, IssuePathRedisFreeTest.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | NoClassDefFoundError absent) {
            return false;
        }
    }

    /** 목록이 비면 위 테스트가 아무것도 안 보고 통과한다. */
    @Test
    void theIssuePathListIsNotEmpty() {
        assertThat(ISSUE_PATH).isNotEmpty();
        assertThat(Stream.of(REDIS_MARKERS).count()).isPositive();
    }
}
