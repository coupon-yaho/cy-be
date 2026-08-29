package com.kafkick.api.coupon.query;

import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.persistence.QueryHint;

import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.repository.QueryHints;
import org.yaml.snakeyaml.Yaml;

import com.kafkick.storage.db.coupon.repository.CouponRoundJpaRepository;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 로드 권한의 lease 는 <b>최악의 로드 시간보다 길어야 한다.</b> 짧으면 lease 가 먼저 끝나고,
 * 뒤늦게 끝난 로더가 그 사이 다른 인스턴스가 올린 새 값을 덮어쓴다.
 *
 * <p>그 상한의 출처는 두 곳이다 — storage.yml 의 Hikari {@code connection-timeout}(커넥션 대기)
 * 과 정의 질의의 {@code jakarta.persistence.query.timeout}(질의 자체). 셋이 각자의 파일에 흩어져
 * 있어 한쪽만 바뀌면 관계가 조용히 깨진다. <b>예외도 로그도 없고</b>, 덮어쓴 값이 다음 TTL 까지
 * 남는 것으로만 드러난다. 그래서 세 값을 여기서 함께 읽어 대조한다.
 */
class CouponDefinitionL2LeaseBudgetTest {

    @Test
    void leaseOutlastsTheWorstCaseLoad() throws Exception {
        Duration connectionWait = Duration.ofMillis(hikariConnectionTimeoutMillis());
        Duration queryTimeout = Duration.ofMillis(definitionQueryTimeoutMillis());
        Duration redisConnect = redisDuration("connect-timeout");
        Duration redisCommand = redisDuration("timeout");
        // 권한은 질의 전에 얻고 L2 게시가 끝난 뒤에 반납된다. 네 구간이 모두 든다.
        Duration worstCaseLoad = connectionWait.plus(queryTimeout)
                .plus(redisConnect).plus(redisCommand);

        CouponDefinitionL2CacheProperties defaults =
                new CouponDefinitionL2CacheProperties(null, null, null, null, null);

        // max-load-time 은 '로드 한 번의 상한' 이라는 가정에 이름을 붙인 것이다. 그 가정이
        // 다른 모듈의 실제 값과 어긋나면, lock-lease 검증은 통과해도 전제가 틀린 채로 돈다.
        // 부등호가 아니라 등호다. 여유는 max-load-time 이 아니라 lock-lease 쪽에 둔다 —
        // 여기서 "크기만 하면 된다" 로 두면, 계산에서 항 하나가 빠져도 기본값이 넉넉한 동안은
        // 초록이다(실제로 mutation probe 가 그 상태를 잡았다).
        assertThat(defaults.maxLoadTime())
                .as("max-load-time 기본값(%s)이 권한 보유 구간의 합(커넥션 대기 %s + 질의 %s"
                        + " + Redis 연결 %s + Redis 명령 %s)과 다르다",
                        defaults.maxLoadTime(), connectionWait, queryTimeout,
                        redisConnect, redisCommand)
                .isEqualTo(worstCaseLoad);
        // lock-lease > max-load-time 자체는 컴팩트 생성자가 모든 설정에 대해 강제한다.
        assertThat(defaults.lockLease()).isGreaterThan(defaults.maxLoadTime());
    }

    private static long definitionQueryTimeoutMillis() throws Exception {
        QueryHints hints = CouponRoundJpaRepository.class
                .getMethod("findV2CouponDefinitions").getAnnotation(QueryHints.class);
        QueryHint hint = hints.value()[0];
        assertThat(hint.name())
                .as("힌트 이름이 바뀌면 단위도 바뀐다 — 초 단위 힌트는 이 계산을 무의미하게 만든다")
                .isEqualTo("jakarta.persistence.query.timeout");
        return Long.parseLong(hint.value());
    }

    /**
     * redis.yml 의 값은 {@code ${REDIS_COMMAND_TIMEOUT:500ms}} 꼴이다. 배포가 환경변수를 안 주면
     * 실제로 쓰이는 것은 그 <b>기본값</b>이므로 거기서 뽑는다.
     */
    @SuppressWarnings("unchecked")
    private static Duration redisDuration(String key) throws Exception {
        try (InputStream stream = CouponDefinitionL2LeaseBudgetTest.class
                .getResourceAsStream("/redis.yml")) {
            assertThat(stream).as("redis.yml 이 테스트 클래스패스에 없다").isNotNull();
            Map<String, Object> root = new Yaml().load(stream);
            Object raw = ((Map<String, Object>) nested(root, "spring", "data", "redis")).get(key);
            assertThat(raw).as("redis.yml 에서 %s 를 찾지 못했다", key).isNotNull();
            Matcher placeholder = Pattern.compile("\\$\\{[^:}]+:([^}]*)}").matcher(raw.toString());
            String value = placeholder.matches() ? placeholder.group(1) : raw.toString();
            Matcher amount = Pattern.compile("^(\\d+)(ms|s)$").matcher(value.trim());
            assertThat(amount.matches())
                    .as("redis.yml 의 %s 값 '%s' 을 기간으로 못 읽었다", key, value).isTrue();
            long number = Long.parseLong(amount.group(1));
            return "s".equals(amount.group(2)) ? Duration.ofSeconds(number) : Duration.ofMillis(number);
        }
    }

    @SuppressWarnings("unchecked")
    private static long hikariConnectionTimeoutMillis() throws Exception {
        try (InputStream stream = CouponDefinitionL2LeaseBudgetTest.class
                .getResourceAsStream("/storage.yml")) {
            assertThat(stream).as("storage.yml 이 테스트 클래스패스에 없다").isNotNull();
            Map<String, Object> root = new Yaml().load(stream);
            Map<String, Object> hikari = (Map<String, Object>) nested(
                    root, "spring", "datasource", "hikari");
            return ((Number) hikari.get("connection-timeout")).longValue();
        }
    }

    @SuppressWarnings("unchecked")
    private static Object nested(Map<String, Object> root, String... path) {
        Object current = root;
        for (String key : path) {
            current = ((Map<String, Object>) current).get(key);
            assertThat(current).as("storage.yml 에서 %s 를 찾지 못했다", key).isNotNull();
        }
        return current;
    }
}
