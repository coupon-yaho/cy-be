package com.kafkick.api.coupon.query;

import java.io.InputStream;
import java.time.Duration;
import java.util.Map;

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
        Duration worstCaseLoad = connectionWait.plus(queryTimeout);

        Duration lease = new CouponDefinitionL2CacheProperties(null, null, null, null).lockLease();

        assertThat(lease)
                .as("lease(%s) 가 최악 로드(%s = 커넥션 대기 %s + 질의 %s) 보다 짧으면"
                        + " 만료된 로더가 남의 새 값을 덮는다",
                        lease, worstCaseLoad, connectionWait, queryTimeout)
                .isGreaterThan(worstCaseLoad);
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
