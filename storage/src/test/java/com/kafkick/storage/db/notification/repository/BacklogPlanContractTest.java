package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
@Import(OutboxMeterTestConfig.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class BacklogPlanContractTest {
    @Autowired JdbcTemplate jdbcTemplate;

    /**
     * <b>백로그 질의가 인덱스 <i>구간</i>만 읽는지 본다.</b>
     *
     * <p>{@code type=index} 는 커버링이라 표는 안 읽지만 <b>인덱스를 끝까지 훑는다</b> —
     * {@code PUBLISHED}·{@code DEAD} 가 쌓일수록 15초마다 도는 이 질의가 비싸진다.
     * {@code type=ref} 여야 그 상태 구간만 읽는다.
     *
     * <p>처음에 {@code IN ('PENDING','IN_PROGRESS')} 하나로 썼다가 {@code type=index} 가
     * 나왔고, {@code Using index} 만 보고 "인덱스만으로 센다" 로 읽었다 —
     * <b>커버링인 것과 구간만 읽는 것은 다르다.</b> 리뷰가 잡았다.
     */
    @Test
    void backlogCountReadsOnlyTheStatusRange() {
        assertThat(planFor("PENDING"))
                .as("type=index 면 인덱스를 끝까지 훑는다 — 누적 행 수에 비례해 비싸진다")
                .startsWith("type=ref");
        assertThat(planFor("IN_PROGRESS")).startsWith("type=ref");
    }

    /**
     * <b>어댑터가 실제로 쓰는 문자열에 {@code EXPLAIN} 을 건다.</b> 테스트가 자기 SQL 을
     * 적어 두면 어댑터를 {@code IN} 하나로 되돌려도 <b>이 테스트는 그대로 통과한다</b> —
     * 처음에 그렇게 써서 돌연변이가 안 잡혔다.
     */
    private String planFor(String status) {
        return jdbcTemplate.query(
                "EXPLAIN " + NotificationOutboxRepositoryImpl.COUNT_BY_STATUS,
                (rs, i) -> "type=" + rs.getString("type") + " key=" + rs.getString("key")
                        + " Extra=" + rs.getString("Extra"),
                status).getFirst();
    }
}
