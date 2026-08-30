package com.kafkick.storage.db.observation;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.kafkick.core.observation.ClosedCouponRound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class JdbcClosedCouponRoundRecoverySourceTest {

    private static final Instant FROM =
            Instant.parse("2026-08-25T05:04:00Z");
    private static final Instant TO =
            Instant.parse("2026-08-26T05:04:00Z");

    @Test
    @DisplayName("최근 CLOSED만 최신순으로 제한하는 관측 SQL을 실행한다")
    @SuppressWarnings("unchecked")
    void queryRecentClosedCouponRoundsNewestFirst() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(
                anyString(),
                any(RowMapper.class),
                any(),
                any(),
                anyInt()
        )).thenReturn(List.of());
        JdbcClosedCouponRoundRecoverySource source =
                new JdbcClosedCouponRoundRecoverySource(jdbc);

        source.findRecentlyClosed(FROM, TO, 1_000);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbc).query(
                sql.capture(),
                any(RowMapper.class),
                org.mockito.ArgumentMatchers.eq(Timestamp.from(FROM)),
                org.mockito.ArgumentMatchers.eq(Timestamp.from(TO)),
                org.mockito.ArgumentMatchers.eq(1_000)
        );
        assertThat(sql.getValue().replaceAll("\\s+", " ").trim())
                .contains("WHERE status = 'CLOSED'")
                .contains("close_at >= ?")
                .contains("close_at <= ?")
                .contains("ORDER BY close_at DESC, id DESC")
                .endsWith("LIMIT ?");
    }

    @Test
    @DisplayName("ID와 close_at을 기동 보정 값으로 매핑한다")
    @SuppressWarnings("unchecked")
    void mapCouponRoundIdAndClosedAt() throws Exception {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getLong("id")).thenReturn(201L);
        when(resultSet.getTimestamp("close_at"))
                .thenReturn(Timestamp.from(TO));
        when(jdbc.query(
                anyString(),
                any(RowMapper.class),
                any(),
                any(),
                anyInt()
        )).thenAnswer(invocation -> {
            RowMapper<ClosedCouponRound> mapper = invocation.getArgument(1);
            return List.of(mapper.mapRow(resultSet, 0));
        });
        JdbcClosedCouponRoundRecoverySource source =
                new JdbcClosedCouponRoundRecoverySource(jdbc);

        assertThat(source.findRecentlyClosed(FROM, TO, 1_000))
                .containsExactly(new ClosedCouponRound(201L, TO));
    }

    @Test
    @DisplayName("0 이하 조회 상한은 DB 호출 전에 거부한다")
    void rejectNonPositiveLimit() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        JdbcClosedCouponRoundRecoverySource source =
                new JdbcClosedCouponRoundRecoverySource(jdbc);

        assertThatThrownBy(() -> source.findRecentlyClosed(FROM, TO, 0))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(jdbc);
    }
}
