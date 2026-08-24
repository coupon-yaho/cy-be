package com.kafkick.storage.db.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class JdbcBenchmarkTopologyObservationTest {

    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final JdbcBenchmarkTopologyObservation observation =
        new JdbcBenchmarkTopologyObservation(jdbc);

    @Test
    void readsConnectionLimitAndCouponStockThroughTheObservationAdapter() {
        given(jdbc.queryForObject("SELECT @@max_connections", Integer.class)).willReturn(50);
        given(jdbc.queryForList(
            "SELECT total_quantity, active_count FROM coupon_stocks WHERE coupon_id = ?", 10L))
            .willReturn(List.of(Map.of("total_quantity", 10_000, "active_count", 0)));

        assertThat(observation.connectionLimit()).isEqualTo(50);
        assertThat(observation.couponStock(10L)).hasValueSatisfying(stock -> {
            assertThat(stock.totalQuantity()).isEqualTo(10_000);
            assertThat(stock.activeCount()).isZero();
        });
    }

    @Test
    void missingCouponIsEmpty() {
        given(jdbc.queryForList(
            "SELECT total_quantity, active_count FROM coupon_stocks WHERE coupon_id = ?", 404L))
            .willReturn(List.of());

        assertThat(observation.couponStock(404L)).isEmpty();
    }
}
