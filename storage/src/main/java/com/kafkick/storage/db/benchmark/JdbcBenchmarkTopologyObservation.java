package com.kafkick.storage.db.benchmark;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.kafkick.core.benchmark.BenchmarkTopologyObservation;

/** 관측 전용 풀에서 벤치마크 시작 게이트가 필요한 DB 값을 읽는다. */
@Component
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class JdbcBenchmarkTopologyObservation implements BenchmarkTopologyObservation {

    private final JdbcTemplate jdbc;

    public JdbcBenchmarkTopologyObservation(@Qualifier("obs") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Integer connectionLimit() {
        return jdbc.queryForObject("SELECT @@max_connections", Integer.class);
    }

    @Override
    public Optional<CouponStock> couponStock(long couponId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
            "SELECT total_quantity, active_count FROM coupon_stocks WHERE coupon_id = ?", couponId);
        if (rows.isEmpty()) return Optional.empty();
        Map<String, Object> row = rows.get(0);
        return Optional.of(new CouponStock(
            ((Number) row.get("total_quantity")).intValue(),
            ((Number) row.get("active_count")).intValue()));
    }
}
