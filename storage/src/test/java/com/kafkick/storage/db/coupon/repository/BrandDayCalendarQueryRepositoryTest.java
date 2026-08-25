package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.time.LocalTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.storage.db.RepositoryTest;

import static org.assertj.core.api.Assertions.assertThat;

@RepositoryTest
@Import(BrandDayCalendarQueryAdapter.class)
class BrandDayCalendarQueryRepositoryTest {

    @Autowired
    private BrandDayCalendarQueryAdapter queryAdapter;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        jdbcTemplate.update(
                "INSERT INTO grades (code, bit_value) VALUES ('GOLD', 4)"
        );
        jdbcTemplate.update(
                "INSERT INTO brands (id, name, category) VALUES (2, '브랜드', '카페')"
        );
        jdbcTemplate.update(
                """
                INSERT INTO coupon_templates (
                    id, brand_id, name, policy_type, discount_amount,
                    valid_days, nth_week, day_of_week, start_time,
                    duration_hours, stock_per_occurrence,
                    eligible_grades_mask, active
                ) VALUES (1, 2, '조회 템플릿', 'FIXED_AMOUNT', 5000,
                          7, 3, 'SAT', ?, 2, 100, 4, true)
                """,
                LocalTime.of(10, 0)
        );
        insertRound(10L, "2026-08-10 10:00:00");
        insertRound(11L, "2026-09-10 10:00:00");
    }

    @Test
    @DisplayName("시작 시각이 조회 기간에 포함된 회차만 순서대로 조회한다")
    void findCalendarRoundsWithinRange() {
        List<CouponRoundDetail> result = queryAdapter.findBetween(
                Instant.parse("2026-08-01T00:00:00Z"),
                Instant.parse("2026-09-01T00:00:00Z")
        );

        assertThat(result).extracting(CouponRoundDetail::couponRoundId)
                .containsExactly(10L);
        assertThat(result.getFirst().totalQuantity()).isEqualTo(100);
        assertThat(result.getFirst().remainingQuantity()).isEqualTo(80);
    }

    private void insertRound(Long id, String openAt) {
        jdbcTemplate.update(
                """
                INSERT INTO coupons (
                    id, template_id, brand_id, name, policy_type,
                    discount_amount, valid_days, eligible_grades_mask,
                    open_at, close_at, status, generated_at, created_at
                ) VALUES (?, 1, 2, '정액 할인', 'FIXED_AMOUNT',
                          5000, 7, 4, ?, DATE_ADD(?, INTERVAL 2 HOUR),
                          'SCHEDULED', '2026-08-01 00:00:00',
                          '2026-08-01 00:00:01')
                """,
                id,
                openAt,
                openAt
        );
        jdbcTemplate.update(
                """
                INSERT INTO coupon_stocks (
                    coupon_id, total_quantity, active_count, updated_at
                ) VALUES (?, 100, 20, '2026-08-01 00:00:00')
                """,
                id
        );
    }
}
