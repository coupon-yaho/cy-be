package com.kafkick.storage.db.coupon.repository;

import java.time.LocalTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.storage.db.RepositoryTest;

import static org.assertj.core.api.Assertions.assertThat;

@RepositoryTest
@Import(CouponRoundDetailQueryAdapter.class)
class CouponRoundDetailQueryRepositoryTest {

    @Autowired
    private CouponRoundDetailQueryAdapter queryAdapter;

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
                    id, brand_id, name, policy_type,
                    discount_rate, max_discount_amount, valid_days,
                    nth_week, day_of_week, start_time, duration_hours,
                    stock_per_occurrence, eligible_grades_mask, active
                ) VALUES (1, 2, '조회 템플릿', 'PERCENT_CAPPED',
                          20, 10000, 7, 3, 'SAT', ?, 2, 100, 12, true)
                """,
                LocalTime.of(10, 0)
        );
        jdbcTemplate.update(
                """
                INSERT INTO coupons (
                    id, template_id, brand_id, name, policy_type,
                    discount_rate, max_discount_amount, valid_days,
                    eligible_grades_mask, open_at, close_at, status,
                    generated_at, created_at
                ) VALUES (10, 1, 2, '골드 VIP 20% 할인', 'PERCENT_CAPPED',
                          20, 10000, 7, 12, ?, ?, 'SCHEDULED', ?, ?)
                """,
                "2026-08-25 10:00:00",
                "2026-08-25 12:00:00",
                "2026-08-24 00:00:00",
                "2026-08-24 00:00:01"
        );
        jdbcTemplate.update(
                """
                INSERT INTO coupon_stocks (
                    coupon_id, total_quantity, active_count, updated_at
                ) VALUES (10, 100, 20, ?)
                """,
                "2026-08-24 00:00:00"
        );
    }

    @Test
    @DisplayName("발급 가능 여부와 무관하게 예약된 회차의 상세 정보를 조회한다")
    void findScheduledCouponRoundDetail() {
        CouponRoundDetail result = queryAdapter.findById(10L).orElseThrow();

        assertThat(result.couponRoundId()).isEqualTo(10L);
        assertThat(result.templateId()).isEqualTo(1L);
        assertThat(result.brandId()).isEqualTo(2L);
        assertThat(result.discountRate()).isEqualTo(20);
        assertThat(result.maxDiscountAmount()).isEqualTo(10_000);
        assertThat(result.discountAmount()).isNull();
        assertThat(result.eligibleGrades())
                .containsExactly(MembershipGrade.GOLD, MembershipGrade.VIP);
        assertThat(result.status()).isEqualTo(CouponRoundStatus.SCHEDULED);
        assertThat(result.totalQuantity()).isEqualTo(100);
        assertThat(result.remainingQuantity()).isEqualTo(80);
    }
}
