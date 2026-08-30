package com.kafkick.storage.db.coupon.repository;

import java.time.LocalTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.query.PublicCouponRoundPage;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.storage.db.RepositoryTest;

import static org.assertj.core.api.Assertions.assertThat;

@RepositoryTest
@Import(PublicCouponRoundQueryAdapter.class)
class PublicCouponRoundQueryRepositoryTest {

    @Autowired
    private PublicCouponRoundQueryAdapter queryAdapter;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        insertReferenceData();
        insertRound(
                10L,
                "마감 회차",
                "CLOSED",
                "2026-08-20 10:00:00",
                "2026-08-20 12:00:00",
                100,
                70
        );
        insertRound(
                11L,
                "진행 회차",
                "OPEN",
                "2026-08-24 10:00:00",
                "2026-08-24 12:00:00",
                80,
                20
        );
        insertRound(
                12L,
                "예약 회차",
                "SCHEDULED",
                "2026-08-25 10:00:00",
                "2026-08-25 12:00:00",
                50,
                0
        );
    }

    @Test
    @DisplayName("회원 조건 없이 모든 상태의 회차를 최근 오픈 순으로 조회한다")
    void findAllPublicCouponRounds() {
        PublicCouponRoundPage result = queryAdapter.findPage(
                null,
                null,
                0,
                20
        );

        assertThat(result.content())
                .extracting(summary -> summary.couponRoundId())
                .containsExactly(12L, 11L, 10L);
        assertThat(result.content().get(0).templateId()).isEqualTo(1L);
        assertThat(result.content().get(0).eligibleGrades())
                .containsExactly(MembershipGrade.GOLD, MembershipGrade.VIP);
        assertThat(result.content().get(0).status())
                .isEqualTo(CouponRoundStatus.SCHEDULED);
        assertThat(result.content().get(0).totalQuantity()).isEqualTo(50);
        assertThat(result.content().get(0).remainingQuantity()).isEqualTo(50);
        assertThat(result.content().get(1).remainingQuantity()).isEqualTo(60);
        assertThat(result.totalElements()).isEqualTo(3);
        assertThat(result.totalPages()).isEqualTo(1);
    }

    @Test
    @DisplayName("공개 회차를 상태별로 필터링한다")
    void filterPublicCouponRoundsByStatus() {
        PublicCouponRoundPage result = queryAdapter.findPage(
                CouponRoundStatus.OPEN,
                null,
                0,
                20
        );

        assertThat(result.content())
                .extracting(summary -> summary.couponRoundId())
                .containsExactly(11L);
        assertThat(result.content().get(0).status())
                .isEqualTo(CouponRoundStatus.OPEN);
        assertThat(result.totalElements()).isEqualTo(1);
    }

    @Test
    @DisplayName("공개 회차를 상태와 참여 가능한 회원 등급으로 함께 필터링한다")
    void filterPublicCouponRoundsByStatusAndEligibleGrade() {
        jdbcTemplate.update(
                "UPDATE coupons SET eligible_grades_mask = 3 WHERE id = 10"
        );

        PublicCouponRoundPage welcomeClosed = queryAdapter.findPage(
                CouponRoundStatus.CLOSED,
                MembershipGrade.WELCOME,
                0,
                20
        );
        PublicCouponRoundPage goldClosed = queryAdapter.findPage(
                CouponRoundStatus.CLOSED,
                MembershipGrade.GOLD,
                0,
                20
        );

        assertThat(welcomeClosed.content())
                .extracting(summary -> summary.couponRoundId())
                .containsExactly(10L);
        assertThat(welcomeClosed.totalElements()).isEqualTo(1);
        assertThat(goldClosed.content()).isEmpty();
        assertThat(goldClosed.totalElements()).isZero();
    }

    @Test
    @DisplayName("오픈 시각과 ID의 고정 순서로 공개 회차를 페이지 조회한다")
    void paginatePublicCouponRoundsInStableOrder() {
        insertSecondTemplate();
        insertRoundWithTemplate(
                13L,
                2L,
                "같은 시각 예약 회차",
                "SCHEDULED",
                "2026-08-25 10:00:00",
                "2026-08-25 12:00:00",
                30,
                0
        );

        PublicCouponRoundPage firstPage = queryAdapter.findPage(
                null,
                null,
                0,
                1
        );
        PublicCouponRoundPage secondPage = queryAdapter.findPage(
                null,
                null,
                1,
                1
        );

        assertThat(firstPage.content())
                .extracting(summary -> summary.couponRoundId())
                .containsExactly(13L);
        assertThat(secondPage.content())
                .extracting(summary -> summary.couponRoundId())
                .containsExactly(12L);
        assertThat(firstPage.totalElements()).isEqualTo(4);
        assertThat(firstPage.totalPages()).isEqualTo(4);
    }

    private void insertReferenceData() {
        jdbcTemplate.update(
                "INSERT INTO grades (code, bit_value) VALUES ('GOLD', 4)"
        );
        jdbcTemplate.update(
                "INSERT INTO brands (id, name, category) VALUES (1, '브랜드', '카페')"
        );
        jdbcTemplate.update(
                """
                INSERT INTO coupon_templates (
                    id, brand_id, name, policy_type,
                    discount_amount, valid_days, nth_week, day_of_week,
                    start_time, duration_hours, stock_per_occurrence,
                    eligible_grades_mask, active
                ) VALUES (1, 1, '공개 조회 템플릿', 'FIXED_AMOUNT',
                          5000, 7, 3, 'SAT', ?, 2, 100, 12, true)
                """,
                LocalTime.of(10, 0)
        );
    }

    private void insertSecondTemplate() {
        jdbcTemplate.update(
                """
                INSERT INTO coupon_templates (
                    id, brand_id, name, policy_type,
                    discount_amount, valid_days, nth_week, day_of_week,
                    start_time, duration_hours, stock_per_occurrence,
                    eligible_grades_mask, active
                ) VALUES (2, 1, '두 번째 공개 조회 템플릿', 'FIXED_AMOUNT',
                          3000, 7, 3, 'SAT', ?, 2, 30, 12, true)
                """,
                LocalTime.of(10, 0)
        );
    }

    private void insertRound(
            Long id,
            String name,
            String status,
            String openAt,
            String closeAt,
            int totalQuantity,
            int activeCount
    ) {
        insertRoundWithTemplate(
                id,
                1L,
                name,
                status,
                openAt,
                closeAt,
                totalQuantity,
                activeCount
        );
    }

    private void insertRoundWithTemplate(
            Long id,
            Long templateId,
            String name,
            String status,
            String openAt,
            String closeAt,
            int totalQuantity,
            int activeCount
    ) {
        jdbcTemplate.update(
                """
                INSERT INTO coupons (
                    id, template_id, brand_id, name, policy_type,
                    discount_amount, valid_days, eligible_grades_mask,
                    open_at, close_at, status, generated_at, created_at
                ) VALUES (?, ?, 1, ?, 'FIXED_AMOUNT',
                          5000, 7, 12, ?, ?, ?, ?, ?)
                """,
                id,
                templateId,
                name,
                openAt,
                closeAt,
                status,
                "2026-08-19 00:00:00",
                "2026-08-19 00:00:01"
        );
        jdbcTemplate.update(
                """
                INSERT INTO coupon_stocks (
                    coupon_id, total_quantity, active_count, updated_at
                ) VALUES (?, ?, ?, ?)
                """,
                id,
                totalQuantity,
                activeCount,
                "2026-08-24 00:00:00"
        );
    }
}
