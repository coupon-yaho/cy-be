package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.query.IssuableCouponRoundPage;
import com.kafkick.storage.db.RepositoryTest;

import static org.assertj.core.api.Assertions.assertThat;

@RepositoryTest
@Import(IssuableCouponRoundQueryAdapter.class)
class IssuableCouponRoundQueryRepositoryTest {

    private static final Instant AS_OF =
            Instant.parse("2026-08-22T09:00:00Z");

    @Autowired
    private IssuableCouponRoundQueryAdapter queryAdapter;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        insertReferenceData();
        insertRound(10L, "먼저 종료", "OPEN", 4,
                "2026-08-22 08:00:00", "2026-08-22 10:00:00", 10, 2);
        insertRound(11L, "나중 종료", "OPEN", 12,
                "2026-08-22 08:30:00", "2026-08-22 11:00:00", 5, 1);
        insertRound(12L, "등급 제외", "OPEN", 8,
                "2026-08-22 08:02:00", "2026-08-22 10:00:00", 10, 0);
        insertRound(13L, "재고 소진", "OPEN", 4,
                "2026-08-22 08:03:00", "2026-08-22 10:00:00", 3, 3);
        insertRound(14L, "예약 상태", "SCHEDULED", 4,
                "2026-08-22 08:04:00", "2026-08-22 10:00:00", 10, 0);
        insertRound(15L, "이미 종료", "OPEN", 4,
                "2026-08-22 07:00:00", "2026-08-22 09:00:00", 10, 0);
        insertRound(16L, "이미 발급", "OPEN", 4,
                "2026-08-22 08:06:00", "2026-08-22 10:00:00", 10, 1);
        insertRound(18L, "사용 완료", "OPEN", 4,
                "2026-08-22 08:08:00", "2026-08-22 10:00:00", 10, 1);
        insertRound(19L, "발급 취소", "OPEN", 4,
                "2026-08-22 08:09:00", "2026-08-22 10:00:00", 10, 0);
        insertRound(20L, "만료 완료", "OPEN", 4,
                "2026-08-22 08:10:00", "2026-08-22 10:00:00", 10, 0);
        insertIssuance(100L, 16L, 20L, "ISSUED");
        insertIssuance(102L, 18L, 20L, "USED");
        insertIssuance(103L, 19L, 20L, "CANCELLED");
        insertIssuance(104L, 20L, 20L, "EXPIRED");
    }

    @Test
    @DisplayName("시간 등급 재고 중복 조건을 만족하는 회차만 종료순으로 조회한다")
    void findOnlyIssuableCouponRounds() {
        IssuableCouponRoundPage result = queryAdapter.findPage(
                20L,
                4,
                AS_OF,
                0,
                20
        );

        assertThat(result.content())
                .extracting(summary -> summary.couponRoundId())
                .containsExactly(10L, 11L);
        assertThat(result.content().get(0).policyType())
                .isEqualTo(CouponPolicyType.FIXED_AMOUNT);
        assertThat(result.content().get(0).discountAmount())
                .isEqualTo(5_000);
        assertThat(result.content().get(0).validDays()).isEqualTo(7);
        assertThat(result.content().get(0).remainingQuantity()).isEqualTo(8);
        assertThat(result.content().get(1).remainingQuantity()).isEqualTo(4);
        assertThat(result.totalElements()).isEqualTo(2);
        assertThat(result.totalPages()).isEqualTo(1);
    }

    @Test
    @DisplayName("다른 회원이 발급받은 회차는 현재 회원에게 계속 노출한다")
    void doNotExcludeRoundIssuedToAnotherMember() {
        insertIssuance(101L, 10L, 21L, "ISSUED");

        IssuableCouponRoundPage result = queryAdapter.findPage(
                20L,
                4,
                AS_OF,
                0,
                20
        );

        assertThat(result.content())
                .extracting(summary -> summary.couponRoundId())
                .contains(10L);
    }

    @Test
    @DisplayName("회원의 발급 이력이 있으면 모든 발급 상태에서 회차를 제외한다")
    void excludeRoundsForEveryIssuanceStatus() {
        IssuableCouponRoundPage result = queryAdapter.findPage(
                20L,
                4,
                AS_OF,
                0,
                20
        );

        assertThat(result.content())
                .extracting(summary -> summary.couponRoundId())
                .doesNotContain(16L, 18L, 19L, 20L);
    }

    @Test
    @DisplayName("종료 시각과 ID 순서로 다음 페이지를 안정적으로 조회한다")
    void paginateInStableClosingOrder() {
        IssuableCouponRoundPage firstPage = queryAdapter.findPage(
                20L,
                4,
                AS_OF,
                0,
                1
        );
        IssuableCouponRoundPage secondPage = queryAdapter.findPage(
                20L,
                4,
                AS_OF,
                1,
                1
        );

        assertThat(firstPage.content())
                .extracting(summary -> summary.couponRoundId())
                .containsExactly(10L);
        assertThat(secondPage.content())
                .extracting(summary -> summary.couponRoundId())
                .containsExactly(11L);
        assertThat(firstPage.totalElements()).isEqualTo(2);
        assertThat(firstPage.totalPages()).isEqualTo(2);
    }

    @Test
    @DisplayName("현재 시각에 정확히 오픈하는 회차를 조회한다")
    void includeRoundOpeningExactlyAtAsOf() {
        insertRound(17L, "지금 오픈", "OPEN", 4,
                "2026-08-22 09:00:00", "2026-08-22 09:30:00", 10, 0);

        IssuableCouponRoundPage result = queryAdapter.findPage(
                20L,
                4,
                AS_OF,
                0,
                20
        );

        assertThat(result.content())
                .extracting(summary -> summary.couponRoundId())
                .contains(17L);
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
                ) VALUES (1, 1, '조회 템플릿', 'FIXED_AMOUNT',
                          5000, 7, 3, 'SAT', ?, 2, 10, 12, true)
                """,
                LocalTime.of(17, 0)
        );
        jdbcTemplate.batchUpdate(
                """
                INSERT INTO members (id, membership_grade, created_at)
                VALUES (?, 'GOLD', ?)
                """,
                java.util.List.of(
                        new Object[]{20L, LocalDateTime.of(2026, 1, 1, 0, 0)},
                        new Object[]{21L, LocalDateTime.of(2026, 1, 1, 0, 0)}
                )
        );
    }

    private void insertRound(
            Long id,
            String name,
            String status,
            int eligibleGradesMask,
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
                ) VALUES (?, 1, 1, ?, 'FIXED_AMOUNT',
                          5000, 7, ?, ?, ?, ?, ?, ?)
                """,
                id,
                name,
                eligibleGradesMask,
                openAt,
                closeAt,
                status,
                "2026-08-21 00:00:00",
                "2026-08-21 00:00:01"
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
                "2026-08-22 08:00:00"
        );
    }

    private void insertIssuance(
            Long id,
            Long couponId,
            Long memberId,
            String status
    ) {
        jdbcTemplate.update(
                """
                INSERT INTO issuances (
                    id, coupon_id, member_id, code, issued_grade, status,
                    issued_at, expires_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'GOLD', ?, ?, ?, ?, ?)
                """,
                id,
                couponId,
                memberId,
                "CODE" + id,
                status,
                "2026-08-22 08:30:00",
                "2026-08-29 08:30:00",
                "2026-08-22 08:30:00",
                "2026-08-22 08:30:00"
        );
    }
}
