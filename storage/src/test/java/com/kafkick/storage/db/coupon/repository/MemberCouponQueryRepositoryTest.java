package com.kafkick.storage.db.coupon.repository;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.query.MemberCouponPage;
import com.kafkick.storage.db.RepositoryTest;

import static org.assertj.core.api.Assertions.assertThat;

// 실제 MySQL에서 회원 소유권·상태·정렬 조건이 적용된 보유 쿠폰 페이지를 검증합니다.

@RepositoryTest
@Import(MemberCouponQueryAdapter.class)
class MemberCouponQueryRepositoryTest {

    @Autowired
    private MemberCouponQueryAdapter memberCouponQueryRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        insertReferenceData();
        insertCouponRounds();
        insertIssuances();
    }

    @Test
    @DisplayName("회원 쿠폰을 발급 시각과 ID 내림차순으로 페이지 조회한다")
    void findMemberCouponPageInStableNewestOrder() {
        MemberCouponPage firstPage = memberCouponQueryRepository
                .findPageByMemberId(20L, null, 0, 2);
        MemberCouponPage secondPage = memberCouponQueryRepository
                .findPageByMemberId(20L, null, 1, 2);

        assertThat(firstPage.content())
                .extracting(summary -> summary.issuanceId())
                .containsExactly(103L, 102L);
        assertThat(firstPage.content().get(0).name())
                .isEqualTo("정률 20% 할인");
        assertThat(firstPage.content().get(0).policyType())
                .isEqualTo(CouponPolicyType.PERCENT_CAPPED);
        assertThat(firstPage.content().get(0).discountRate()).isEqualTo(20);
        assertThat(firstPage.content().get(0).maxDiscountAmount())
                .isEqualTo(10_000);
        assertThat(firstPage.content().get(0).discountAmount()).isNull();
        assertThat(firstPage.page()).isZero();
        assertThat(firstPage.size()).isEqualTo(2);
        assertThat(firstPage.totalElements()).isEqualTo(4);
        assertThat(firstPage.totalPages()).isEqualTo(2);

        assertThat(secondPage.content())
                .extracting(summary -> summary.issuanceId())
                .containsExactly(101L, 100L);
    }

    @Test
    @DisplayName("회원 쿠폰을 발급 상태로 필터링한다")
    void filterMemberCouponsByStatus() {
        MemberCouponPage result = memberCouponQueryRepository
                .findPageByMemberId(20L, IssuanceStatus.USED, 0, 20);

        assertThat(result.content()).hasSize(1);
        assertThat(result.content().get(0).issuanceId()).isEqualTo(101L);
        assertThat(result.content().get(0).status())
                .isEqualTo(IssuanceStatus.USED);
        assertThat(result.content().get(0).name())
                .isEqualTo("정액 5천원 할인");
        assertThat(result.content().get(0).discountAmount()).isEqualTo(5_000);
        assertThat(result.totalElements()).isEqualTo(1);
    }

    @Test
    @DisplayName("다른 회원의 쿠폰은 조회 결과에 포함하지 않는다")
    void excludeOtherMemberCoupons() {
        MemberCouponPage result = memberCouponQueryRepository
                .findPageByMemberId(20L, null, 0, 20);

        assertThat(result.content())
                .extracting(summary -> summary.issuanceId())
                .doesNotContain(200L);
        assertThat(result.totalElements()).isEqualTo(4);
    }

    @Test
    @DisplayName("회원 쿠폰 조회용 보조 인덱스를 별도로 만들지 않는다")
    void omitMemberCouponListIndex() {
        List<String> columns = jdbcTemplate.queryForList(
                """
                SELECT column_name
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = 'issuances'
                  AND index_name = 'idx_issuances_member_issued'
                ORDER BY seq_in_index
                """,
                String.class
        );

        assertThat(columns).isEmpty();
    }

    private void insertReferenceData() {
        jdbcTemplate.update(
                "INSERT INTO grades (code, bit_value) VALUES (?, ?)",
                "GOLD",
                4
        );
        jdbcTemplate.update(
                "INSERT INTO brands (id, name, category) VALUES (?, ?, ?)",
                1L,
                "테스트 브랜드",
                "카페"
        );
        jdbcTemplate.update(
                """
                INSERT INTO coupon_templates (
                    id, brand_id, name, policy_type,
                    discount_amount, valid_days, nth_week, day_of_week,
                    start_time, duration_hours, stock_per_occurrence,
                    eligible_grades_mask, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                1L,
                1L,
                "조회 테스트 템플릿",
                "FIXED_AMOUNT",
                5_000,
                7,
                3,
                "TUE",
                LocalTime.of(14, 0),
                2,
                100,
                12,
                true
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

    private void insertCouponRounds() {
        insertRound(10L, "정액 5천원 할인", "FIXED_AMOUNT", null, null, 5_000);
        insertRound(11L, "정률 20% 할인", "PERCENT_CAPPED", 20, 10_000, null);
        insertRound(12L, "정액 3천원 할인", "FIXED_AMOUNT", null, null, 3_000);
        insertRound(13L, "정액 1천원 할인", "FIXED_AMOUNT", null, null, 1_000);
        insertRound(14L, "정액 5천원 할인", "FIXED_AMOUNT", null, null, 5_000);
    }

    private void insertRound(
            Long id,
            String name,
            String policyType,
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount
    ) {
        LocalDateTime openAt = LocalDateTime.of(2026, 8, 18, 5, 0)
                .plusDays(id - 10L);
        jdbcTemplate.update(
                """
                INSERT INTO coupons (
                    id, template_id, brand_id, name, policy_type,
                    discount_rate, max_discount_amount, discount_amount,
                    valid_days, eligible_grades_mask,
                    open_at, close_at, status, generated_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                id,
                1L,
                1L,
                name,
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount,
                7,
                12,
                openAt,
                openAt.plusHours(2),
                "OPEN",
                LocalDateTime.of(2026, 8, 17, 0, 0),
                LocalDateTime.of(2026, 8, 17, 0, 1)
        );
    }

    private void insertIssuances() {
        insertIssuance(100L, 10L, 20L, "AAAAAAAAAAAAAAA1", "ISSUED",
                LocalDateTime.of(2026, 8, 18, 5, 30));
        insertIssuance(101L, 14L, 20L, "AAAAAAAAAAAAAAA2", "USED",
                LocalDateTime.of(2026, 8, 18, 5, 40));
        insertIssuance(102L, 12L, 20L, "AAAAAAAAAAAAAAA3", "EXPIRED",
                LocalDateTime.of(2026, 8, 18, 5, 50));
        insertIssuance(103L, 11L, 20L, "AAAAAAAAAAAAAAA4", "CANCELLED",
                LocalDateTime.of(2026, 8, 18, 5, 50));
        insertIssuance(200L, 13L, 21L, "BBBBBBBBBBBBBBB1", "ISSUED",
                LocalDateTime.of(2026, 8, 18, 6, 0));
    }

    private void insertIssuance(
            Long id,
            Long couponRoundId,
            Long memberId,
            String code,
            String status,
            LocalDateTime issuedAt
    ) {
        jdbcTemplate.update(
                """
                INSERT INTO issuances (
                    id, coupon_id, member_id, code, issued_grade, status,
                    issued_at, expires_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'GOLD', ?, ?, ?, ?, ?)
                """,
                id,
                couponRoundId,
                memberId,
                code,
                status,
                issuedAt,
                issuedAt.plusDays(7),
                issuedAt,
                issuedAt
        );
    }
}
