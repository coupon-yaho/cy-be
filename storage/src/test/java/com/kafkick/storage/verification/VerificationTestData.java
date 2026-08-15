package com.kafkick.storage.verification;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import com.kafkick.core.coupon.IssuanceStatus;

/**
 * issuances 는 coupons·members 를, coupons 는 coupon_templates·brands 를, members 는 grades 를
 * FK 로 문다. 사용 행 하나를 넣으려면 이 사슬을 전부 세워야 한다.
 *
 * <p>검증하려는 값과 무관한 컬럼은 고정값으로 채운다. 회차·회원은 처음 필요할 때 한 번만 만든다.
 */
final class VerificationTestData {

    private static final LocalDateTime EPOCH = LocalDateTime.of(2026, 8, 1, 0, 0);

    private final JdbcClient jdbcClient;

    private Long couponId;
    private boolean gradesInserted;
    private int codeSequence;

    VerificationTestData(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /** 발급건 하나를 만들고 식별자를 돌려준다. */
    long issuance(IssuanceStatus status) {
        return insertGenerated(jdbcClient.sql("""
                        INSERT INTO issuances
                            (coupon_id, member_id, code, issued_grade, status,
                             issued_at, expires_at, updated_at)
                        VALUES (:couponId, :memberId, :code, 'VIP', :status,
                                :issuedAt, :expiresAt, :issuedAt)
                        """)
                .param("couponId", couponId())
                .param("memberId", newMemberId())
                .param("code", nextCode())
                .param("status", status.name())
                .param("issuedAt", Timestamp.valueOf(EPOCH))
                .param("expiresAt", Timestamp.valueOf(EPOCH.plusDays(7))));
    }

    /** 사용 행 하나. {@code canceledAt} 이 null 이면 취소되지 않은 사용이다. */
    void usage(long issuanceId, LocalDateTime usedAt, LocalDateTime canceledAt) {
        jdbcClient.sql("""
                        INSERT INTO issuance_usages
                            (issuance_id, order_id, discount_amount, used_at, canceled_at)
                        VALUES (:issuanceId, NULL, 1000, :usedAt, :canceledAt)
                        """)
                .param("issuanceId", issuanceId)
                .param("usedAt", Timestamp.valueOf(usedAt))
                .param("canceledAt", canceledAt == null ? null : Timestamp.valueOf(canceledAt))
                .update();
    }

    private long couponId() {
        if (couponId == null) {
            long brandId = insertBrand();
            long templateId = insertTemplate(brandId);
            couponId = insertCoupon(templateId, brandId);
        }
        return couponId;
    }

    /**
     * 발급건마다 회원을 새로 만든다. {@code issuances.uk_coupon_member} 가 1인 1매를 강제하므로
     * 같은 회차에 같은 회원으로 두 건을 넣을 수 없다.
     */
    private long newMemberId() {
        if (!gradesInserted) {
            jdbcClient.sql("""
                    INSERT IGNORE INTO grades (code, bit_value)
                    VALUES ('WELCOME', 1), ('SILVER', 2), ('GOLD', 4), ('VIP', 8)
                    """).update();
            gradesInserted = true;
        }

        return insertGenerated(jdbcClient.sql("""
                        INSERT INTO members (membership_grade, created_at)
                        VALUES ('VIP', :createdAt)
                        """)
                .param("createdAt", Timestamp.valueOf(EPOCH)));
    }

    private long insertBrand() {
        return insertGenerated(jdbcClient.sql("""
                INSERT INTO brands (name, category) VALUES ('테스트브랜드', '카페')
                """));
    }

    private long insertTemplate(long brandId) {
        return insertGenerated(jdbcClient.sql("""
                        INSERT INTO coupon_templates
                            (brand_id, name, policy_type, discount_amount, valid_days,
                             stock_per_occurrence, eligible_grades_mask, active)
                        VALUES (:brandId, '테스트템플릿', 'FIXED_AMOUNT', 1000, 7, 100, 15, true)
                        """)
                .param("brandId", brandId));
    }

    private long insertCoupon(long templateId, long brandId) {
        return insertGenerated(jdbcClient.sql("""
                        INSERT INTO coupons
                            (template_id, brand_id, name, policy_type, discount_amount,
                             valid_days, eligible_grades_mask, open_at, close_at, status, created_at)
                        VALUES (:templateId, :brandId, '테스트회차', 'FIXED_AMOUNT', 1000,
                                7, 15, :openAt, :closeAt, 'OPEN', :createdAt)
                        """)
                .param("templateId", templateId)
                .param("brandId", brandId)
                .param("openAt", Timestamp.valueOf(EPOCH))
                .param("closeAt", Timestamp.valueOf(EPOCH.plusDays(1)))
                .param("createdAt", Timestamp.valueOf(EPOCH)));
    }

    private String nextCode() {
        return String.format("TEST%012d", ++codeSequence);
    }

    private static long insertGenerated(JdbcClient.StatementSpec spec) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        spec.update(keyHolder);

        Number generated = keyHolder.getKey();
        if (generated == null) {
            throw new IllegalStateException("식별자가 생성되지 않았습니다.");
        }
        return generated.longValue();
    }
}
