package com.kafkick.storage.db;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;

/**
 * issuances 는 coupons·members 를, coupons 는 coupon_templates·brands 를, members 는 grades 를
 * FK 로 문다. 사용 행 하나를 넣으려면 이 사슬을 전부 세워야 한다.
 *
 * <p>검증하려는 값과 무관한 컬럼은 고정값으로 채운다. 회차·회원은 처음 필요할 때 한 번만 만든다.
 */
public final class VerificationSeed {

    /**
     * 발급 시각의 기준. 어떤 테스트의 {@code asOf} 보다도 앞서야 한다 —
     * {@code issuances.updated_at} 이 이 값으로 찍히는데, V3 는 {@code updated_at <= asOf} 인
     * 발급건만 비교하므로 이 값이 뒤에 있으면 발급건이 통째로 빠진다.
     */
    private static final LocalDateTime EPOCH = LocalDateTime.of(2025, 1, 1, 0, 0);

    /** 자식이 먼저다. 순서가 틀리면 FK 가 삭제를 거부한다. */
    private static final List<String> TABLES_IN_DELETE_ORDER = List.of(
            "asof_state", "verification_findings", "verification_runs",
            "issuance_usages", "issuance_histories", "issuances",
            "coupon_stocks", "coupons", "coupon_templates", "brands",
            "members", "grades");

    private final JdbcClient jdbcClient;

    private Long couponId;
    private boolean gradesInserted;
    private int codeSequence;

    public VerificationSeed(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /** 발급건 하나를 만들고 식별자를 돌려준다. */
    public long issuance(IssuanceStatus status) {
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
                .param("issuedAt", EPOCH)
                .param("expiresAt", EPOCH.plusDays(7)));
    }

    /** 이력 한 행을 만들고 식별자를 돌려준다. {@code fromStatus} 가 null 이면 발급 이력이다. */
    public long history(
            long issuanceId,
            IssuanceEventType eventType,
            IssuanceStatus fromStatus,
            IssuanceStatus toStatus,
            LocalDateTime createdAt
    ) {
        return insertGenerated(jdbcClient.sql("""
                        INSERT INTO issuance_histories
                            (issuance_id, event_type, from_status, to_status, created_at)
                        VALUES (:issuanceId, :eventType, :fromStatus, :toStatus, :createdAt)
                        """)
                .param("issuanceId", issuanceId)
                .param("eventType", eventType.name())
                .param("fromStatus", fromStatus == null ? null : fromStatus.name())
                .param("toStatus", toStatus.name())
                .param("createdAt", createdAt));
    }

    /** 사용 행 하나. {@code canceledAt} 이 null 이면 취소되지 않은 사용이다. */
    public void usage(long issuanceId, LocalDateTime usedAt, LocalDateTime canceledAt) {
        jdbcClient.sql("""
                        INSERT INTO issuance_usages
                            (issuance_id, order_id, discount_amount, used_at, canceled_at)
                        VALUES (:issuanceId, NULL, 1000, :usedAt, :canceledAt)
                        """)
                .param("issuanceId", issuanceId)
                .param("usedAt", usedAt)
                .param("canceledAt", canceledAt)
                .update();
    }

    /**
     * 검증이 건드리는 테이블을 FK 역순으로 비운다.
     *
     * <p>{@code @RepositoryTest} 는 테스트마다 롤백하므로 부를 일이 없다.
     * 잡을 실제로 돌리는 테스트는 트랜잭션 밖이라 롤백이 없어 이걸 써야 한다.
     */
    public void clear() {
        TABLES_IN_DELETE_ORDER.forEach(
                table -> jdbcClient.sql("DELETE FROM " + table).update());

        // 캐시를 안 비우면 다음 issuance() 가 방금 지운 회차·등급을 FK 로 가리킨다.
        couponId = null;
        gradesInserted = false;
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
                .param("createdAt", EPOCH));
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
                .param("openAt", EPOCH)
                .param("closeAt", EPOCH.plusDays(1))
                .param("createdAt", EPOCH));
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
