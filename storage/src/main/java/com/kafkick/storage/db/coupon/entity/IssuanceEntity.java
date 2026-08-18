// issuances 테이블의 회원별 쿠폰 발급건과 발급 시점 스냅샷을 표현합니다.
package com.kafkick.storage.db.coupon.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.storage.db.support.UpdatableEntity;

@Entity
@Table(name = "issuances")
public class IssuanceEntity extends UpdatableEntity {

    @Column(name = "coupon_id", nullable = false)
    private Long couponId;

    @Column(name = "member_id", nullable = false)
    private Long memberId;

    @Column(nullable = false, length = 16, unique = true)
    private String code;

    @Enumerated(EnumType.STRING)
    @Column(name = "issued_grade", nullable = false, length = 10)
    private MembershipGrade issuedGrade;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 12)
    private IssuanceStatus status;

    @Column(name = "issued_at", nullable = false, updatable = false)
    private Instant issuedAt;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    protected IssuanceEntity() {
    }

    public IssuanceEntity(
            Long id,
            Long couponId,
            Long memberId,
            String code,
            MembershipGrade issuedGrade,
            IssuanceStatus status,
            Instant issuedAt,
            Instant expiresAt,
            Instant updatedAt
    ) {
        super(id, null, updatedAt);
        this.couponId = couponId;
        this.memberId = memberId;
        this.code = code;
        this.issuedGrade = issuedGrade;
        this.status = status;
        this.issuedAt = issuedAt;
        this.expiresAt = expiresAt;
    }

    public Long getCouponId() {
        return couponId;
    }

    public Long getMemberId() {
        return memberId;
    }

    public String getCode() {
        return code;
    }

    public MembershipGrade getIssuedGrade() {
        return issuedGrade;
    }

    public IssuanceStatus getStatus() {
        return status;
    }

    public Instant getIssuedAt() {
        return issuedAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

}
