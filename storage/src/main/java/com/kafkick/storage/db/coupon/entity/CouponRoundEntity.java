// coupons 테이블에 저장되는 쿠폰 회차 스냅샷을 표현합니다.
package com.kafkick.storage.db.coupon.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;

import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.storage.db.support.BaseEntity;

@Entity
@Table(name = "coupons")
public class CouponRoundEntity extends BaseEntity {

    @Column(name = "template_id", nullable = false)
    private Long templateId;

    @Column(name = "brand_id", nullable = false)
    private Long brandId;

    @Column(nullable = false, length = 100)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(name = "policy_type", nullable = false, length = 20)
    private CouponPolicyType policyType;

    @Column(name = "discount_rate")
    private Integer discountRate;

    @Column(name = "max_discount_amount")
    private Integer maxDiscountAmount;

    @Column(name = "discount_amount")
    private Integer discountAmount;

    @Column(name = "valid_days", nullable = false)
    private int validDays;

    @Column(name = "eligible_grades_mask", nullable = false)
    private Byte eligibleGradesMask;

    @Column(name = "open_at", nullable = false)
    private Instant openAt;

    @Column(name = "close_at", nullable = false)
    private Instant closeAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private CouponRoundStatus status;

    @Column(name = "generated_at", nullable = false, updatable = false)
    private Instant generatedAt;

    protected CouponRoundEntity() {
    }

    public CouponRoundEntity(
            Long id,
            Long templateId,
            Long brandId,
            String name,
            CouponPolicyType policyType,
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount,
            int validDays,
            Byte eligibleGradesMask,
            Instant openAt,
            Instant closeAt,
            CouponRoundStatus status,
            Instant generatedAt
    ) {
        super(id, null);
        this.templateId = templateId;
        this.brandId = brandId;
        this.name = name;
        this.policyType = policyType;
        this.discountRate = discountRate;
        this.maxDiscountAmount = maxDiscountAmount;
        this.discountAmount = discountAmount;
        this.validDays = validDays;
        this.eligibleGradesMask = eligibleGradesMask;
        this.openAt = openAt;
        this.closeAt = closeAt;
        this.status = status;
        this.generatedAt = generatedAt;
    }

    public Long getTemplateId() {
        return templateId;
    }

    public Long getBrandId() {
        return brandId;
    }

    public String getName() {
        return name;
    }

    public CouponPolicyType getPolicyType() {
        return policyType;
    }

    public Integer getDiscountRate() {
        return discountRate;
    }

    public Integer getMaxDiscountAmount() {
        return maxDiscountAmount;
    }

    public Integer getDiscountAmount() {
        return discountAmount;
    }

    public int getValidDays() {
        return validDays;
    }

    public Byte getEligibleGradesMask() {
        return eligibleGradesMask;
    }

    public Instant getOpenAt() {
        return openAt;
    }

    public Instant getCloseAt() {
        return closeAt;
    }

    public CouponRoundStatus getStatus() {
        return status;
    }

    public Instant getGeneratedAt() {
        return generatedAt;
    }

}
