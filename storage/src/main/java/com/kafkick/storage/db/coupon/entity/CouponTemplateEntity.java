package com.kafkick.storage.db.coupon.entity;

import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;

import java.time.LocalTime;

import com.kafkick.storage.db.support.UpdatableEntity;

/**
 * 반복 발급 규칙을 저장하는 템플릿 엔티티다.
 *
 * <p>생성·수정 시각은 공통 감사 엔티티 계약으로 관리한다.</p>
 */
@Entity
@Table(name = "coupon_templates")
public class CouponTemplateEntity extends UpdatableEntity {

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

    @Column(name = "nth_week", nullable = false)
    private Byte nthWeek;

    @Enumerated(EnumType.STRING)
    @Column(name = "day_of_week", nullable = false, length = 3)
    private CouponDayOfWeek dayOfWeek;

    @Column(name = "start_time", nullable = false)
    private LocalTime startTime;

    @Column(name = "duration_hours", nullable = false)
    private int durationHours;

    @Column(name = "stock_per_occurrence", nullable = false)
    private int stockPerOccurrence;

    @Column(name = "eligible_grades_mask", nullable = false)
    private Byte eligibleGradesMask;

    @Column(nullable = false)
    private boolean active;

    protected CouponTemplateEntity() {
    }

    public CouponTemplateEntity(
            Long id,
            Long brandId,
            String name,
            CouponPolicyType policyType,
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount,
            int validDays,
            Byte nthWeek,
            CouponDayOfWeek dayOfWeek,
            LocalTime startTime,
            int durationHours,
            int stockPerOccurrence,
            Byte eligibleGradesMask,
            boolean active
    ) {
        super(id, null, null);
        this.brandId = brandId;
        this.name = name;
        this.policyType = policyType;
        this.discountRate = discountRate;
        this.maxDiscountAmount = maxDiscountAmount;
        this.discountAmount = discountAmount;
        this.validDays = validDays;
        this.nthWeek = nthWeek;
        this.dayOfWeek = dayOfWeek;
        this.startTime = startTime;
        this.durationHours = durationHours;
        this.stockPerOccurrence = stockPerOccurrence;
        this.eligibleGradesMask = eligibleGradesMask;
        this.active = active;
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

    public Byte getNthWeek() {
        return nthWeek;
    }

    public CouponDayOfWeek getDayOfWeek() {
        return dayOfWeek;
    }

    public LocalTime getStartTime() {
        return startTime;
    }

    public int getDurationHours() {
        return durationHours;
    }

    public int getStockPerOccurrence() {
        return stockPerOccurrence;
    }

    public Byte getEligibleGradesMask() {
        return eligibleGradesMask;
    }

    public boolean isActive() {
        return active;
    }
}
