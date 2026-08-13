// CouponTemplate 도메인 모델과 coupon_templates 테이블을 상호 변환합니다.
package com.kafkick.storage.coupon.entity;

import com.kafkick.core.coupon.CouponTemplate;
import com.kafkick.core.coupon.CouponDayOfWeek;
import com.kafkick.core.coupon.CouponPolicyType;
import com.kafkick.core.coupon.MembershipGrade;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalTime;

@Entity
@Table(name = "coupon_templates")
public class CouponTemplateEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

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

    private CouponTemplateEntity(CouponTemplate couponTemplate) {
        this.id = couponTemplate.id();
        this.brandId = couponTemplate.brandId();
        this.name = couponTemplate.name();
        this.policyType = couponTemplate.policyType();
        this.discountRate = couponTemplate.discountRate();
        this.maxDiscountAmount = couponTemplate.maxDiscountAmount();
        this.discountAmount = couponTemplate.discountAmount();
        this.validDays = couponTemplate.validDays();
        this.nthWeek = (byte) couponTemplate.nthWeek();
        this.dayOfWeek = couponTemplate.dayOfWeek();
        this.startTime = couponTemplate.startTime();
        this.durationHours = couponTemplate.durationHours();
        this.stockPerOccurrence = couponTemplate.stockPerOccurrence();
        this.eligibleGradesMask = (byte) couponTemplate.eligibleGradesMask();
        this.active = couponTemplate.active();
    }

    public static CouponTemplateEntity from(CouponTemplate couponTemplate) {
        return new CouponTemplateEntity(couponTemplate);
    }

    public CouponTemplate toDomain() {
        return CouponTemplate.restore(
                id,
                brandId,
                name,
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount,
                validDays,
                nthWeek.intValue(),
                dayOfWeek,
                startTime,
                durationHours,
                stockPerOccurrence,
                MembershipGrade.fromMask(
                        eligibleGradesMask.intValue()
                ),
                active
        );
    }
}
