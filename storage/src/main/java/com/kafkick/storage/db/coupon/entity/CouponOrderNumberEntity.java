package com.kafkick.storage.db.coupon.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import com.kafkick.storage.db.support.BaseEntity;

@Entity
@Table(name = "coupon_order_numbers")
public class CouponOrderNumberEntity extends BaseEntity {

    protected CouponOrderNumberEntity() {
    }

    public static CouponOrderNumberEntity create() {
        return new CouponOrderNumberEntity();
    }
}
