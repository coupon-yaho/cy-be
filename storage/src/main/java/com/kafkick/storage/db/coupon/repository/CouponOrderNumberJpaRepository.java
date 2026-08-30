package com.kafkick.storage.db.coupon.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kafkick.storage.db.coupon.entity.CouponOrderNumberEntity;

public interface CouponOrderNumberJpaRepository
        extends JpaRepository<CouponOrderNumberEntity, Long> {
}
