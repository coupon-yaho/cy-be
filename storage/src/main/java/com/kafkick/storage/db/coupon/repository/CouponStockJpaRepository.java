// coupon_stocks 테이블의 Spring Data JPA 저장 계약입니다.
package com.kafkick.storage.db.coupon.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kafkick.storage.db.coupon.entity.CouponStockEntity;

public interface CouponStockJpaRepository
        extends JpaRepository<CouponStockEntity, Long> {
}
