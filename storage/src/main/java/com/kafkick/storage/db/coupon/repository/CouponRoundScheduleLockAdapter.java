package com.kafkick.storage.db.coupon.repository;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;

import org.springframework.stereotype.Repository;

import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.port.CouponRoundScheduleLockPort;

@Repository
public class CouponRoundScheduleLockAdapter
        implements CouponRoundScheduleLockPort {

    private final EntityManager entityManager;

    public CouponRoundScheduleLockAdapter(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Override
    public void lock() {
        try {
            entityManager.createNativeQuery("""
                    SELECT id
                    FROM coupon_round_schedule_guard
                    WHERE id = 1
                    FOR UPDATE
                    """).getSingleResult();
        } catch (PersistenceException exception) {
            throw new CouponPersistenceException(
                    "쿠폰 회차 전역 예약 잠금에 실패했습니다.",
                    exception
            );
        }
    }
}
