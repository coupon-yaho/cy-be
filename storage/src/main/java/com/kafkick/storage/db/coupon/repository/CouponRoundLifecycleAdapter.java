package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.port.CouponRoundLifecyclePort;

@Repository
public class CouponRoundLifecycleAdapter implements CouponRoundLifecyclePort {

    private final CouponRoundJpaRepository couponRoundJpaRepository;

    public CouponRoundLifecycleAdapter(
            CouponRoundJpaRepository couponRoundJpaRepository
    ) {
        this.couponRoundJpaRepository = couponRoundJpaRepository;
    }

    @Override
    public int closeOpenRounds(Instant asOf) {
        try {
            return couponRoundJpaRepository.closeOpenRounds(asOf);
        } catch (DataAccessException exception) {
            throw persistenceFailure("OPEN 회차 종료 처리", exception);
        }
    }

    @Override
    public int closeMissedScheduledRounds(Instant asOf) {
        try {
            return couponRoundJpaRepository.closeMissedScheduledRounds(asOf);
        } catch (DataAccessException exception) {
            throw persistenceFailure("누락된 SCHEDULED 회차 종료 처리", exception);
        }
    }

    @Override
    public int openScheduledRounds(Instant asOf) {
        try {
            return couponRoundJpaRepository.openNextScheduledRound(asOf);
        } catch (DataAccessException exception) {
            throw persistenceFailure("SCHEDULED 회차 오픈 처리", exception);
        }
    }

    private static CouponPersistenceException persistenceFailure(
            String operation,
            DataAccessException cause
    ) {
        return new CouponPersistenceException(
                operation + "에 실패했습니다.",
                cause
        );
    }
}
