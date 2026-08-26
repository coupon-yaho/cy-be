package com.kafkick.storage.db.coupon.repository;

import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.port.OrderNumberGenerator;
import com.kafkick.storage.db.coupon.entity.CouponOrderNumberEntity;

@Repository
public class OrderNumberGeneratorImpl implements OrderNumberGenerator {

    private final CouponOrderNumberJpaRepository orderNumberJpaRepository;

    public OrderNumberGeneratorImpl(
            CouponOrderNumberJpaRepository orderNumberJpaRepository
    ) {
        this.orderNumberJpaRepository = orderNumberJpaRepository;
    }

    @Override
    public long generate() {
        try {
            return orderNumberJpaRepository.saveAndFlush(
                    CouponOrderNumberEntity.create()
            ).getId();
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "쿠폰 사용 주문번호 생성에 실패했습니다.",
                    exception
            );
        }
    }
}
