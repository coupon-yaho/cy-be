package com.kafkick.storage.db.coupon.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kafkick.storage.db.coupon.entity.IssuanceEntity;

public interface IssuanceJpaRepository
        extends JpaRepository<IssuanceEntity, Long> {
}
