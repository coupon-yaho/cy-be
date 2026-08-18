package com.kafkick.storage.db.coupon.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kafkick.storage.db.coupon.entity.IssuanceUsageEntity;

public interface IssuanceUsageJpaRepository
        extends JpaRepository<IssuanceUsageEntity, Long> {
}
