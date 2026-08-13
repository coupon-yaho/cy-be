// 쿠폰 템플릿의 저장과 조회를 담당합니다.
package com.kafkick.storage.coupon.repository;

import com.kafkick.storage.coupon.entity.CouponTemplateEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CouponTemplateJpaRepository
        extends JpaRepository<CouponTemplateEntity, Long> {
}
