// Storage 모듈의 JPA Entity와 Spring Data Repository 검색 범위를 설정합니다.
package com.kafkick.storage.coupon.config;

import com.kafkick.storage.coupon.entity.CouponTemplateEntity;
import com.kafkick.storage.coupon.repository.CouponTemplateJpaRepository;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EntityScan(basePackageClasses = CouponTemplateEntity.class)
@EnableJpaRepositories(
        basePackageClasses = CouponTemplateJpaRepository.class
)
public class CouponStorageConfig {
}
