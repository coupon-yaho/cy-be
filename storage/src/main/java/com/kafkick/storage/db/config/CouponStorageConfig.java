package com.kafkick.storage.db.config;

import com.kafkick.storage.db.coupon.entity.IssuanceEntity;
import com.kafkick.storage.db.coupontemplate.entity.CouponTemplateEntity;
import com.kafkick.storage.db.notification.entity.NotificationEntity;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EntityScan(basePackageClasses = {
        IssuanceEntity.class,
        CouponTemplateEntity.class,
        NotificationEntity.class
})
@EnableJpaRepositories(basePackages = {
        "com.kafkick.storage.db.coupon.repository",
        "com.kafkick.storage.db.coupontemplate.repository",
        "com.kafkick.storage.db.notification.repository"
})
public class CouponStorageConfig {
}
