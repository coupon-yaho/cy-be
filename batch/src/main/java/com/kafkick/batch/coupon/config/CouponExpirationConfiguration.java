package com.kafkick.batch.coupon.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.kafkick.batch.coupon.expiration.CouponExpirationProperties;

// 쿠폰 만료 배치의 외부 설정값과 스케줄링만 활성화합니다.
@Configuration
@EnableScheduling
@EnableConfigurationProperties(CouponExpirationProperties.class)
public class CouponExpirationConfiguration {
}
