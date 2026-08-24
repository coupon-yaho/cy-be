package com.kafkick.batch.coupon.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.kafkick.batch.coupon.round.CouponRoundGenerationProperties;

@Configuration
@EnableConfigurationProperties(CouponRoundGenerationProperties.class)
public class CouponRoundConfiguration {
}
