package com.kafkick.api.coupon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.coupon.port.MemberCouponQueryPort;
import com.kafkick.core.coupon.service.MemberCouponQueryService;

@Configuration
public class MemberCouponQueryConfiguration {

    @Bean
    public MemberCouponQueryService memberCouponQueryService(
            MemberCouponQueryPort memberCouponQueryPort
    ) {
        return new MemberCouponQueryService(memberCouponQueryPort);
    }
}
