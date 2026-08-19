// 기술 독립적인 사용자 보유 쿠폰 조회 유즈케이스를 API 빈으로 구성합니다.
package com.kafkick.api.coupon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.coupon.port.MemberCouponQueryRepository;
import com.kafkick.core.coupon.service.MemberCouponQueryService;

@Configuration(proxyBeanMethods = false)
public class MemberCouponQueryConfiguration {

    @Bean
    public MemberCouponQueryService memberCouponQueryService(
            MemberCouponQueryRepository memberCouponQueryRepository
    ) {
        return new MemberCouponQueryService(memberCouponQueryRepository);
    }
}
