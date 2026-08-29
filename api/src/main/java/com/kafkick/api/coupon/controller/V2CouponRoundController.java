package com.kafkick.api.coupon.controller;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.dto.response.V2CouponDefinitionResponse;
import com.kafkick.api.coupon.query.V2IssuableCouponRoundQuery;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.support.TimeProvider;

/** V2 게이트를 쓰는 회차만의 조회 경로. V1 목록 계약은 건드리지 않는다. */
@RestController
@RequestMapping("/api/v2/coupon-rounds")
public final class V2CouponRoundController {

    private final V2IssuableCouponRoundQuery query;
    private final TimeProvider timeProvider;

    public V2CouponRoundController(V2IssuableCouponRoundQuery query, TimeProvider timeProvider) {
        this.query = query;
        this.timeProvider = timeProvider;
    }

    @GetMapping
    public ResponseEnvelope<List<V2CouponDefinitionResponse>> findOpenDefinitions() {
        return ResponseEnvelope.success(query.findOpenDefinitions(timeProvider.instant())
                .stream().map(V2CouponDefinitionResponse::from).toList());
    }
}
