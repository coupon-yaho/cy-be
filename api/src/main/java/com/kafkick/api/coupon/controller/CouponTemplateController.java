// 관리자용 쿠폰 템플릿 생성 결과를 공통 응답 형식과 Location 헤더로 반환합니다.
package com.kafkick.api.coupon.controller;

import java.net.URI;

import jakarta.validation.Valid;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.dto.CouponTemplateCreateRequest;
import com.kafkick.api.coupon.dto.CouponTemplateCreateResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.service.CouponTemplateCreateService;

@RestController
@RequestMapping("/api/v1/admin/coupon-templates")
public class CouponTemplateController {

    private final CouponTemplateCreateService couponTemplateCreateService;

    public CouponTemplateController(
            CouponTemplateCreateService couponTemplateCreateService
    ) {
        this.couponTemplateCreateService = couponTemplateCreateService;
    }

    @PostMapping
    public ResponseEntity<ResponseEnvelope<CouponTemplateCreateResponse>> create(
            @Valid @RequestBody CouponTemplateCreateRequest request
    ) {
        CouponTemplate savedCouponTemplate =
                couponTemplateCreateService.create(request.toCommand());
        CouponTemplateCreateResponse response =
                CouponTemplateCreateResponse.from(savedCouponTemplate);

        URI location = URI.create(
                "/api/v1/admin/coupon-templates/" + response.id()
        );

        return ResponseEntity.created(location)
                .body(ResponseEnvelope.success(response));
    }
}
