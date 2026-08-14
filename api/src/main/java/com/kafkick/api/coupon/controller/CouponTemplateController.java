// 관리자 쿠폰 템플릿 생성 및 단건 조회 API를 공통 응답 형식으로 제공합니다.
package com.kafkick.api.coupon.controller;

import java.net.URI;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.dto.CouponTemplateCreateRequest;
import com.kafkick.api.coupon.dto.CouponTemplateCreateResponse;
import com.kafkick.api.coupon.dto.CouponTemplateDetailResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.service.CouponTemplateCreateService;
import com.kafkick.core.coupon.service.CouponTemplateQueryService;

@RestController
@RequestMapping("/api/v1/admin/coupon-templates")
public class CouponTemplateController {

    private final CouponTemplateCreateService couponTemplateCreateService;
    private final CouponTemplateQueryService couponTemplateQueryService;

    public CouponTemplateController(
            CouponTemplateCreateService couponTemplateCreateService,
            CouponTemplateQueryService couponTemplateQueryService
    ) {
        this.couponTemplateCreateService = couponTemplateCreateService;
        this.couponTemplateQueryService = couponTemplateQueryService;
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

    @GetMapping("/{couponTemplateId}")
    public ResponseEnvelope<CouponTemplateDetailResponse> findById(
            @PathVariable
            @Positive(message = "쿠폰 템플릿 ID는 0보다 커야 합니다.")
            Long couponTemplateId
    ) {
        CouponTemplate couponTemplate =
                couponTemplateQueryService.findById(couponTemplateId);

        return ResponseEnvelope.success(
                CouponTemplateDetailResponse.from(couponTemplate)
        );
    }
}
