// 관리자 쿠폰 템플릿 생성·조회·수정·활성화 API를 공통 응답 형식으로 제공합니다.
package com.kafkick.api.coupon.controller;

import java.net.URI;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.adapter.CouponTemplateActivationTransactionalAdapter;
import com.kafkick.api.coupon.adapter.CouponTemplateUpdateTransactionalAdapter;
import com.kafkick.api.coupon.dto.CouponTemplateActivationRequest;
import com.kafkick.api.coupon.dto.CouponTemplateCreateRequest;
import com.kafkick.api.coupon.dto.CouponTemplateCreateResponse;
import com.kafkick.api.coupon.dto.CouponTemplateDetailResponse;
import com.kafkick.api.coupon.dto.CouponTemplatePageResponse;
import com.kafkick.api.coupon.dto.CouponTemplateUpdateRequest;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.service.CouponTemplateCreateService;
import com.kafkick.core.coupon.service.CouponTemplateQueryService;

@RestController
@RequestMapping("/api/v1/admin/coupon-templates")
public class CouponTemplateController {

    private final CouponTemplateCreateService couponTemplateCreateService;
    private final CouponTemplateQueryService couponTemplateQueryService;
    private final CouponTemplateUpdateTransactionalAdapter
            couponTemplateUpdateTransactionalAdapter;
    private final CouponTemplateActivationTransactionalAdapter
            couponTemplateActivationTransactionalAdapter;

    public CouponTemplateController(
            CouponTemplateCreateService couponTemplateCreateService,
            CouponTemplateQueryService couponTemplateQueryService,
            CouponTemplateUpdateTransactionalAdapter
                    couponTemplateUpdateTransactionalAdapter,
            CouponTemplateActivationTransactionalAdapter
                    couponTemplateActivationTransactionalAdapter
    ) {
        this.couponTemplateCreateService = couponTemplateCreateService;
        this.couponTemplateQueryService = couponTemplateQueryService;
        this.couponTemplateUpdateTransactionalAdapter =
                couponTemplateUpdateTransactionalAdapter;
        this.couponTemplateActivationTransactionalAdapter =
                couponTemplateActivationTransactionalAdapter;
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

    @GetMapping
    public ResponseEnvelope<CouponTemplatePageResponse> findPage(
            @RequestParam(defaultValue = "0")
            @Min(value = 0, message = "페이지 번호는 0 이상이어야 합니다.")
            int page,
            @RequestParam(defaultValue = "20")
            @Min(value = 1, message = "페이지 크기는 1 이상이어야 합니다.")
            @Max(value = 100, message = "페이지 크기는 100 이하여야 합니다.")
            int size
    ) {
        return ResponseEnvelope.success(
                CouponTemplatePageResponse.from(
                        couponTemplateQueryService.findPage(page, size)
                )
        );
    }

    @PutMapping("/{couponTemplateId}")
    public ResponseEnvelope<CouponTemplateDetailResponse> update(
            @PathVariable
            @Positive(message = "쿠폰 템플릿 ID는 0보다 커야 합니다.")
            Long couponTemplateId,
            @Valid @RequestBody CouponTemplateUpdateRequest request
    ) {
        CouponTemplate updatedCouponTemplate =
                couponTemplateUpdateTransactionalAdapter.update(
                        couponTemplateId,
                        request.toCommand()
                );

        return ResponseEnvelope.success(
                CouponTemplateDetailResponse.from(updatedCouponTemplate)
        );
    }

    @PatchMapping("/{couponTemplateId}/activation")
    public ResponseEnvelope<CouponTemplateDetailResponse> changeActivation(
            @PathVariable
            @Positive(message = "쿠폰 템플릿 ID는 0보다 커야 합니다.")
            Long couponTemplateId,
            @Valid @RequestBody CouponTemplateActivationRequest request
    ) {
        CouponTemplate couponTemplate =
                couponTemplateActivationTransactionalAdapter
                        .changeActivation(
                                couponTemplateId,
                                request.toCommand()
                        );

        return ResponseEnvelope.success(
                CouponTemplateDetailResponse.from(couponTemplate)
        );
    }
}
