package com.kafkick.api.coupon.controller;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.MemberRequestHeaders;
import com.kafkick.api.coupon.dto.MemberCouponPageResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.service.MemberCouponQueryService;

@RestController
@RequestMapping("/api/v1/coupons")
public class MemberCouponController {

    private final MemberCouponQueryService memberCouponQueryService;

    public MemberCouponController(
            MemberCouponQueryService memberCouponQueryService
    ) {
        this.memberCouponQueryService = memberCouponQueryService;
    }

    @GetMapping
    public ResponseEnvelope<MemberCouponPageResponse> findPage(
            @RequestHeader(MemberRequestHeaders.MEMBER_ID)
            @Positive(message = "회원 ID는 0보다 커야 합니다.")
            Long memberId,
            @RequestParam(required = false)
            IssuanceStatus status,
            @RequestParam(defaultValue = "0")
            @Min(value = 0, message = "페이지 번호는 0 이상이어야 합니다.")
            int page,
            @RequestParam(defaultValue = "20")
            @Min(value = 1, message = "페이지 크기는 1 이상이어야 합니다.")
            @Max(value = 100, message = "페이지 크기는 100 이하여야 합니다.")
            int size
    ) {
        return ResponseEnvelope.success(MemberCouponPageResponse.from(
                memberCouponQueryService.findPage(
                        memberId,
                        status,
                        page,
                        size
                )
        ));
    }
}
