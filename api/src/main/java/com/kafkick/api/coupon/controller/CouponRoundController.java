package com.kafkick.api.coupon.controller;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.dto.response.CouponRoundDetailResponse;
import com.kafkick.api.coupon.dto.response.IssuableCouponRoundPageResponse;
import com.kafkick.api.coupon.dto.response.PublicCouponRoundPageResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.core.coupon.service.CouponRoundDetailQueryService;
import com.kafkick.core.coupon.service.IssuableCouponRoundQueryService;
import com.kafkick.core.coupon.service.PublicCouponRoundQueryService;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.TimeProvider;

@RestController
@RequestMapping("/api/v1/coupon-rounds")
public class CouponRoundController {

    private final IssuableCouponRoundQueryService queryService;
    private final CouponRoundDetailQueryService detailQueryService;
    private final PublicCouponRoundQueryService publicQueryService;
    private final TimeProvider timeProvider;

    public CouponRoundController(
            IssuableCouponRoundQueryService queryService,
            CouponRoundDetailQueryService detailQueryService,
            PublicCouponRoundQueryService publicQueryService,
            TimeProvider timeProvider
    ) {
        this.queryService = queryService;
        this.detailQueryService = detailQueryService;
        this.publicQueryService = publicQueryService;
        this.timeProvider = timeProvider;
    }

    /**
     * 공개 회차를 상태와 참여 등급으로 필터링합니다.
     * 상태는 SCHEDULED, OPEN, CLOSED를, 등급은 WELCOME, SILVER, GOLD, VIP를 허용하며
     * 지원하지 않는 enum 값은 400 잘못된 요청으로 처리됩니다.
     */
    @GetMapping("/public")
    public ResponseEnvelope<PublicCouponRoundPageResponse> findPublicPage(
            @RequestParam(required = false)
            CouponRoundStatus status,
            @RequestParam(required = false)
            MembershipGrade eligibleGrade,
            @RequestParam(defaultValue = "0")
            @Min(value = 0, message = "페이지 번호는 0 이상이어야 합니다.")
            int page,
            @RequestParam(defaultValue = "20")
            @Min(value = 1, message = "페이지 크기는 1 이상이어야 합니다.")
            @Max(value = 100, message = "페이지 크기는 100 이하여야 합니다.")
            int size
    ) {
        return ResponseEnvelope.success(
                PublicCouponRoundPageResponse.from(
                        publicQueryService.findPage(
                                status,
                                eligibleGrade,
                                page,
                                size
                        )
                )
        );
    }

    @GetMapping
    public ResponseEnvelope<IssuableCouponRoundPageResponse> findIssuablePage(
            @RequestHeader(MemberRequestHeaders.MEMBER_ID)
            @Positive(message = "회원 ID는 0보다 커야 합니다.")
            Long memberId,
            @RequestHeader(MemberRequestHeaders.MEMBERSHIP_GRADE)
            MembershipGrade membershipGrade,
            @RequestParam(defaultValue = "0")
            @Min(value = 0, message = "페이지 번호는 0 이상이어야 합니다.")
            int page,
            @RequestParam(defaultValue = "20")
            @Min(value = 1, message = "페이지 크기는 1 이상이어야 합니다.")
            @Max(value = 100, message = "페이지 크기는 100 이하여야 합니다.")
            int size
    ) {
        return ResponseEnvelope.success(
                IssuableCouponRoundPageResponse.from(
                        queryService.findPage(
                                memberId,
                                membershipGrade,
                                timeProvider.instant(),
                                page,
                                size
                        )
                )
        );
    }

    @GetMapping("/{couponRoundId}")
    public ResponseEnvelope<CouponRoundDetailResponse> findOne(
            @PathVariable
            @Positive(message = "쿠폰 회차 ID는 0보다 커야 합니다.")
            Long couponRoundId
    ) {
        return ResponseEnvelope.success(
                CouponRoundDetailResponse.from(
                        detailQueryService.findById(couponRoundId)
                )
        );
    }
}
