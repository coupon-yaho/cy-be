package com.kafkick.api.coupon.controller;

import java.net.URI;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.dto.request.CouponRoundReservationRequest;
import com.kafkick.api.coupon.dto.response.CouponRoundReservationResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.service.CouponRoundReservationService;
import com.kafkick.core.support.TimeProvider;

@RestController
@RequestMapping("/api/v1/admin/coupon-templates")
public class CouponRoundAdminController {

    private final CouponRoundReservationService reservationService;
    private final TimeProvider timeProvider;

    public CouponRoundAdminController(
            CouponRoundReservationService reservationService,
            TimeProvider timeProvider
    ) {
        this.reservationService = reservationService;
        this.timeProvider = timeProvider;
    }

    @PostMapping("/{couponTemplateId}/rounds")
    public ResponseEntity<
            ResponseEnvelope<CouponRoundReservationResponse>
            > reserve(
            @PathVariable
            @Positive(message = "쿠폰 템플릿 ID는 0보다 커야 합니다.")
            Long couponTemplateId,
            @Valid @RequestBody CouponRoundReservationRequest request
    ) {
        CouponRound couponRound = reservationService.reserve(
                request.toCommand(
                        couponTemplateId,
                        timeProvider.instant()
                )
        );
        CouponRoundReservationResponse response =
                CouponRoundReservationResponse.from(couponRound);
        URI location = URI.create(
                "/api/v1/admin/coupon-rounds/" + response.id()
        );

        return ResponseEntity.created(location)
                .body(ResponseEnvelope.success(response));
    }
}
