package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.stereotype.Service;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.coupon.exception.CouponRoundErrorCode;
import com.kafkick.core.coupon.service.command.CouponRoundReservationCommand;
import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.coupontemplate.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;
import com.kafkick.core.support.exception.BusinessException;

@Service
public class CouponRoundReservationService {

    private final CouponTemplateRepository couponTemplateRepository;
    private final CouponRoundCreationService couponRoundCreationService;

    public CouponRoundReservationService(
            CouponTemplateRepository couponTemplateRepository,
            CouponRoundCreationService couponRoundCreationService
    ) {
        this.couponTemplateRepository = Objects.requireNonNull(
                couponTemplateRepository
        );
        this.couponRoundCreationService = Objects.requireNonNull(
                couponRoundCreationService
        );
    }

    public CouponRound reserve(CouponRoundReservationCommand command) {
        validateCommand(command);
        CouponTemplate template = couponTemplateRepository
                .findById(command.templateId())
                .orElseThrow(() -> new BusinessException(
                        CouponTemplateErrorCode.COUPON_TEMPLATE_NOT_FOUND,
                        "couponTemplateId=" + command.templateId()
                ));
        if (!template.active()) {
            throw new BusinessException(
                    CouponTemplateErrorCode.INVALID_COUPON_TEMPLATE,
                    "비활성 쿠폰 템플릿으로 회차를 예약할 수 없습니다."
            );
        }

        try {
            CouponRound couponRound = CouponRound.schedule(
                    template,
                    command.openAt(),
                    command.closeAt(),
                    command.generatedAt()
            );
            CouponStock initialStock = CouponStock.initialize(
                    template.stockPerOccurrence(),
                    command.generatedAt()
            );
            return couponRoundCreationService.create(
                    couponRound,
                    initialStock
            );
        } catch (IllegalArgumentException exception) {
            throw new BusinessException(
                    CouponRoundErrorCode.INVALID_COUPON_ROUND_SCHEDULE,
                    exception.getMessage(),
                    exception
            );
        }
    }

    private static void validateCommand(CouponRoundReservationCommand command) {
        if (command == null
                || command.templateId() == null
                || command.templateId() <= 0
                || command.openAt() == null
                || command.closeAt() == null
                || command.generatedAt() == null) {
            throw new BusinessException(
                    CouponRoundErrorCode.INVALID_COUPON_ROUND_SCHEDULE
            );
        }
        if (command.openAt().isBefore(command.generatedAt())) {
            throw new BusinessException(
                    CouponRoundErrorCode.INVALID_COUPON_ROUND_SCHEDULE,
                    "이미 지난 시각으로 쿠폰 회차를 예약할 수 없습니다."
            );
        }
    }
}
