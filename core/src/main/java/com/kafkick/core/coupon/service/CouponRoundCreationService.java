package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.coupon.exception.CouponRoundAlreadyExistsException;
import com.kafkick.core.coupon.exception.CouponRoundScheduleConflictException;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponRoundScheduleLockPort;

@Service
public class CouponRoundCreationService {

    private final CouponRoundRepository couponRoundRepository;
    private final CouponRoundScheduleLockPort scheduleLockPort;

    public CouponRoundCreationService(
            CouponRoundRepository couponRoundRepository,
            CouponRoundScheduleLockPort scheduleLockPort
    ) {
        this.couponRoundRepository = Objects.requireNonNull(
                couponRoundRepository
        );
        this.scheduleLockPort = Objects.requireNonNull(scheduleLockPort);
    }

    @Transactional
    public CouponRound create(
            CouponRound couponRound,
            CouponStock initialStock
    ) {
        Objects.requireNonNull(couponRound);
        Objects.requireNonNull(initialStock);
        scheduleLockPort.lock();
        if (couponRoundRepository.existsByTemplateIdAndOpenAt(
                couponRound.templateId(),
                couponRound.openAt()
        )) {
            throw new CouponRoundAlreadyExistsException(
                    "동일 템플릿과 오픈 시각의 쿠폰 회차가 이미 존재합니다.",
                    null
            );
        }
        if (couponRoundRepository.existsOverlappingSchedule(
                couponRound.openAt(),
                couponRound.closeAt()
        )) {
            throw new CouponRoundScheduleConflictException(
                    "다른 쿠폰 회차와 예약 시간이 겹칩니다."
            );
        }
        return couponRoundRepository.saveWithInitialStock(
                couponRound,
                initialStock
        );
    }
}
