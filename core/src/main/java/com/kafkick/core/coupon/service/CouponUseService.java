package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.domain.IssuanceUsage;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.port.IssuanceUsageRepository;
import com.kafkick.core.coupon.service.command.CouponUseCommand;
import com.kafkick.core.coupon.service.result.CouponUseResult;
import com.kafkick.core.support.exception.BusinessException;

@Service
public class CouponUseService {

    private final IssuanceRepository issuanceRepository;
    private final CouponRoundRepository couponRoundRepository;
    private final IssuanceUsageRepository issuanceUsageRepository;
    private final IssuanceHistoryRepository issuanceHistoryRepository;

    public CouponUseService(
            IssuanceRepository issuanceRepository,
            CouponRoundRepository couponRoundRepository,
            IssuanceUsageRepository issuanceUsageRepository,
            IssuanceHistoryRepository issuanceHistoryRepository
    ) {
        this.issuanceRepository = Objects.requireNonNull(
                issuanceRepository
        );
        this.couponRoundRepository = Objects.requireNonNull(
                couponRoundRepository
        );
        this.issuanceUsageRepository = Objects.requireNonNull(
                issuanceUsageRepository
        );
        this.issuanceHistoryRepository = Objects.requireNonNull(
                issuanceHistoryRepository
        );
    }

    @Transactional
    public CouponUseResult use(CouponUseCommand command) {
        validateCommand(command);
        Issuance issuance = issuanceRepository
                .findById(command.issuanceId())
                .orElseThrow(() -> new BusinessException(
                        CouponUseErrorCode.ISSUANCE_NOT_FOUND,
                        "issuanceId=" + command.issuanceId()
                ));
        validateOwner(issuance, command.memberId());

        CouponRound couponRound = couponRoundRepository
                .findById(issuance.couponRoundId())
                .orElseThrow(() -> new BusinessException(
                        CouponIssueErrorCode.COUPON_ROUND_NOT_FOUND,
                        "couponRoundId=" + issuance.couponRoundId()
                ));
        Issuance usedIssuance = issuance.use(command.usedAt());
        boolean changed = issuanceRepository.updateStatusIfCurrent(
                issuance.id(),
                issuance.memberId(),
                issuance.status(),
                usedIssuance.status(),
                command.usedAt()
        );
        if (!changed) {
            throw new BusinessException(
                    CouponIssueErrorCode.INVALID_TRANSITION,
                    "issuanceId=" + issuance.id()
            );
        }

        int discountAmount = calculateDiscount(
                couponRound,
                command.orderAmount()
        );
        IssuanceUsage savedUsage = issuanceUsageRepository.save(
                IssuanceUsage.use(
                        issuance.id(),
                        command.orderId(),
                        discountAmount,
                        command.usedAt()
                )
        );
        issuanceHistoryRepository.save(IssuanceHistory.use(
                issuance.id(),
                issuance.status(),
                issuance.expiresAt(),
                command.idempotencyKey(),
                command.usedAt()
        ));

        return new CouponUseResult(
                issuance.id(),
                usedIssuance.status(),
                savedUsage.orderId(),
                savedUsage.discountAmount(),
                savedUsage.usedAt()
        );
    }

    private static void validateCommand(CouponUseCommand command) {
        if (command == null
                || command.issuanceId() == null
                || command.issuanceId() <= 0
                || command.memberId() == null
                || command.memberId() <= 0
                || command.orderId() == null
                || command.orderId() <= 0
                || command.orderAmount() == null
                || command.orderAmount() <= 0
                || command.idempotencyKey() == null
                || command.usedAt() == null) {
            throw new BusinessException(
                    CouponUseErrorCode.INVALID_COUPON_USE_REQUEST
            );
        }
    }

    private static void validateOwner(Issuance issuance, Long memberId) {
        if (!issuance.memberId().equals(memberId)) {
            throw new BusinessException(
                    CouponUseErrorCode.NOT_COUPON_OWNER,
                    "issuanceId=" + issuance.id()
                            + ", memberId=" + memberId
            );
        }
    }

    private static int calculateDiscount(
            CouponRound couponRound,
            int orderAmount
    ) {
        return switch (couponRound.policyType()) {
            case PERCENT_CAPPED -> (int) Math.min(
                    (long) orderAmount * couponRound.discountRate() / 100,
                    couponRound.maxDiscountAmount()
            );
            case FIXED_AMOUNT -> Math.min(
                    orderAmount,
                    couponRound.discountAmount()
            );
        };
    }
}
