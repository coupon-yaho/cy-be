package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.command.CouponCancelCommand;
import com.kafkick.core.coupon.service.result.CouponCancelResult;
import com.kafkick.core.support.exception.BusinessException;

@Service
public class CouponCancelService {

    private final IssuanceRepository issuanceRepository;
    private final IssuanceHistoryRepository issuanceHistoryRepository;
    private final CouponStockRepository couponStockRepository;

    public CouponCancelService(
            IssuanceRepository issuanceRepository,
            IssuanceHistoryRepository issuanceHistoryRepository,
            CouponStockRepository couponStockRepository
    ) {
        this.issuanceRepository = Objects.requireNonNull(issuanceRepository);
        this.issuanceHistoryRepository = Objects.requireNonNull(
                issuanceHistoryRepository
        );
        this.couponStockRepository = Objects.requireNonNull(
                couponStockRepository
        );
    }

    @Transactional
    public CouponCancelResult cancel(CouponCancelCommand command) {
        validateCommand(command);
        Issuance issuance = issuanceRepository
                .findById(command.issuanceId())
                .orElseThrow(() -> new BusinessException(
                        CouponUseErrorCode.ISSUANCE_NOT_FOUND,
                        "issuanceId=" + command.issuanceId()
                ));
        validateOwner(issuance, command.memberId());
        Issuance canceledIssuance = issuance.cancel(command.canceledAt());

        lockStock(issuance.couponRoundId());
        boolean statusChanged = issuanceRepository.updateStatusIfCurrent(
                issuance.id(),
                issuance.memberId(),
                issuance.status(),
                canceledIssuance.status(),
                command.canceledAt()
        );
        if (!statusChanged) {
            throw new BusinessException(
                    CouponIssueErrorCode.INVALID_TRANSITION,
                    "issuanceId=" + issuance.id()
            );
        }

        boolean stockReleased = couponStockRepository.release(
                issuance.couponRoundId(),
                1,
                command.canceledAt()
        );
        if (!stockReleased) {
            throw new BusinessException(
                    CouponUseErrorCode.COUPON_STOCK_RELEASE_FAILED,
                    "couponRoundId=" + issuance.couponRoundId()
            );
        }
        issuanceHistoryRepository.save(IssuanceHistory.cancel(
                issuance.id(),
                issuance.status(),
                command.idempotencyKey(),
                command.canceledAt()
        ));

        return new CouponCancelResult(
                issuance.id(),
                canceledIssuance.status(),
                command.canceledAt()
        );
    }

    private void lockStock(Long couponRoundId) {
        if (!couponStockRepository.lockForUpdate(couponRoundId)) {
            throw new BusinessException(
                    CouponUseErrorCode.COUPON_STOCK_RELEASE_FAILED,
                    "couponRoundId=" + couponRoundId
            );
        }
    }

    private static void validateCommand(CouponCancelCommand command) {
        if (command == null
                || command.issuanceId() == null
                || command.issuanceId() <= 0
                || command.memberId() == null
                || command.memberId() <= 0
                || command.idempotencyKey() == null
                || command.canceledAt() == null) {
            throw new BusinessException(
                    CouponUseErrorCode.INVALID_COUPON_CANCEL_REQUEST
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
}
