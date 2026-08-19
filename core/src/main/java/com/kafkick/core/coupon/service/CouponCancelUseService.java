// 사용 취소 상태 전이·실적 종료·만료 재고 복원·이력 저장을 조율합니다.
package com.kafkick.core.coupon.service;

import java.util.Objects;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.domain.IssuanceUsage;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.port.IssuanceUsageRepository;
import com.kafkick.core.support.exception.BusinessException;

public class CouponCancelUseService {

    private final IssuanceRepository issuanceRepository;
    private final IssuanceUsageRepository issuanceUsageRepository;
    private final IssuanceHistoryRepository issuanceHistoryRepository;
    private final CouponStockRepository couponStockRepository;

    public CouponCancelUseService(
            IssuanceRepository issuanceRepository,
            IssuanceUsageRepository issuanceUsageRepository,
            IssuanceHistoryRepository issuanceHistoryRepository,
            CouponStockRepository couponStockRepository
    ) {
        this.issuanceRepository = Objects.requireNonNull(issuanceRepository);
        this.issuanceUsageRepository = Objects.requireNonNull(
                issuanceUsageRepository
        );
        this.issuanceHistoryRepository = Objects.requireNonNull(
                issuanceHistoryRepository
        );
        this.couponStockRepository = Objects.requireNonNull(
                couponStockRepository
        );
    }

    public CouponCancelUseResult cancelUse(CouponCancelUseCommand command) {
        validateCommand(command);
        Issuance issuance = issuanceRepository
                .findById(command.issuanceId())
                .orElseThrow(() -> new BusinessException(
                        CouponUseErrorCode.ISSUANCE_NOT_FOUND,
                        "issuanceId=" + command.issuanceId()
                ));
        validateOwner(issuance, command.memberId());

        Issuance canceledIssuance = issuance.cancelUse(command.canceledAt());
        IssuanceUsage activeUsage = issuanceUsageRepository
                .findActiveByIssuanceId(issuance.id())
                .orElseThrow(() -> new BusinessException(
                        CouponUseErrorCode.ACTIVE_USAGE_NOT_FOUND,
                        "issuanceId=" + issuance.id()
                ));
        IssuanceUsage canceledUsage = activeUsage.cancel(
                command.canceledAt()
        );

        if (canceledIssuance.status() == IssuanceStatus.EXPIRED) {
            couponStockRepository.lockForUpdate(issuance.couponRoundId());
        }

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

        if (canceledIssuance.status() == IssuanceStatus.EXPIRED) {
            couponStockRepository.releaseOneAfterLock(
                    issuance.couponRoundId(),
                    command.canceledAt()
            );
        }

        boolean usageCanceled = issuanceUsageRepository.cancelIfActive(
                activeUsage.id(),
                command.canceledAt()
        );
        if (!usageCanceled) {
            throw new BusinessException(
                    CouponIssueErrorCode.INVALID_TRANSITION,
                    "issuanceId=" + issuance.id()
            );
        }

        issuanceHistoryRepository.save(IssuanceHistory.cancelUse(
                issuance.id(),
                issuance.status(),
                issuance.expiresAt(),
                command.idempotencyKey(),
                command.canceledAt()
        ));

        return new CouponCancelUseResult(
                issuance.id(),
                canceledIssuance.status(),
                canceledUsage.orderId(),
                canceledUsage.discountAmount(),
                canceledUsage.canceledAt()
        );
    }

    private static void validateCommand(CouponCancelUseCommand command) {
        if (command == null
                || command.issuanceId() == null
                || command.issuanceId() <= 0
                || command.memberId() == null
                || command.memberId() <= 0
                || command.idempotencyKey() == null
                || command.canceledAt() == null) {
            throw new BusinessException(
                    CouponUseErrorCode.INVALID_COUPON_CANCEL_USE_REQUEST
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
