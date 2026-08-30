package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
import com.kafkick.core.coupon.service.command.CouponCancelUseCommand;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;
import com.kafkick.core.coupon.v2.V2StockRestorationService;
import com.kafkick.core.support.exception.BusinessException;

@Service
public class CouponCancelUseService {

    private final IssuanceRepository issuanceRepository;
    private final IssuanceUsageRepository issuanceUsageRepository;
    private final IssuanceHistoryRepository issuanceHistoryRepository;
    private final CouponStockRepository couponStockRepository;
    private final V2StockRestorationService v2StockRestorationService;

    public CouponCancelUseService(
            IssuanceRepository issuanceRepository,
            IssuanceUsageRepository issuanceUsageRepository,
            IssuanceHistoryRepository issuanceHistoryRepository,
            CouponStockRepository couponStockRepository,
            V2StockRestorationService v2StockRestorationService
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
        this.v2StockRestorationService = Objects.requireNonNull(v2StockRestorationService);
    }

    @Transactional
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

        // 재고 행 X 락은 커밋까지 유지된다. 취소·만료와 같은 순서로 마지막에 잡는다(§9.6 D9).
        if (canceledIssuance.status() == IssuanceStatus.EXPIRED) {
            // 엔진 판별의 coupons 왕복을 재고 행 X 락 밖에 둔다(§9.6 D9).
            v2StockRestorationService.restoreAfterCommit(issuance.couponRoundId(), 1);
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
        }

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
