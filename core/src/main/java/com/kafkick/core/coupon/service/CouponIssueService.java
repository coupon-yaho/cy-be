package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponStockOccupationResult;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.code.CouponCodeGenerator;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.notification.NotificationRequestService;

@Service
public class CouponIssueService {

    private final CouponRoundRepository couponRoundRepository;
    private final IssuanceRepository issuanceRepository;
    private final CouponStockRepository couponStockRepository;
    private final IssuanceHistoryRepository issuanceHistoryRepository;
    private final CouponCodeGenerator couponCodeGenerator;
    private final NotificationRequestService notificationRequestService;

    public CouponIssueService(
            CouponRoundRepository couponRoundRepository,
            IssuanceRepository issuanceRepository,
            CouponStockRepository couponStockRepository,
            IssuanceHistoryRepository issuanceHistoryRepository,
            CouponCodeGenerator couponCodeGenerator,
            NotificationRequestService notificationRequestService
    ) {
        this.couponRoundRepository = Objects.requireNonNull(
                couponRoundRepository
        );
        this.issuanceRepository = Objects.requireNonNull(
                issuanceRepository
        );
        this.couponStockRepository = Objects.requireNonNull(
                couponStockRepository
        );
        this.issuanceHistoryRepository = Objects.requireNonNull(
                issuanceHistoryRepository
        );
        this.couponCodeGenerator = Objects.requireNonNull(
                couponCodeGenerator
        );
        this.notificationRequestService = Objects.requireNonNull(notificationRequestService);
    }

    @Transactional
    public Issuance issue(CouponIssueCommand command) {
        CouponIssuePolicy.validateCommand(command);
        CouponRound couponRound = couponRoundRepository
                .findById(command.couponRoundId())
                .orElseThrow(() -> new BusinessException(
                        CouponIssueErrorCode.COUPON_ROUND_NOT_FOUND,
                        "couponRoundId=" + command.couponRoundId()
                ));

        CouponIssuePolicy.validateIssuable(couponRound, command);
        if (!couponStockRepository.lockForUpdate(couponRound.id())) {
            throw new BusinessException(
                    CouponIssueErrorCode.COUPON_STOCK_NOT_FOUND,
                    "couponRoundId=" + couponRound.id()
            );
        }

        Issuance issuance = Issuance.issue(
                couponRound.id(),
                command.memberId(),
                couponCodeGenerator.generate(),
                command.membershipGrade(),
                couponRound.validDays(),
                command.issuedAt()
        );

        // 재고 행을 먼저 잠가 발급·취소·만료 경로의 잠금 순서를 통일한다.
        // 이후 발급건 선점과 재고 차감은 동일 트랜잭션에서 함께 커밋·롤백된다.
        Issuance savedIssuance = issuanceRepository.save(issuance);
        CouponStockOccupationResult occupationResult =
                couponStockRepository.occupyAfterLock(
                couponRound.id(),
                command.issuedAt()
        );
        validateStockOccupation(couponRound.id(), occupationResult);
        issuanceHistoryRepository.save(IssuanceHistory.issue(
                savedIssuance.id(),
                command.idempotencyKey(),
                command.issuedAt()
        ));
        notificationRequestService.request(savedIssuance);

        return savedIssuance;
    }

    private static void validateStockOccupation(
            Long couponRoundId,
            CouponStockOccupationResult occupationResult
    ) {
        if (occupationResult == CouponStockOccupationResult.SOLD_OUT) {
            throw new BusinessException(
                    CouponIssueErrorCode.SOLD_OUT,
                    "couponRoundId=" + couponRoundId
            );
        }
        if (occupationResult == CouponStockOccupationResult.NOT_FOUND) {
            throw new BusinessException(
                    CouponIssueErrorCode.COUPON_STOCK_NOT_FOUND,
                    "couponRoundId=" + couponRoundId
            );
        }
    }

}
