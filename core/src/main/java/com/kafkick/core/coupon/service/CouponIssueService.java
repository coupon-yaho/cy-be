package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
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

@Service
public class CouponIssueService {

    private final CouponRoundRepository couponRoundRepository;
    private final IssuanceRepository issuanceRepository;
    private final CouponStockRepository couponStockRepository;
    private final IssuanceHistoryRepository issuanceHistoryRepository;
    private final CouponCodeGenerator couponCodeGenerator;

    public CouponIssueService(
            CouponRoundRepository couponRoundRepository,
            IssuanceRepository issuanceRepository,
            CouponStockRepository couponStockRepository,
            IssuanceHistoryRepository issuanceHistoryRepository,
            CouponCodeGenerator couponCodeGenerator
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
    }

    @Transactional
    public Issuance issue(CouponIssueCommand command) {
        validateCommand(command);
        CouponRound couponRound = couponRoundRepository
                .findById(command.couponRoundId())
                .orElseThrow(() -> new BusinessException(
                        CouponIssueErrorCode.COUPON_ROUND_NOT_FOUND,
                        "couponRoundId=" + command.couponRoundId()
                ));

        validateIssuable(couponRound, command);
        Issuance issuance = Issuance.issue(
                couponRound.id(),
                command.memberId(),
                couponCodeGenerator.generate(),
                command.membershipGrade(),
                couponRound.validDays(),
                command.issuedAt()
        );

        // 1인 1매 UNIQUE 선점과 ISSUE 이력을 먼저 저장한 뒤 재고 행을
        // 조건부 원자 UPDATE한다. 재고가 없으면 동일 트랜잭션이 전부 롤백된다.
        Issuance savedIssuance = issuanceRepository.save(issuance);
        issuanceHistoryRepository.save(IssuanceHistory.issue(
                savedIssuance.id(),
                command.idempotencyKey(),
                command.issuedAt()
        ));
        CouponStockOccupationResult occupationResult =
                couponStockRepository.occupyOne(
                        couponRound.id(),
                        command.issuedAt()
                );
        validateStockOccupation(couponRound.id(), occupationResult);

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

    private static void validateCommand(CouponIssueCommand command) {
        if (command == null
                || command.couponRoundId() == null
                || command.couponRoundId() <= 0
                || command.memberId() == null
                || command.memberId() <= 0
                || command.membershipGrade() == null
                || command.issuedAt() == null) {
            throw new BusinessException(
                    CouponIssueErrorCode.INVALID_COUPON_ISSUE_REQUEST
            );
        }
    }

    private static void validateIssuable(
            CouponRound couponRound,
            CouponIssueCommand command
    ) {
        Instant issuedAt = command.issuedAt();
        if (!issuedAt.isBefore(couponRound.closeAt())
                || couponRound.status() == CouponRoundStatus.CLOSED) {
            throw new BusinessException(
                    CouponIssueErrorCode.CAMPAIGN_CLOSED,
                    "couponRoundId=" + couponRound.id()
            );
        }
        if (issuedAt.isBefore(couponRound.openAt())
                || couponRound.status() != CouponRoundStatus.OPEN) {
            throw new BusinessException(
                    CouponIssueErrorCode.NOT_OPENED,
                    "couponRoundId=" + couponRound.id()
            );
        }
        if (!couponRound.eligibleGrades().contains(
                command.membershipGrade()
        )) {
            throw new BusinessException(
                    CouponIssueErrorCode.GRADE_NOT_ELIGIBLE,
                    "couponRoundId=" + couponRound.id()
                            + ", memberId=" + command.memberId()
            );
        }
    }
}
