package com.kafkick.core.coupon.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.exception.CouponExpirationErrorCode;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.command.CouponExpirationCommand;
import com.kafkick.core.coupon.service.result.CouponExpirationResult;
import com.kafkick.core.support.exception.BusinessException;

@Service
public class CouponExpirationService {

    private final IssuanceRepository issuanceRepository;
    private final IssuanceHistoryRepository issuanceHistoryRepository;
    private final CouponStockRepository couponStockRepository;

    public CouponExpirationService(
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
    public CouponExpirationResult expire(CouponExpirationCommand command) {
        validateCommand(command);
        if (command.issuances().isEmpty()) {
            return new CouponExpirationResult(0, 0);
        }

        List<Issuance> expiredIssuances = command.issuances().stream()
                .map(issuance -> issuance.expire(command.asOf()))
                .toList();
        lockStock(command.couponRoundId());
        List<IssuanceHistory> histories = new ArrayList<>();
        for (int index = 0; index < command.issuances().size(); index++) {
            Issuance issuance = command.issuances().get(index);
            Issuance expired = expiredIssuances.get(index);
            boolean statusChanged = issuanceRepository.updateStatusIfCurrent(
                    issuance.id(),
                    issuance.memberId(),
                    issuance.status(),
                    expired.status(),
                    command.asOf()
            );
            if (statusChanged) {
                histories.add(IssuanceHistory.expire(
                        issuance.id(),
                        issuance.status(),
                        issuance.expiresAt(),
                        command.asOf()
                ));
            }
        }

        if (!histories.isEmpty()) {
            boolean stockReleased = couponStockRepository.release(
                    command.couponRoundId(),
                    histories.size(),
                    command.asOf()
            );
            if (!stockReleased) {
                throw new BusinessException(
                        CouponExpirationErrorCode
                                .COUPON_EXPIRATION_SAVE_FAILED,
                        "couponRoundId=" + command.couponRoundId()
                );
            }
            issuanceHistoryRepository.saveAllExpirations(histories);
        }
        return new CouponExpirationResult(
                command.issuances().size(),
                histories.size()
        );
    }

    private void lockStock(Long couponRoundId) {
        if (!couponStockRepository.lockForUpdate(couponRoundId)) {
            throw new BusinessException(
                    CouponExpirationErrorCode.COUPON_EXPIRATION_SAVE_FAILED,
                    "couponRoundId=" + couponRoundId
            );
        }
    }

    private static void validateCommand(CouponExpirationCommand command) {
        if (command == null
                || command.couponRoundId() == null
                || command.couponRoundId() <= 0
                || command.issuances() == null
                || command.asOf() == null) {
            throw new IllegalArgumentException(
                    "쿠폰 만료 처리 요청 값이 올바르지 않습니다."
            );
        }
        for (Issuance issuance : command.issuances()) {
            if (issuance == null
                    || !command.couponRoundId().equals(
                            issuance.couponRoundId()
                    )) {
                throw new IllegalArgumentException(
                        "동일 회차의 쿠폰만 함께 만료 처리할 수 있습니다."
                );
            }
        }
    }
}
