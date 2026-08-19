// 회차별 만료 상태 전이·재고 복원·EXPIRE 이력 저장을 조율합니다.
package com.kafkick.core.coupon.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;

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

    public CouponExpirationResult expire(CouponExpirationCommand command) {
        validateCommand(command);
        if (command.issuances().isEmpty()) {
            return new CouponExpirationResult(0, 0);
        }

        List<Issuance> expiredIssuances = command.issuances().stream()
                .map(issuance -> issuance.expire(command.asOf()))
                .toList();
        couponStockRepository.lockForUpdate(command.couponRoundId());

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
            couponStockRepository.releaseAfterLock(
                    command.couponRoundId(),
                    histories.size(),
                    command.asOf()
            );
            issuanceHistoryRepository.saveAll(histories);
        }
        return new CouponExpirationResult(
                command.issuances().size(),
                histories.size()
        );
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
