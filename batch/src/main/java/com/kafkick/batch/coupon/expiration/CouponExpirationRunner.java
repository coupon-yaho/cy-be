// 만료 후보를 ID keyset으로 조회해 청크 안에서 회차별로 처리합니다.
package com.kafkick.batch.coupon.expiration;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.CouponExpirationCommand;
import com.kafkick.core.coupon.service.CouponExpirationResult;
import com.kafkick.core.support.TimeProvider;

@Component
public class CouponExpirationRunner {

    private static final long INITIAL_CURSOR = 0L;

    private final IssuanceRepository issuanceRepository;
    private final CouponExpirationTransactionExecutor transactionExecutor;
    private final TimeProvider timeProvider;
    private final CouponExpirationProperties properties;

    public CouponExpirationRunner(
            IssuanceRepository issuanceRepository,
            CouponExpirationTransactionExecutor transactionExecutor,
            TimeProvider timeProvider,
            CouponExpirationProperties properties
    ) {
        this.issuanceRepository = issuanceRepository;
        this.transactionExecutor = transactionExecutor;
        this.timeProvider = timeProvider;
        this.properties = properties;
    }

    public CouponExpirationBatchResult runOnce() {
        Instant asOf = timeProvider.instant().truncatedTo(ChronoUnit.MICROS);
        long cursor = INITIAL_CURSOR;
        int scannedCount = 0;
        int expiredCount = 0;

        while (true) {
            List<Issuance> candidates = issuanceRepository
                    .findExpiredIssuedAfterId(
                            asOf,
                            cursor,
                            properties.chunkSize()
                    );
            if (candidates.isEmpty()) {
                break;
            }

            scannedCount += candidates.size();
            cursor = candidates.get(candidates.size() - 1).id();
            for (Map.Entry<Long, List<Issuance>> entry
                    : groupByRound(candidates).entrySet()) {
                CouponExpirationResult result = transactionExecutor.execute(
                        new CouponExpirationCommand(
                                entry.getKey(),
                                entry.getValue(),
                                asOf
                        )
                );
                expiredCount += result.expiredCount();
            }

            if (candidates.size() < properties.chunkSize()) {
                break;
            }
        }
        return new CouponExpirationBatchResult(
                asOf,
                scannedCount,
                expiredCount
        );
    }

    private static Map<Long, List<Issuance>> groupByRound(
            List<Issuance> candidates
    ) {
        Map<Long, List<Issuance>> grouped = new LinkedHashMap<>();
        for (Issuance issuance : candidates) {
            grouped.computeIfAbsent(
                    issuance.couponRoundId(),
                    ignored -> new java.util.ArrayList<>()
            ).add(issuance);
        }
        return grouped;
    }
}
