package com.kafkick.batch.coupon.expiration;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.port.CouponExpirationCandidateQueryPort;
import com.kafkick.core.coupon.service.command.CouponExpirationCommand;
import com.kafkick.core.coupon.service.result.CouponExpirationResult;
import com.kafkick.core.coupon.service.CouponExpirationService;
import com.kafkick.core.support.TimeProvider;

@Component
public class CouponExpirationRunner {

    private static final long INITIAL_CURSOR = 0L;

    private final CouponExpirationCandidateQueryPort candidateQueryPort;
    private final CouponExpirationService expirationService;
    private final TimeProvider timeProvider;
    private final CouponExpirationProperties properties;

    public CouponExpirationRunner(
            CouponExpirationCandidateQueryPort candidateQueryPort,
            CouponExpirationService expirationService,
            TimeProvider timeProvider,
            CouponExpirationProperties properties
    ) {
        this.candidateQueryPort = candidateQueryPort;
        this.expirationService = expirationService;
        this.timeProvider = timeProvider;
        this.properties = properties;
    }

    public CouponExpirationBatchResult runOnce() {
        Instant asOf = timeProvider.instant().truncatedTo(ChronoUnit.MICROS);
        long cursor = INITIAL_CURSOR;
        int scannedCount = 0;
        int expiredCount = 0;

        while (true) {
            List<Issuance> candidates = candidateQueryPort
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
                expiredCount += expireInTransactions(
                        entry.getKey(),
                        entry.getValue(),
                        asOf
                );
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

    private int expireInTransactions(
            Long couponRoundId,
            List<Issuance> issuances,
            Instant asOf
    ) {
        int expiredCount = 0;
        for (int fromIndex = 0;
             fromIndex < issuances.size();
             fromIndex += properties.transactionSize()) {
            int toIndex = Math.min(
                    fromIndex + properties.transactionSize(),
                    issuances.size()
            );
            CouponExpirationResult result = expirationService.expire(
                    new CouponExpirationCommand(
                            couponRoundId,
                            List.copyOf(issuances.subList(fromIndex, toIndex)),
                            asOf
                    )
            );
            expiredCount += result.expiredCount();
        }
        return expiredCount;
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
