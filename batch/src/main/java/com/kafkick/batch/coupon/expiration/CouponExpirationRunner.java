package com.kafkick.batch.coupon.expiration;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.port.CouponExpirationCandidateQueryPort;
import com.kafkick.core.coupon.service.command.CouponExpirationCommand;
import com.kafkick.core.coupon.service.result.CouponExpirationResult;
import com.kafkick.core.coupon.service.CouponExpirationService;
import com.kafkick.core.coupon.service.V2StockRestorationService;
import org.springframework.dao.DataAccessException;

import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

@Component
public class CouponExpirationRunner {

    private static final Logger log = LoggerFactory.getLogger(CouponExpirationRunner.class);

    private static final long INITIAL_CURSOR = 0L;

    private final CouponExpirationCandidateQueryPort candidateQueryPort;
    private final CouponExpirationService expirationService;
    private final V2StockRestorationService stockRestorationService;
    private final TimeProvider timeProvider;
    private final CouponExpirationProperties properties;

    public CouponExpirationRunner(
            CouponExpirationCandidateQueryPort candidateQueryPort,
            CouponExpirationService expirationService,
            V2StockRestorationService stockRestorationService,
            TimeProvider timeProvider,
            CouponExpirationProperties properties
    ) {
        this.candidateQueryPort = candidateQueryPort;
        this.expirationService = expirationService;
        this.stockRestorationService = stockRestorationService;
        this.timeProvider = timeProvider;
        this.properties = properties;
    }

    public CouponExpirationBatchResult runOnce() {
        // 표식 쓰기가 실패했던 회차의 폴백은 이번 실행에서 걷는다. 실행 사이에 재구성이
        // 끼면 그 폴백은 낡은 답이고, 들고 가면 재구성이 푼 회차를 배치가 다시 세운다.
        stockRestorationService.beginExpirationRun();
        Instant asOf = timeProvider.instant().truncatedTo(ChronoUnit.MICROS);
        long cursor = INITIAL_CURSOR;
        int scannedCount = 0;
        int expiredCount = 0;
        Set<Long> haltedRoundIds = new LinkedHashSet<>();
        Set<Long> failedRoundIds = new LinkedHashSet<>();

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
                if (haltedRoundIds.contains(entry.getKey())
                        || failedRoundIds.contains(entry.getKey())) {
                    // 이번 실행에서 이미 멈췄거나 실패한 회차다. 뒤 페이지의 같은 회차 건은
                    // 트랜잭션을 열어 볼 것도 없다. 실패를 안 거르면 락 대기 같은 지속형
                    // 장애에서 그 회차가 페이지 수만큼 트랜잭션을 다시 열어 실패하고,
                    // 뒤 회차가 밀리지 않게 하려던 격리가 정확히 뒤집힌다.
                    continue;
                }
                // 회차 단위로 격리한다. 한 회차의 데드락·타임아웃이 틱 전체를 죽이면 뒤
                // 회차의 만료가 통째로 밀리고, 중단 회차 경보까지 못 찍혀 무슨 일이
                // 있었는지도 안 남는다.
                //
                // 잡는 것은 DB 장애와 업무 예외뿐이다. 조립 오류(게이트·표식 저장소 부재
                // 등)는 회차 하나의 문제가 아니라 전 회차 공통 사고라 삼키면 매 틱 모든
                // 회차가 조용히 실패하고 스케줄러는 expired=0 을 info 로 남긴다 —
                // 그건 정상과 구별되지 않는다. 그대로 전파해 틱을 죽인다.
                int[] committed = new int[1];
                try {
                    expireInTransactions(
                            entry.getKey(),
                            entry.getValue(),
                            asOf,
                            haltedRoundIds,
                            committed
                    );
                } catch (DataAccessException | BusinessException failure) {
                    failedRoundIds.add(entry.getKey());
                    log.error("회차 만료에 실패해 건너뜁니다. couponRoundId={}, 대상={}",
                            entry.getKey(), entry.getValue().size(), failure);
                } finally {
                    // 실패 전에 커밋된 청크는 이미 DB 에 남아 있다. 지역변수째 버리면
                    // 실제로 걷힌 건수가 0 으로 보고돼 관제 수치가 과소가 된다.
                    expiredCount += committed[0];
                }
            }

            if (candidates.size() < properties.chunkSize()) {
                break;
            }
        }
        return new CouponExpirationBatchResult(
                asOf,
                scannedCount,
                expiredCount,
                List.copyOf(haltedRoundIds),
                List.copyOf(failedRoundIds)
        );
    }

    private void expireInTransactions(
            Long couponRoundId,
            List<Issuance> issuances,
            Instant asOf,
            Set<Long> haltedRoundIds,
            int[] committed
    ) {
        for (int fromIndex = 0;
             fromIndex < issuances.size();
             fromIndex += properties.transactionSize()) {
            // 앞 트랜잭션의 afterCommit 에서 복원이 상한 초과로 거절됐으면 이 회차는
            // 재고가 이미 틀어져 있다. 계속 돌리면 DB 만 EXPIRED 로 전이되고 Redis 는
            // 하나도 안 받아 어긋남이 청크마다 커진다. 재동기화가 회수한다(06).
            if (stockRestorationService.isRestorationHalted(couponRoundId)) {
                if (haltedRoundIds.add(couponRoundId)) {
                    log.error("V2 재고 복원이 상한 초과로 거절돼 이 회차의 만료를 중단합니다. "
                            + "수동 재동기화가 필요합니다. couponRoundId={}, 남은 대상={}",
                            couponRoundId, issuances.size() - fromIndex);
                }
                break;
            }
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
            committed[0] += result.expiredCount();
        }
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
