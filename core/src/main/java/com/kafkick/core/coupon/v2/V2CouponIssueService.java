package com.kafkick.core.coupon.v2;

import java.util.Objects;
import java.util.Optional;

import org.springframework.transaction.support.TransactionOperations;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.code.CouponCodeGenerator;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.idempotency.IdempotencyKeys;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.v2.port.ClaimCommand;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;
import com.kafkick.core.coupon.v2.port.CompleteOutcome;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.EngineVersion;

public final class V2CouponIssueService {

    private final IssuanceGatePort gate;
    private final IssuanceRepository issuances;
    private final IssuanceHistoryRepository histories;
    private final IdempotencyRepository idempotencies;
    private final CouponCodeGenerator codeGenerator;
    private final IdempotencyResultCodec<CouponIssueResult> resultCodec;
    private final RequestTokenGenerator tokenGenerator;
    private final TransactionOperations transactions;

    public V2CouponIssueService(
            IssuanceGatePort gate,
            IssuanceRepository issuances,
            IssuanceHistoryRepository histories,
            IdempotencyRepository idempotencies,
            CouponCodeGenerator codeGenerator,
            IdempotencyResultCodec<CouponIssueResult> resultCodec,
            RequestTokenGenerator tokenGenerator,
            TransactionOperations transactions
    ) {
        this.gate = Objects.requireNonNull(gate, "gate");
        this.issuances = Objects.requireNonNull(issuances, "issuances");
        this.histories = Objects.requireNonNull(histories, "histories");
        this.idempotencies = Objects.requireNonNull(idempotencies, "idempotencies");
        this.codeGenerator = Objects.requireNonNull(codeGenerator, "codeGenerator");
        this.resultCodec = Objects.requireNonNull(resultCodec, "resultCodec");
        this.tokenGenerator = Objects.requireNonNull(tokenGenerator, "tokenGenerator");
        this.transactions = Objects.requireNonNull(transactions, "transactions");
    }

    public V2CouponIssueResult issue(
            CouponIssueCommand command,
            CouponRoundIssuanceDefinition definition
    ) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(definition, "definition");
        if (!Objects.equals(command.couponRoundId(), definition.couponRoundId())
                || definition.engineVersion() != EngineVersion.V2) {
            throw new IllegalArgumentException("V2 회차 정의와 발급 명령이 일치해야 합니다.");
        }

        String token = tokenGenerator.generate();
        ClaimResult claim;
        try {
            claim = gate.claim(new ClaimCommand(
                    command.couponRoundId(), command.memberId(),
                    command.membershipGrade().getBitValue(), command.idempotencyKey(), token));
        } catch (RuntimeException claimFailure) {
            throw new V2CouponIssueException(
                    claimFailure,
                    compensatePreserving(claimFailure, command, token),
                    Dependency.REDIS);
        }
        if (claim.outcome() == ClaimOutcome.REPLAY_DONE) {
            return replayDone(command, claim);
        }
        if (!claim.outcome().isClaimed()) {
            return V2CouponIssueResult.rejected(claim);
        }

        CouponIssueResult persisted;
        try {
            persisted = transactions.execute(status -> persist(command, definition));
        } catch (RuntimeException failure) {
            return resolveAfterFailure(command, claim, token, failure, Dependency.MYSQL);
        }
        try {
            CompleteOutcome completed = gate.complete(
                    command.couponRoundId(), command.memberId(), token);
            return new V2CouponIssueResult(claim, persisted, completed, false);
        } catch (RuntimeException failure) {
            return resolveAfterFailure(command, claim, token, failure, Dependency.REDIS);
        }
    }

    private V2CouponIssueResult replayDone(
            CouponIssueCommand command,
            ClaimResult claim
    ) {
        IdempotencyRecord record = idempotencies.findByKey(command.idempotencyKey())
                .orElseThrow(() -> new IllegalStateException(
                        "DONE 게이트에 대응하는 멱등 결과가 없습니다."));
        String expectedHash = IdempotencyKeys.hash(CouponIssueCommand.canonicalRequest(
                command.couponRoundId(), command.memberId(), command.membershipGrade()));
        if (record.status() != IdempotencyStatus.DONE
                || !Objects.equals(record.memberId(), command.memberId())
                || !record.requestHash().equals(expectedHash)) {
            throw new IllegalStateException("DONE 게이트와 멱등 결과가 일치하지 않습니다.");
        }
        return V2CouponIssueResult.replayed(
                claim,
                resultCodec.read(record.responseBody())
        );
    }

    private CouponIssueResult persist(
            CouponIssueCommand command,
            CouponRoundIssuanceDefinition definition
    ) {
        Issuance saved = issuances.save(Issuance.issue(
                command.couponRoundId(), command.memberId(), codeGenerator.generate(),
                command.membershipGrade(), definition.validDays(), command.issuedAt()));
        histories.save(IssuanceHistory.issue(
                saved.id(), command.idempotencyKey(), command.issuedAt()));
        CouponIssueResult result = CouponIssueResult.from(saved);
        boolean inserted = idempotencies.insertCompleted(
                command.idempotencyKey(), command.memberId(), saved.id(),
                IdempotencyKeys.hash(CouponIssueCommand.canonicalRequest(
                        command.couponRoundId(), command.memberId(), command.membershipGrade())),
                resultCodec.write(result), command.issuedAt());
        if (!inserted) {
            throw new IllegalStateException("완료된 멱등 레코드가 저장되지 않았습니다.");
        }
        return result;
    }

    private V2CouponIssueResult resolveAfterFailure(
            CouponIssueCommand command,
            ClaimResult claim,
            String token,
            RuntimeException failure,
            Dependency dependency
    ) {
        Optional<Issuance> committed;
        boolean matched;
        try {
            committed = issuances.findForCouponRoundMemberAndIdempotencyKey(
                    command.couponRoundId(), command.memberId(), command.idempotencyKey());
            // 멱등 레코드 조회도 같은 DB 왕복이다. 밖에 두면 같은 장애가
            // 호출 순서에 따라 다른 의존성으로 집계된다.
            matched = committed.isPresent()
                    && hasMatchingCompletedRecord(command, committed.get());
        } catch (RuntimeException requeryFailure) {
            suppress(failure, requeryFailure);
            // 재조회가 깨졌으므로 원 실패가 무엇이든 지금 막힌 것은 DB 다.
            throw new V2CouponIssueException(failure, null, Dependency.MYSQL);
        }
        if (committed.isPresent()) {
            if (!matched) {
                throw new V2CouponIssueException(failure, null, dependency);
            }
            CompleteOutcome completed;
            try {
                completed = gate.complete(
                        command.couponRoundId(), command.memberId(), token);
            } catch (RuntimeException retryFailure) {
                suppress(failure, retryFailure);
                // 발급은 이미 커밋됐다. 보상하면 DB 에 짝이 있는 재고를 되돌린다.
                throw new V2CouponIssueException(failure, null, Dependency.REDIS);
            }
            return new V2CouponIssueResult(
                    claim, CouponIssueResult.from(committed.get()), completed, true);
        }
        throw new V2CouponIssueException(
                failure,
                compensatePreserving(failure, command, token),
                dependency);
    }

    private boolean hasMatchingCompletedRecord(
            CouponIssueCommand command,
            Issuance committed
    ) {
        String expectedHash = IdempotencyKeys.hash(CouponIssueCommand.canonicalRequest(
                command.couponRoundId(), command.memberId(), command.membershipGrade()));
        return idempotencies.findByKey(command.idempotencyKey())
                .filter(record -> record.status() == IdempotencyStatus.DONE)
                .filter(record -> Objects.equals(record.memberId(), command.memberId()))
                .filter(record -> Objects.equals(record.issuanceId(), committed.id()))
                .filter(record -> Objects.equals(record.requestHash(), expectedHash))
                .isPresent();
    }

    /**
     * 후속 실패를 원 실패에 매단다. 같은 인스턴스면 {@code addSuppressed} 가
     * {@code IllegalArgumentException} 을 던져 원 실패를 통째로 덮어쓴다.
     */
    private static void suppress(RuntimeException original, RuntimeException additional) {
        if (original != additional) {
            original.addSuppressed(additional);
        }
    }

    private CompensateOutcome compensatePreserving(
            RuntimeException original,
            CouponIssueCommand command,
            String token
    ) {
        try {
            return gate.compensate(command.couponRoundId(), command.memberId(), token);
        } catch (RuntimeException compensationFailure) {
            suppress(original, compensationFailure);
            return null;
        }
    }
}
