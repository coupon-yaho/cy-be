package com.kafkick.core.coupon.v2;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionOperations;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.domain.CouponStockOccupationResult;
import com.kafkick.core.coupon.exception.CouponAlreadyIssuedException;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.CouponStockRepository;
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

    private static final Logger log = LoggerFactory.getLogger(V2CouponIssueService.class);
    /** 보상 실패 로그의 최소 간격. 장애 구간에 요청 수만큼 찍히는 것을 막는다. */
    private static final long COMPENSATION_LOG_INTERVAL_NANOS = Duration.ofSeconds(1).toNanos();

    /**
     * 마지막으로 보상 실패를 남긴 시각.
     *
     * <p><b>{@code Long.MIN_VALUE} 를 센티널로 쓰지 않는다.</b> {@code nanoTime()} 이 양수인
     * 흔한 환경에서 {@code now - Long.MIN_VALUE} 가 부호 있는 64비트를 넘겨 <b>음수로</b>
     * 접히고, 그러면 첫 호출부터 억제 조건에 걸려 이 로그가 영원히 안 찍힌다 — "한 창에 한
     * 줄" 이라는 약속이 조용히 깨진다. 한 창 전으로 앉혀 첫 호출이 반드시 통과하게 한다.
     */
    private final AtomicLong lastCompensationLogAt =
            new AtomicLong(System.nanoTime() - COMPENSATION_LOG_INTERVAL_NANOS);
    private final AtomicLong suppressedCompensationLogs = new AtomicLong();

    private final IssuanceGatePort gate;
    private final IssuanceRepository issuances;
    private final IssuanceHistoryRepository histories;
    private final IdempotencyRepository idempotencies;
    private final CouponStockRepository stocks;
    private final CouponCodeGenerator codeGenerator;
    private final IdempotencyResultCodec<CouponIssueResult> resultCodec;
    private final RequestTokenGenerator tokenGenerator;
    private final TransactionOperations transactions;

    public V2CouponIssueService(
            IssuanceGatePort gate,
            IssuanceRepository issuances,
            IssuanceHistoryRepository histories,
            IdempotencyRepository idempotencies,
            CouponStockRepository stocks,
            CouponCodeGenerator codeGenerator,
            IdempotencyResultCodec<CouponIssueResult> resultCodec,
            RequestTokenGenerator tokenGenerator,
            TransactionOperations transactions
    ) {
        this.gate = Objects.requireNonNull(gate, "gate");
        this.issuances = Objects.requireNonNull(issuances, "issuances");
        this.histories = Objects.requireNonNull(histories, "histories");
        this.idempotencies = Objects.requireNonNull(idempotencies, "idempotencies");
        this.stocks = Objects.requireNonNull(stocks, "stocks");
        this.codeGenerator = Objects.requireNonNull(codeGenerator, "codeGenerator");
        this.resultCodec = Objects.requireNonNull(resultCodec, "resultCodec");
        this.tokenGenerator = Objects.requireNonNull(tokenGenerator, "tokenGenerator");
        this.transactions = Objects.requireNonNull(transactions, "transactions");
    }

    /**
     * 게이트 선점 → 단일 트랜잭션 → 완료 CAS 로 v2 발급을 실행한다.
     *
     * <p>게이트의 거절은 예외가 아니라 결과다 — {@link V2CouponIssueResult#issueResult()} 가
     * 빈 값으로 돌아온다.
     *
     * <p>실패 경로는 선점 이후인지에 따라 다르다. <b>선점 자체가 실패하면</b> 재조회 없이
     * 보상을 시도하고 곧바로 예외다 — 아직 DB 에 아무것도 쓰지 않았다. <b>선점 이후</b>의
     * 발급 트랜잭션·완료 CAS 실패는 커밋 응답만 잃은 경우가 있어 (회차, 회원, 멱등키) 로
     * 재조회하고, 커밋된 발급과 완료된 멱등 레코드가 모두 확인되면 완료 CAS 를 다시 태워
     * <b>정상 결과로 돌려준다</b>({@link V2CouponIssueResult#recoveredAfterFailure()} 가 참).
     *
     * @throws IllegalArgumentException 명령의 회차와 정의의 회차가 다르거나 정의의 엔진이
     *     V2 가 아닐 때. 게이트를 호출하기 전에 중단한다
     * @throws V2CouponIssueException 선점 호출이 실패했을 때, 선점 이후의 실패를 위 재조회로
     *     복구하지 못했을 때, 또는 완료 CAS 가 비정상 결과를 냈을 때. 실패한 의존성과
     *     보상 CAS 결과를 함께 싣는다
     */
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
            if (claimFailure instanceof IssuanceGateCircuitOpenException) {
                // 차단기는 Lua를 보내지 않았다. 없는 선점을 보상하려고 Redis에 다시 붙으면 open의
                // 목적(워커를 즉시 풀기)을 스스로 무너뜨린다.
                throw new V2CouponIssueException(claimFailure, null, Dependency.REDIS, true);
            }
            throw new V2CouponIssueException(
                    claimFailure,
                    compensatePreserving(claimFailure, command, token),
                    Dependency.REDIS,
                    true);
        }
        if (claim.outcome() == ClaimOutcome.REPLAY_DONE) {
            return replayDone(command, claim);
        }
        if (!claim.outcome().isClaimed()) {
            return V2CouponIssueResult.rejected(claim);
        }

        Optional<CouponIssueResult> persisted;
        try {
            persisted = transactions.execute(status -> persist(command, definition));
        } catch (V2CouponStockSoldOutException soldOut) {
            return rejectAfterDatabaseSoldOut(command, token, soldOut);
        } catch (CouponAlreadyIssuedException duplicate) {
            // 게이트가 통과시킨 중복이다. **DB 가 판정을 확정했으므로 보상이 어떻게 끝났든
            // 응답은 409 다** — 기다려도 바뀌지 않는 사실에 재시도를 권하면 이미 쿠폰을 받은
            // 회원이 다시 누른다. 되돌아오지 않은 선점은 예외가 아니라 결과에 실어 관제로
            // 넘긴다. 게이트의 회원 집합이 DB 와 갈렸다는 신호라, 게이트가 스스로 거른
            // 중복과 같은 카운터에는 넣지 않는다.
            return V2CouponIssueResult.rejectedAfterDatabaseDuplicate(
                    compensatePreserving(duplicate, command, token));
        } catch (RuntimeException failure) {
            return resolveAfterFailure(command, claim, token, failure, Dependency.MYSQL);
        }
        CompleteOutcome completed;
        try {
            completed = gate.complete(
                    command.couponRoundId(), command.memberId(), token);
        } catch (RuntimeException failure) {
            return resolveAfterFailure(command, claim, token, failure, Dependency.REDIS);
        }
        // 검증은 catch 밖이다. 완료 CAS 이상은 재조회로 풀 수 있는 실패가 아니다.
        return V2CouponIssueResult.issued(claim, persisted.orElseThrow(), healthy(completed));
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

    private Optional<CouponIssueResult> persist(
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
        // Redis는 빠른 선점·중복 게이트일 뿐이다. Sentinel 승격이 마지막 DECR을 잃어도 이
        // 조건부 UPDATE가 총량을 넘기지 않으므로 DB가 최종 발급 권한이다. 반드시 마지막에
        // 둔다: 같은 재고 행 X-lock에 앞선 INSERT들을 묶으면 회차 전체가 직렬화된다.
        CouponStockOccupationResult occupation = stocks.occupyOne(
                command.couponRoundId(), command.issuedAt());
        if (occupation == CouponStockOccupationResult.SOLD_OUT) {
            throw new V2CouponStockSoldOutException(command.couponRoundId());
        }
        if (occupation != CouponStockOccupationResult.OCCUPIED) {
            throw new IllegalStateException("V2 발급 중 쿠폰 재고 행을 점유하지 못했습니다: " + occupation);
        }
        return Optional.of(result);
    }

    /**
     * DB 가 매진을 확정했다. <b>보상이 어떻게 끝났든 응답은 SOLD_OUT 이다.</b>
     *
     * <p>보상 실패를 예외로 올리면 차단기가 열린 구간의 매진 응답이 통째로
     * {@code REDIS_UNAVAILABLE}(503 + Retry-After) 로 바뀐다 — OPEN 상태의 보상 결과는
     * {@link CompensateOutcome#NOT_ATTEMPTED_CIRCUIT_OPEN} 이 <b>기본값</b>이라 예외 상황이
     * 아니라 정상 경로다. 그러면 클라이언트가 매진된 회차를 1초마다 무한 재시도하고,
     * 관제에는 매진이 0 건으로 보인다. 되돌아오지 않은 선점은 결과에 실어 누수 카운터로 넘긴다.
     */
    private V2CouponIssueResult rejectAfterDatabaseSoldOut(
            CouponIssueCommand command, String token, V2CouponStockSoldOutException soldOut) {
        return V2CouponIssueResult.rejectedAfterDatabaseSoldOut(
                compensatePreserving(soldOut, command, token));
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
            return V2CouponIssueResult.recovered(
                    claim, CouponIssueResult.from(committed.get()), healthy(completed));
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
     * 완료 CAS 의 정상 결과만 통과시킨다.
     *
     * <p>정상은 {@link CompleteOutcome#PROMOTED} 와 재시도끼리 겹친
     * {@link CompleteOutcome#ALREADY_DONE} 뿐이다. 나머지는 <b>DB 에 발급이 있는데 게이트는
     * 완료되지 않은</b> 불일치다 — 성공으로 반환하면 그 불일치가 200 응답에 은폐된다.
     * 발급이 이미 커밋됐으므로 보상하지 않고 상신한다.
     *
     * @throws V2CouponIssueException 완료 CAS 가 정상 결과를 내지 않았을 때
     */
    private static CompleteOutcome healthy(CompleteOutcome outcome) {
        if (outcome == CompleteOutcome.PROMOTED
                || outcome == CompleteOutcome.ALREADY_DONE) {
            return outcome;
        }
        throw new V2CouponIssueException(
                new IllegalStateException("완료 CAS 가 비정상 결과를 냈습니다: " + outcome),
                null,
                Dependency.REDIS);
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

    /**
     * 보상 실패를 <b>한 창에 한 줄만</b> 남긴다. 억제된 건수를 함께 실어, 로그만 보고
     * 사고 규모를 과소평가하지 않게 한다.
     *
     * <p>정확한 창 경계는 필요 없다 — 목적이 감사 로그가 아니라 원인 파악이라, 같은 장애
     * 구간에서 cause 하나와 대략의 건수면 충분하다. 그래서 락 없이 CAS 한 번으로 끝낸다.
     */
    private void logCompensationFailure(CouponIssueCommand command, RuntimeException failure) {
        if (!claimLogSlot(System.nanoTime())) {
            return;
        }
        long suppressed = suppressedCompensationLogs.getAndSet(0);
        log.warn("v2 보상 CAS 실패. couponRoundId={}, memberId={}, cause={}, 직전 창에서 생략={}",
                command.couponRoundId(), command.memberId(), failure.toString(), suppressed);
    }

    /**
     * 이 호출이 로그를 남길 차례인지. 아니면 생략 건수를 올린다.
     *
     * <p>시각을 인자로 받는다 — 창 경계를 테스트가 시계 없이 확인할 수 있어야 한다.
     * 이 판정이 조용히 틀리면 로그가 아예 안 나가고, 안 나가는 것은 "사고가 없다" 와
     * 구분되지 않는다.
     */
    boolean claimLogSlot(long now) {
        long previous = lastCompensationLogAt.get();
        if (now - previous < COMPENSATION_LOG_INTERVAL_NANOS
                || !lastCompensationLogAt.compareAndSet(previous, now)) {
            suppressedCompensationLogs.incrementAndGet();
            return false;
        }
        return true;
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
            // DB 가 판정을 확정한 경로에서는 원 예외가 던져지지 않고 버려진다 — 여기서
            // 남기지 않으면 왜 깨졌는지가 저장소 어디에도 없다.
            //
            // ⚠️ **건당 남기지 않는다.** 이 자리는 failover 구간에 초당 수천 건이 되고 이
            // 저장소에는 logback 설정이 없어 동기 콘솔 appender 다 — 스택을 빼도 단문이
            // 요청 수만큼 쌓이면 로그 I/O 가 락 하나에 직렬화되어 응답 지연을 밀어 올리고,
            // 그 지연이 이 작업이 재려는 failover 복구 시간에 그대로 들어간다. 원인은 한
            // 창에 한 줄이면 충분하고(같은 장애면 cause 가 같다), 건수는 세 미터
            // (claim.leaked·compensation.no.claim·compensation.already.done)가 센다.
            // 억제한 건수도 함께 남겨 로그만 보고 "한 건이었다"로 읽지 않게 한다.
            // 회차·회원 id 는 내부 식별자라 마스킹 대상이 아니다.
            logCompensationFailure(command, compensationFailure);
            // null 로 접지 않는다. null 은 "보상하지 않기로 했다"는 결정이고 이쪽은
            // "보상이 깨졌다"라 선점의 행방을 모른다 — 응답 분류가 갈려야 한다.
            return CompensateOutcome.ATTEMPT_FAILED;
        }
    }
}
