package com.kafkick.api.observation.issuance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Objects;
import java.util.Optional;

import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DataAccessException;

import com.kafkick.api.observation.ObservationIssuanceProperties;
import com.kafkick.api.support.RetryAfterException;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponIssueV2ErrorCode;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;
import com.kafkick.core.coupon.service.idempotency.IdempotencyKeys;
import com.kafkick.core.coupon.service.result.CouponIssueExecutionResult;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.v2.CouponIssuanceRouter;
import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.coupon.v2.V2CouponIssueResult;
import com.kafkick.core.coupon.v2.V2CouponIssueService;
import com.kafkick.core.coupon.v2.V2CouponIssueException;
import com.kafkick.core.coupon.v2.IssuanceGateCircuitOpenException;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.EngineVersion;

/** 쿠폰 발급 업무 결과를 바꾸지 않고 요청 단위 관측 수명주기를 연결합니다. */
@Component
public final class CouponIssueObservationCoordinator {

    private static final Logger log =
            LoggerFactory.getLogger(CouponIssueObservationCoordinator.class);

    /** 원인 사슬을 훑는 깊이 상한. 순환 사슬에서 요청 스레드가 갇히지 않게 한다. */
    private static final int CAUSE_CHAIN_LIMIT = 16;

    /** 다시 시도하기 전 물러서는 시간. 진 쪽들이 같은 순간에 되돌아와 다시 부딪히지 않게 한다. */
    private static final long BACKOFF_MIN_NANOS = 1_000_000L;
    private static final long BACKOFF_MAX_NANOS = 5_000_000L;

    private final CouponOperationExecutionService operationExecutionService;
    private final IssuanceObservationContextFactory contextFactory;
    private final IssuanceObservationService observationService;
    private final CouponIssueObservationDependencyMapper dependencyMapper;
    private final CouponIssuanceRouter router;
    private final ObjectProvider<V2CouponIssueService> v2Services;
    private final V2IssuanceOutcomeMeters v2Meters;
    private final IssueLockRetryMeters lockRetryMeters;
    private final IssueLockRetryProperties lockRetryProperties;
    private final ObservationIssuanceProperties issuanceProperties;
    private final TimeProvider timeProvider;

    /**
     * 발급 실행기와 관측 Context·Session 경계를 조립합니다.
     *
     * @param operationExecutionService 멱등·권위 발급 실행기
     * @param contextFactory 요청별 관측 Context 생성기
     * @param observationService attempt와 Session 기록 서비스
     * @param dependencyMapper 발급 실패 관측 분류기
     * @param router 회차별 발급 엔진 라우터
     * @param v2Services 게이트가 있을 때만 존재하는 v2 발급 서비스
     * @param v2Meters v2 발급 결과 카운터 아홉 종
     * @param issuanceProperties {@code Retry-After} 초를 포함한 발급 관측 임계치
     * @param timeProvider 발급 시각 공급자
     */
    public CouponIssueObservationCoordinator(
            CouponOperationExecutionService operationExecutionService,
            IssuanceObservationContextFactory contextFactory,
            IssuanceObservationService observationService,
            CouponIssueObservationDependencyMapper dependencyMapper,
            CouponIssuanceRouter router,
            ObjectProvider<V2CouponIssueService> v2Services,
            V2IssuanceOutcomeMeters v2Meters,
            IssueLockRetryMeters lockRetryMeters,
            IssueLockRetryProperties lockRetryProperties,
            ObservationIssuanceProperties issuanceProperties,
            TimeProvider timeProvider
    ) {
        this.operationExecutionService = Objects.requireNonNull(operationExecutionService);
        this.contextFactory = Objects.requireNonNull(contextFactory);
        this.observationService = Objects.requireNonNull(observationService);
        this.dependencyMapper = Objects.requireNonNull(dependencyMapper);
        this.router = Objects.requireNonNull(router);
        this.v2Services = Objects.requireNonNull(v2Services);
        this.v2Meters = Objects.requireNonNull(v2Meters);
        this.lockRetryMeters = Objects.requireNonNull(lockRetryMeters, "lockRetryMeters");
        this.lockRetryProperties = Objects.requireNonNull(lockRetryProperties, "lockRetryProperties");
        this.issuanceProperties = Objects.requireNonNull(issuanceProperties);
        this.timeProvider = Objects.requireNonNull(timeProvider);
    }

    /**
     * 실제 발급 실행과 ISSUE_ATTEMPT·ISSUE_RESULT 기록을 한 요청 수명주기로 조정합니다.
     *
     * <p>Context나 기록기의 실패는 모두 격리합니다. 신규 실행은 Core의 정책 통과 callback에서
     * attempt를 기록하고 결과를 한 번 완료합니다. DONE replay는 정책과 발급을 다시 실행하지 않고
     * replayed attempt만 기록합니다.
     *
     * @param requestId 요청 필터가 확정한 요청 식별자
     * @param couponRoundId 쿠폰 회차 식별자
     * @param memberId 회원 식별자
     * @param membershipGrade 요청 시점 회원 등급
     * @param idempotencyKey UUID v4 멱등 키
     * @return 기존 쿠폰 발급 결과
     */
    public CouponIssueResult issue(
            String requestId,
            Long couponRoundId,
            Long memberId,
            MembershipGrade membershipGrade,
            String idempotencyKey
    ) {
        return router.route(
                couponRoundId,
                definition -> issueV1(requestId, couponRoundId, memberId,
                        membershipGrade, idempotencyKey, definition.engineVersion()),
                definition -> issueV2(requestId, couponRoundId, memberId,
                        membershipGrade, idempotencyKey, definition)
        );
    }

    /**
     * <b>락 경합으로 물러선 발급을 다시 시도한다.</b>
     *
     * <p><b>실측했다.</b> 동시 발급 테스트에서 이 겹을 빼고 여섯 번 돌리면 한 번 실패한다 —
     * 같은 회차에 발급이 몰리면 MySQL 이 한쪽을 데드락으로 걷어낸다. 다시 시도하지 않으면
     * 그 요청은 사용자에게 500 이고, 부하 회차의 에러율에 그대로 얹힌다.
     *
     * <p><b>다시 시도해도 안전한 이유는 트랜잭션이 하나이기 때문이다.</b>
     * 발급·이력·재고·멱등 기록이 한 트랜잭션이라 데드락 롤백이 넷을 전부 되돌린다. 멱등
     * 기록도 안 남으므로 다시 시도해도 <i>"이미 처리된 키"</i> 에 막히지 않고 중복 발급도
     * 생기지 않는다.
     *
     * <h2>왜 core 가 아니라 여기인가</h2>
     *
     * <p>{@code CannotAcquireLockException} 은 {@code org.springframework.dao} 이고,
     * <b>core 는 그 패키지를 참조할 수 없다</b>({@code CoreArchitectureTest} 가 전용 검사로
     * 막는다). 락 경합은 도메인 규칙이 아니라 인프라 실패 모드라 그 경계가 맞다.
     *
     * <p>관측 범위는 시도마다 열지 않는다. 사용자는 요청을 한 번 했고, 이 값은 그 요청의
     * 체감 시간이다. 다시 시도한 사실은 아래 경고 로그에 남는다.
     *
     * <p>V2 는 대상이 아니다 — 경합이 Redis 에서 먼저 걸러지고 DB 쓰기가 짧다. 거기서도
     * 같은 것이 관측되면 그때 넓힌다. 안 재고 넓히지 않는다.
     */
    private CouponIssueExecutionResult issueRetryingOnLockContention(
            Long couponRoundId,
            Long memberId,
            MembershipGrade membershipGrade,
            String idempotencyKey,
            ObservationScope observation
    ) {
        int maxAttempts = lockRetryProperties.maxAttempts();
        long deadline = System.nanoTime() + lockRetryProperties.budget().toNanos();
        for (int attempt = 1; ; attempt++) {
            try {
                return operationExecutionService.issueWithMetadata(
                        couponRoundId,
                        memberId,
                        membershipGrade,
                        idempotencyKey,
                        observation::recordClaimedAttempt
                );
            } catch (RuntimeException failure) {
                if (!causedByLockContention(failure)) {
                    throw failure;
                }
                if (attempt >= maxAttempts || System.nanoTime() >= deadline) {
                    // 상한까지 갔다는 사실만 남긴다. 원인이 반복 데드락인지 지속 병목인지는
                    // 여기서 구분하지 못한다 — 그 판정은 이 로그와 지표를 본 사람이 한다.
                    lockRetryMeters.exhausted();
                    log.warn("발급이 락 경합으로 {}회 만에 포기했습니다. couponRoundId={} memberId={}",
                            attempt, couponRoundId, memberId);
                    // 바깥 예외를 그대로 보존한다. 어댑터가 붙인 맥락을 벗기지 않는다.
                    throw failure;
                }
                lockRetryMeters.recovered();
                // 부하 구간에 요청마다 warn 을 찍으면 로그 I/O 가 측정값을 오염시킨다.
                log.debug("발급이 락 경합으로 물러섭니다. attempt={}/{} couponRoundId={} memberId={}",
                        attempt, maxAttempts, couponRoundId, memberId);
                LockSupport.parkNanos(ThreadLocalRandom.current()
                        .nextLong(BACKOFF_MIN_NANOS, BACKOFF_MAX_NANOS));
            }
        }
    }

    /**
     * <b>원인 사슬을 훑는다.</b> 저장소 어댑터가 {@code DataAccessException} 을
     * {@code CouponPersistenceException}·{@code IdempotencyPersistenceException} 으로 감싸므로,
     * <b>운영에서 오는 락 경합은 대개 그 안에 들어 있다.</b> 처음에는 원본 타입만 잡았는데
     * 그러면 감싸인 쪽이 안 걸려서 재시도가 사실상 안 돌았다(리뷰가 잡았다).
     *
     * <p>감싸이지 않은 경우도 있다 — JPA 가 INSERT 를 커밋 시점으로 미루면 어댑터의
     * {@code catch} 밖에서 터진다. 그래서 <b>양쪽을 다 본다.</b>
     *
     * <p>락 경합이 아닌 실패는 여기서 {@code false} 라 곧바로 원형 그대로 다시 던져진다.
     * 넓게 잡아 아무 실패나 재시도하면 진짜 결함을 세 번 반복하고 응답만 느려진다.
     */
    private static boolean causedByLockContention(Throwable failure) {
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < CAUSE_CHAIN_LIMIT; depth++) {
            if (cause instanceof PessimisticLockingFailureException) {
                return true;
            }
            if (cause.getCause() == cause) {
                return false;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private CouponIssueResult issueV1(
            String requestId,
            Long couponRoundId,
            Long memberId,
            MembershipGrade membershipGrade,
            String idempotencyKey,
            EngineVersion engineVersion
    ) {
        ObservationScope observation = openObservation(
                requestId,
                memberId,
                couponRoundId,
                membershipGrade,
                engineVersion
        );
        try {
            CouponIssueExecutionResult execution = issueRetryingOnLockContention(
                    couponRoundId, memberId, membershipGrade, idempotencyKey, observation);
            if (execution.replayed()) {
                observation.recordReplayAttempt();
            } else {
                observation.completeIssued(execution.result());
            }
            return execution.result();
        } catch (RuntimeException failure) {
            completeFailure(observation, failure);
            throw failure;
        } finally {
            observation.finish();
        }
    }

    /**
     * 게이트 통신 실패는 요청 안에서 재시도하지 않는다. 명령 timeout 뒤에도 Redis 스크립트는
     * 선점을 끝낼 수 있어 새 requestToken으로 다시 보내면 완료·보상 CAS가 앞선 선점과 갈린다.
     */
    private RuntimeException redisUnavailable(RuntimeException failure) {
        if (failure instanceof V2CouponIssueException gateFailure
                && (transportFailedBeforeClaim(gateFailure)
                        || compensationLeftTheClaimUnresolved(gateFailure))) {
            return new RetryAfterException(
                    CouponIssueV2ErrorCode.REDIS_UNAVAILABLE,
                    issuanceProperties.redisUnavailableRetryAfterSeconds(), gateFailure);
        }
        return failure;
    }

    /** 선점 결과조차 받지 못한 통신·차단기 실패. 게이트 상태를 모르므로 서버가 되돌릴 수 없다. */
    private static boolean transportFailedBeforeClaim(V2CouponIssueException gateFailure) {
        return gateFailure.dependency() == Dependency.REDIS
                && gateFailure.claimFailedBeforeResult()
                && (gateFailure.getCause() instanceof DataAccessException
                        || gateFailure.getCause() instanceof IssuanceGateCircuitOpenException);
    }

    /**
     * 보상이 선점을 정리했다고 확언하지 못한 결과. 셋 모두 <b>이 요청의 선점이 어떻게 됐는지
     * 서버가 모른다</b>는 뜻이라 재시도를 권한다 — 되돌아온 것이 확실한 {@code REVERTED} 나,
     * DB 가 이미 판정을 확정한 1인 다매·매진 거절은 여기로 오지 않는다.
     *
     * <p><b>재고 사실({@link #leftClaimBehind})에서 파생시키고, 정책 차이만 따로 뺀다.</b>
     * 두 자리가 각자 값 목록을 들면 같은 값을 반대로 읽는 사고가 난다 — 실제로 이 경로에서
     * 반복됐다. 그래서 목록은 하나뿐이고, 여기서는 그 사실에 <b>이름 있는 예외 하나</b>만
     * 얹는다: {@code BAD_ARGUMENT} 는 선점을 남기지만 <b>호출부 버그</b>라 다시 눌러도 같은
     * 실패다({@code CouponIssueV2ErrorCode.BAD_ARGUMENT} 가 이미 {@link Dependency#NONE} 으로
     * 같은 판단을 했다). 남은 선점은 {@code claim.leaked} 가 세고, 응답만 재시도를 권하지 않는다.
     *
     * <p><b>의존성으로 거르지 않는다.</b> 보상은 정의상 항상 Redis 왕복이라, DB 실패로 들어온
     * 요청({@link Dependency#MYSQL})의 보상이 깨진 것도 똑같이 선점 행방을 모르는 상태다.
     * 여기서 의존성을 요구하면 그 경로만 재시도 안내 없는 500 으로 새어 나간다.
     *
     * <p><b>보상 결과가 아예 없는 경우({@code Optional.empty})는 여기 넣지 않는다.</b> 그것은
     * 발급이 이미 커밋됐거나 완료 CAS 가 비정상이라 <b>보상하지 않기로 한</b> 경로다 —
     * 재시도를 권하면 쿠폰을 이미 받은 클라이언트가 다시 누른다. 보상이 깨진 경우는
     * {@link CompensateOutcome#ATTEMPT_FAILED} 로 따로 온다.
     */
    private static boolean compensationLeftTheClaimUnresolved(V2CouponIssueException gateFailure) {
        return gateFailure.compensateOutcome()
                .filter(CouponIssueObservationCoordinator::leftClaimBehind)
                .filter(outcome -> outcome != CompensateOutcome.BAD_ARGUMENT)
                .isPresent();
    }

    /**
     * V2 회차의 발급을 실행합니다.
     *
     * <p><b>멱등키 형식을 게이트보다 먼저 봅니다.</b> 재구성은 원래 멱등키를 복원할 수 없어
     * {@code issued} 값에 마커 문자열을 적어 두는데(설계 §4.3 반려표), 그 값은 문서에 공개돼
     * 있습니다. 클라이언트가 그대로 보내면 Lua 의 멱등키 전체 비교가 <b>일치</b>로 판정해
     * {@code -6}(완료된 재시도)이 되고, 그러면 DB 에 없는 멱등 레코드를 찾다가 500 이 됩니다.
     * UUID v4 허용목록이라 <b>마커 값을 무엇으로 바꾸든 같은 자리에서 막힙니다</b> — 값을
     * 감추는 것은 해법이 아닙니다. Lua 의 인자 가드는 그대로 최종 방어선으로 남습니다.
     *
     * <p>V2 서비스 빈은 게이트({@code IssuanceGatePort})가 있을 때만 만들어집니다. 회차는
     * V2 인데 게이트가 없는 구성이면 <b>요청을 즉시 중단</b>합니다 — v1 으로 대신 흘리면
     * 그 회차의 재고 권한이 Redis 와 DB 로 갈려 초과 발급이 납니다.
     *
     * @throws IllegalStateException 게이트가 활성화되지 않아 V2 서비스 빈이 없을 때
     */
    private CouponIssueResult issueV2(
            String requestId,
            Long couponRoundId,
            Long memberId,
            MembershipGrade membershipGrade,
            String idempotencyKey,
            CouponRoundIssuanceDefinition definition
    ) {
        // v1 은 CouponOperationExecutionService 첫 줄에서 이 검증을 지난다. v2 는 그 실행기를
        // 안 거치므로 여기가 같은 자리다. 게이트보다 먼저 서야 한다 — 뒤에 두면 이미 선점이
        // 성립한 뒤라 되돌릴 것이 생긴다.
        IdempotencyKeys.validate(
                idempotencyKey, CouponIssueErrorCode.INVALID_COUPON_ISSUE_REQUEST);
        ObservationScope observation = openObservation(
                requestId, memberId, couponRoundId, membershipGrade,
                definition.engineVersion());
        try {
            V2CouponIssueService service = v2Services.getIfAvailable();
            if (service == null) {
                throw new IllegalStateException("V2 발급 게이트가 활성화되지 않았습니다.");
            }
            V2CouponIssueResult execution = service.issue(
                    new CouponIssueCommand(couponRoundId, memberId, membershipGrade,
                            idempotencyKey, timeProvider.instant()),
                    definition
            );
            ClaimOutcome outcome = execution.claimResult().outcome();
            if (execution.databaseSoldOutAfterRedisClaim()) {
                v2Meters.recordDatabaseStockDivergence();
            }
            if (execution.databaseDuplicateAfterRedisClaim()) {
                v2Meters.recordDatabaseMemberDivergence();
            }
            // 되돌아오지 않은 선점은 그 회차 Redis 재고를 영구히 낮춘다. 괴리 카운터는
            // "DB 가 막았다"까지만 말하므로 이 누수를 구분하지 못한다.
            execution.compensateOutcome().ifPresent(this::countCompensationAnomaly);
            if (outcome.isClaimed()) {
                observation.recordClaimedAttempt();
            } else if (execution.replayed()) {
                observation.recordReplayAttempt();
                v2Meters.recordReplayDone();
            } else {
                countRejection(outcome, execution.databaseDuplicateAfterRedisClaim());
                throw rejection(outcome);
            }
            CouponIssueResult result = execution.issueResult()
                    .orElseThrow(() -> new IllegalStateException(
                            "선점·replay 결과에 발급 결과가 없습니다: " + outcome));
            observation.completeIssued(result);
            return result;
        } catch (RuntimeException failure) {
            // 보상 결과가 결과 객체에 실리는 것은 DB 가 판정을 확정한 매진·중복 두 경로뿐이다.
            // 나머지 실패는 전부 예외로 나가므로, 여기서 세지 않으면 그 경로의 누수가
            // 하나도 안 잡힌다.
            if (failure instanceof V2CouponIssueException gateFailure) {
                gateFailure.compensateOutcome().ifPresent(this::countCompensationAnomaly);
            }
            RuntimeException responseFailure = redisUnavailable(failure);
            if (responseFailure instanceof RetryAfterException retryAfter
                    && retryAfter.getErrorCode() == CouponIssueV2ErrorCode.REDIS_UNAVAILABLE) {
                v2Meters.recordRedisUnavailable();
            }
            completeFailure(observation, responseFailure, failure);
            throw responseFailure;
        } finally {
            observation.finish();
        }
    }

    /**
     * 거절을 카운터에 셉니다. 세는 자리를 매핑에서 떼어 놓았습니다 — 예외를 만드는 메서드가
     * 부수효과를 내면, 나중에 그 메서드를 로그·테스트에서 한 번 더 부르는 순간 요청 하나가
     * 두 번 세어집니다. {@code dupPerMember} 는 1인1매 방어의 발동 빈도라 부풀면 오탐 경보가
     * 되고, 컴파일도 테스트도 그때 깨지지 않습니다.
     *
     * @param outcome 선점 거절 결과
     * @param caughtOnlyByDatabase 게이트를 통과했는데 DB 제약이 잡은 중복인지
     */
    private void countRejection(ClaimOutcome outcome, boolean caughtOnlyByDatabase) {
        if (outcome == ClaimOutcome.DUP_PER_MEMBER) {
            // 게이트가 <b>막지 못한</b> 건은 세지 않는다. 이 카운터의 정의가 "1인1매 방어의
            // 발동 빈도" 라, 게이트를 통과한 건을 섞으면 정의가 둘이 되고 위 javadoc 이
            // 경고한 그 부풀림이 된다. 합계가 필요하면 질의에서 회원 괴리와 더한다.
            if (!caughtOnlyByDatabase) {
                v2Meters.recordDupPerMember();
            }
        } else if (outcome == ClaimOutcome.REPLAY_PENDING) {
            v2Meters.recordReplayPending();
        }
    }

    /**
     * 이 보상 결과가 게이트에 선점을 <b>남겼는가</b>.
     *
     * <p><b>보상 결과를 해석하는 자리는 여기 하나다.</b> 응답(재시도 권유)과 관제(누수 계수)가
     * 각자 술어를 들면 같은 값을 반대로 읽는 일이 생기고, 실제로 그 사고가 이 경로에서
     * 반복됐다. 두 소비자가 이 함수만 본다.
     *
     * <p><b>{@code switch} 문이 아니라 식이다.</b> 문은 열거형 전수를 강요하지 않아 새 값이
     * 조용히 빠진다 — 식이라야 값이 늘 때 컴파일이 깨져 사람이 분류를 다시 본다.
     */
    private static boolean leftClaimBehind(CompensateOutcome compensation) {
        return switch (compensation) {
            // 되돌렸다 / 되돌릴 것이 애초에 없었다(다른 절차가 먼저 정리했다).
            case REVERTED, NO_CLAIM -> false;
            // 게이트가 이미 D 로 승격시켰다. 이 요청이 <b>되돌릴 것</b>은 없다 — DB 트랜잭션이
            // 롤백된 경로라면 재고 한 장이 소비된 채 남지만, 그것은 이 요청의 선점이 아니라
            // 승격된 발급의 몫이고 compensation.already.done 이 따로 센다.
            case ALREADY_DONE -> false;
            // 남의 토큰이 덮었거나(NOT_MINE), 보내지 못했거나, 호출이 깨졌거나, 인자가
            // 거부됐거나(BAD_ARGUMENT), 값·카운터를 못 읽었다. 어느 쪽이든 HDEL·INCR 이
            // 하나도 실행되지 않아 이 요청의 DECR 이 복구되지 않은 채 끝난다.
            //
            // BAD_ARGUMENT 는 지금 도달할 수 없다 — 보상의 인자 가드가 선점의 것과 술어가
            // 같고(memberId 는 Long.toString 이라 빈 값이 될 수 없고 토큰은 선점에 넘긴 그
            // 인스턴스다), 선점이 -10 으로 거절됐으면 그 요청은 보상을 부르지 않는다.
            // 두 스크립트의 가드가 갈리는 변경이 들어오면 도달하게 되고, 그때는 선점 성공
            // 여부를 여기서 알 수 없으므로 <b>보수적으로 누수로 센다</b> — 기준선이 0 이어야
            // 하는 미터에 없는 건을 더하는 쪽이, 있는 누수를 빠뜨리는 쪽보다 낫다.
            case NOT_MINE, NOT_ATTEMPTED_CIRCUIT_OPEN, ATTEMPT_FAILED,
                    CORRUPT_VALUE, COUNTER_UNREADABLE, BAD_ARGUMENT -> true;
        };
    }

    /**
     * 되돌아오지 않은 선점을 셉니다.
     *
     * <p>판정은 {@link #leftClaimBehind(CompensateOutcome)} 이 합니다 — 응답 분류와 같은
     * 함수라, 같은 값을 두 곳이 반대로 읽는 일이 생기지 않습니다.
     *
     * <p><b>{@code claim.leaked} 의 기준선은 0 이어야 합니다.</b> 그래야 임계 경보를 걸 수
     * 있고, 진짜 Sentinel 승격 유실이 잡음에 묻히지 않습니다. 그래서 남긴 것이 없는 결과는
     * 이 미터에 넣지 않고 {@code compensation.no.claim}·{@code compensation.already.done}
     * 으로 갈라 셉니다. CY-781 이 Lua 의 {@code 0} 을 "없다"({@code NO_CLAIM})와
     * "남의 토큰"({@code NOT_MINE})으로 나눈 뒤부터 이 구분이 추측이 아니라 사실입니다.
     *
     * @param compensation 보상 CAS 결과
     */
    private void countCompensationAnomaly(CompensateOutcome compensation) {
        if (leftClaimBehind(compensation)) {
            v2Meters.recordClaimLeaked();
            return;
        }
        // 남긴 것이 없는 결과 중 둘은 그래도 봐 둘 사건이다. 되돌릴 선점이 없었다는 것은
        // 다른 절차가 먼저 정리했다는 뜻이고, 이미 승격된 선점에 보상이 온 것은 경보다.
        if (compensation == CompensateOutcome.NO_CLAIM) {
            v2Meters.recordCompensationFoundNoClaim();
        } else if (compensation == CompensateOutcome.ALREADY_DONE) {
            v2Meters.recordCompensationOnCompletedClaim();
        }
    }

    /**
     * 게이트 거절을 그 거절만의 HTTP 응답으로 옮깁니다.
     *
     * <p><b>{@code DUP_PER_MEMBER}·{@code REPLAY_DONE}·{@code REPLAY_PENDING} 을 절대
     * 뭉치지 않습니다.</b> 멱등이 있는 이유가 재시도를 안전하게 만드는 것인데, 응답을 못 받고
     * 다시 누른 클라이언트에게 "이미 발급받으셨습니다" 를 주면 그건 멱등이 아니라 고장입니다.
     * 이건 클라이언트가 이미 본 응답이라 나중에 리팩토링으로 못 고칩니다.
     *
     * <p><b>파손({@code CORRUPT_VALUE})에서 회수를 부르지 않습니다.</b> 발급이 도는 중의 회수는
     * 살아 있는 선점을 지울 수 있어, 게이트가 닫힌 재구성 절차에서만 돕니다(문서 13).
     *
     * <p>{@code default} 절이 없습니다 — 게이트 결과가 늘면 여기서 컴파일이 깨집니다. 조용히
     * 한 덩어리로 접히면 새 반환 코드가 {@code UNMAPPED} 로 관제에 도착합니다.
     *
     * <p><b>부수효과가 없습니다.</b> 계수는 {@link #countRejection(ClaimOutcome, boolean)} 이 합니다.
     *
     * @param outcome 선점 거절 결과
     * @return 그 거절에 대응하는 업무 예외
     */
    private BusinessException rejection(ClaimOutcome outcome) {
        return switch (outcome) {
            case CLOSED -> new BusinessException(CouponIssueErrorCode.COUPON_ROUND_CLOSED);
            case NOT_OPEN -> new BusinessException(CouponIssueErrorCode.NOT_OPENED);
            case GRADE_NOT_ALLOWED ->
                    new BusinessException(CouponIssueErrorCode.GRADE_NOT_ELIGIBLE);
            case DUP_PER_MEMBER -> new BusinessException(CouponIssueErrorCode.ALREADY_ISSUED);
            case SOLD_OUT -> new BusinessException(CouponIssueErrorCode.SOLD_OUT);
            // 폴링하지 않는다. 다시 오면 대개 완료라 replay 로 갈린다.
            case REPLAY_PENDING -> new RetryAfterException(
                    CouponIssueV2ErrorCode.REPLAY_PENDING,
                    issuanceProperties.replayPendingRetryAfterSeconds());
            case CORRUPT_VALUE -> new BusinessException(CouponIssueV2ErrorCode.VALUE_CORRUPT);
            case GATE_NOT_READY -> new RetryAfterException(
                    CouponIssueV2ErrorCode.GATE_NOT_READY,
                    issuanceProperties.gateNotReadyRetryAfterSeconds());
            case BAD_ARGUMENT -> new BusinessException(CouponIssueV2ErrorCode.BAD_ARGUMENT);
            // 기다려서 풀리지 않는다. Retry-After 를 붙이면 같은 실패가 되돌아온다.
            case COUNTER_UNREADABLE ->
                    new BusinessException(CouponIssueV2ErrorCode.COUNTER_UNREADABLE);
            case CLAIMED, REPLAY_DONE -> {
                throw new IllegalStateException(
                        "거절이 아닌 결과가 거절 매핑에 도달했습니다: " + outcome);
            }
        };
    }

    /** 관측 Context와 Session을 만들지 못하면 무동작 요청 범위로 대체합니다. */
    private ObservationScope openObservation(
            String requestId,
            Long memberId,
            Long couponRoundId,
            MembershipGrade membershipGrade,
            EngineVersion engineVersion
    ) {
        try {
            Optional<IssuanceFlowEvent.Ctx> context = contextFactory.create(
                    requestId,
                    memberId,
                    couponRoundId,
                    membershipGrade,
                    engineVersion
            );
            if (context.isEmpty()) {
                return ObservationScope.disabled(observationService);
            }
            return ObservationScope.enabled(
                    context.get(),
                    observationService.begin(context.get()),
                    observationService
            );
        } catch (RuntimeException ignored) {
            return ObservationScope.disabled(observationService);
        }
    }

    /** 업무 실패를 매핑해 등록하되 매핑·완료 실패는 원래 예외를 덮지 않습니다. */
    private void completeFailure(
            ObservationScope observation,
        RuntimeException failure
    ) {
        completeFailure(observation, failure, failure);
    }

    /**
     * 업무 실패를 매핑해 등록합니다. <b>응답과 귀속의 출처를 나눕니다.</b>
     *
     * <p>{@code REDIS_UNAVAILABLE} 로 옮긴 예외를 그대로 매퍼에 넘기면 그 코드의
     * {@link Dependency#REDIS} 가 관측에 박혀 <b>MySQL 장애가 Redis 장애로 집계됩니다</b> —
     * 어댑터가 확정해 실어 보낸 {@code dependency()} 가 응답 매핑 한 줄에 덮이는 것이라,
     * {@code V2CouponIssueException} 이 그 값을 나르는 이유 자체가 사라집니다. Chaos 리포트가
     * "MySQL 주입 구간에 Redis 장애 급증" 으로 뒤집혀 읽힙니다. HTTP 상태·사유는 클라이언트가
     * 볼 응답에서, 의존성은 <b>원 실패</b>에서 가져옵니다.
     *
     * @param observation 요청 단위 관측
     * @param responseFailure 클라이언트에게 나갈 예외 — HTTP 상태·사유의 출처
     * @param originalFailure 무엇이 실제로 막혔는지 아는 예외 — 의존성 귀속의 출처
     */
    private void completeFailure(
            ObservationScope observation,
            RuntimeException responseFailure,
            RuntimeException originalFailure
    ) {
        try {
            CouponIssueObservationFailure mapped =
                    dependencyMapper.classify(responseFailure);
            observation.completeRejected(
                    mapped.httpStatus(),
                    mapped.reasonCode(),
                    dependencyMapper.dependency(originalFailure)
            );
        } catch (RuntimeException ignored) {
            // 관측 매핑 실패는 원래 발급 예외를 그대로 보존한다.
        }
    }

    /** nullable 상태를 외부에 노출하지 않는 요청 단위 관측 어댑터입니다. */
    private static final class ObservationScope {

        private final IssuanceFlowEvent.Ctx context;
        private final IssuanceObservationSession session;
        private final IssuanceObservationService service;
        private boolean attemptRecorded;

        private ObservationScope(
                IssuanceFlowEvent.Ctx context,
                IssuanceObservationSession session,
                IssuanceObservationService service
        ) {
            this.context = context;
            this.session = session;
            this.service = service;
        }

        /** 관측 가능한 요청 범위를 만듭니다. */
        private static ObservationScope enabled(
                IssuanceFlowEvent.Ctx context,
                IssuanceObservationSession session,
                IssuanceObservationService service
        ) {
            return new ObservationScope(context, session, service);
        }

        /** Context가 없는 요청의 무동작 범위를 만듭니다. */
        private static ObservationScope disabled(
                IssuanceObservationService service
        ) {
            return new ObservationScope(null, null, service);
        }

        /** 신규·stale 선점 요청의 시도를 기록합니다. */
        private void recordClaimedAttempt() {
            recordAttemptOnce(context);
        }

        /** DONE 응답 재사용 시 replay 표식이 있는 시도만 기록합니다. */
        private void recordReplayAttempt() {
            if (context == null) {
                return;
            }
            try {
                recordAttemptOnce(context.withReplayed(true));
            } catch (RuntimeException ignored) {
                // replay Context 변환 실패도 저장된 업무 응답을 바꾸지 않는다.
            }
        }

        /** 발급 성공 결과를 Session에 한 번 등록합니다. */
        private void completeIssued(CouponIssueResult result) {
            if (session == null) {
                return;
            }
            try {
                session.completeIssued(result.issuanceId(), result.code());
            } catch (RuntimeException ignored) {
                // Session 실패는 이미 확정된 발급 결과를 바꾸지 않는다.
            }
        }

        /** 매핑이 끝난 오류 결과를 Session에 한 번 등록합니다. */
        private void completeRejected(
                int httpStatus,
                ReasonCode reasonCode,
                Dependency dependency
        ) {
            if (session == null) {
                return;
            }
            session.completeIssueRejected(httpStatus, reasonCode, dependency);
        }

        /** 결과 등록 여부와 무관하게 Session 종료를 한 번 시도합니다. */
        private void finish() {
            if (session == null) {
                return;
            }
            try {
                session.finish();
            } catch (RuntimeException ignored) {
                // 종료 기록 실패는 이미 확정된 업무 결과나 예외를 바꾸지 않는다.
            }
        }

        /** 한 HTTP 요청의 attempt를 한 번만 기록하고 기록 실패를 전파하지 않습니다. */
        private void recordAttemptOnce(IssuanceFlowEvent.Ctx attemptContext) {
            if (attemptContext == null || attemptRecorded) {
                return;
            }
            attemptRecorded = true;
            try {
                service.recordIssueAttempt(attemptContext);
            } catch (RuntimeException ignored) {
                // callback은 권위 발급 트랜잭션 앞에 있으므로 관측 실패를 격리한다.
            }
        }
    }
}
