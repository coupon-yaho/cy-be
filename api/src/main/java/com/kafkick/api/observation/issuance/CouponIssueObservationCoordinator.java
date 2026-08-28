package com.kafkick.api.observation.issuance;

import java.util.Objects;
import java.util.Optional;

import org.springframework.stereotype.Component;
import org.springframework.beans.factory.ObjectProvider;

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
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
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

    private final CouponOperationExecutionService operationExecutionService;
    private final IssuanceObservationContextFactory contextFactory;
    private final IssuanceObservationService observationService;
    private final CouponIssueObservationDependencyMapper dependencyMapper;
    private final CouponIssuanceRouter router;
    private final ObjectProvider<V2CouponIssueService> v2Services;
    private final V2IssuanceOutcomeMeters v2Meters;
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
     * @param v2Meters v2 중복·재시도 카운터 세 종
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
            CouponIssueExecutionResult execution =
                    operationExecutionService.issueWithMetadata(
                            couponRoundId,
                            memberId,
                            membershipGrade,
                            idempotencyKey,
                            observation::recordClaimedAttempt
                    );
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
            if (outcome.isClaimed()) {
                observation.recordClaimedAttempt();
            } else if (execution.replayed()) {
                observation.recordReplayAttempt();
                v2Meters.recordReplayDone();
            } else {
                countRejection(outcome);
                throw rejection(outcome);
            }
            CouponIssueResult result = execution.issueResult()
                    .orElseThrow(() -> new IllegalStateException(
                            "선점·replay 결과에 발급 결과가 없습니다: " + outcome));
            observation.completeIssued(result);
            return result;
        } catch (RuntimeException failure) {
            completeFailure(observation, failure);
            throw failure;
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
     */
    private void countRejection(ClaimOutcome outcome) {
        if (outcome == ClaimOutcome.DUP_PER_MEMBER) {
            v2Meters.recordDupPerMember();
        } else if (outcome == ClaimOutcome.REPLAY_PENDING) {
            v2Meters.recordReplayPending();
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
     * <p><b>부수효과가 없습니다.</b> 계수는 {@link #countRejection(ClaimOutcome)} 이 합니다.
     *
     * @param outcome 선점 거절 결과
     * @return 그 거절에 대응하는 업무 예외
     */
    private BusinessException rejection(ClaimOutcome outcome) {
        return switch (outcome) {
            case CLOSED -> new BusinessException(CouponIssueErrorCode.CAMPAIGN_CLOSED);
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
        try {
            CouponIssueObservationFailure mapped =
                    dependencyMapper.classify(failure);
            observation.completeRejected(
                    mapped.httpStatus(),
                    mapped.reasonCode(),
                    mapped.dependency()
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
