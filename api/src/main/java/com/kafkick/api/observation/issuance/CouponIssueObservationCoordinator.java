package com.kafkick.api.observation.issuance;

import java.util.Objects;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.service.CouponOperationExecutionService;
import com.kafkick.core.coupon.service.result.CouponIssueExecutionResult;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.ReasonCode;

/** 쿠폰 발급 업무 결과를 바꾸지 않고 요청 단위 관측 수명주기를 연결합니다. */
@Component
public final class CouponIssueObservationCoordinator {

    private final CouponOperationExecutionService operationExecutionService;
    private final IssuanceObservationContextFactory contextFactory;
    private final IssuanceObservationService observationService;
    private final CouponIssueObservationDependencyMapper dependencyMapper;

    /**
     * 발급 실행기와 관측 Context·Session 경계를 조립합니다.
     *
     * @param operationExecutionService 멱등·권위 발급 실행기
     * @param contextFactory 요청별 관측 Context 생성기
     * @param observationService attempt와 Session 기록 서비스
     * @param dependencyMapper 발급 실패 관측 분류기
     */
    public CouponIssueObservationCoordinator(
            CouponOperationExecutionService operationExecutionService,
            IssuanceObservationContextFactory contextFactory,
            IssuanceObservationService observationService,
            CouponIssueObservationDependencyMapper dependencyMapper
    ) {
        this.operationExecutionService = Objects.requireNonNull(
                operationExecutionService
        );
        this.contextFactory = Objects.requireNonNull(contextFactory);
        this.observationService = Objects.requireNonNull(observationService);
        this.dependencyMapper = Objects.requireNonNull(dependencyMapper);
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
        ObservationScope observation = openObservation(
                requestId,
                memberId,
                couponRoundId,
                membershipGrade
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

    /** 관측 Context와 Session을 만들지 못하면 무동작 요청 범위로 대체합니다. */
    private ObservationScope openObservation(
            String requestId,
            Long memberId,
            Long couponRoundId,
            MembershipGrade membershipGrade
    ) {
        try {
            Optional<IssuanceFlowEvent.Ctx> context = contextFactory.create(
                    requestId,
                    memberId,
                    couponRoundId,
                    membershipGrade
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
