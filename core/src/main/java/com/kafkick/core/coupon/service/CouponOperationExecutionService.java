package com.kafkick.core.coupon.service;

import java.time.Instant;

import org.springframework.stereotype.Service;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.service.command.CouponCancelCommand;
import com.kafkick.core.coupon.service.command.CouponCancelUseCommand;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.command.CouponUseCommand;
import com.kafkick.core.coupon.service.idempotency.IdempotencyExecutionService;
import com.kafkick.core.coupon.service.idempotency.IdempotentExecutionResult;
import com.kafkick.core.coupon.service.idempotency.IdempotentOperationService;
import com.kafkick.core.coupon.service.result.CouponCancelResult;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.service.result.CouponIssueExecutionResult;
import com.kafkick.core.coupon.service.result.CouponUseResult;
import com.kafkick.core.membership.domain.MembershipGrade;

@Service
public class CouponOperationExecutionService {

    private final IdempotencyExecutionService idempotencyExecutionService;
    private final IdempotentOperationService operationService;
    private final CouponIssueService couponIssueService;
    private final CouponIssuePolicyValidator couponIssuePolicyValidator;
    private final CouponUseService couponUseService;
    private final CouponCancelUseService couponCancelUseService;
    private final CouponCancelService couponCancelService;
    private final IdempotencyResultCodec<CouponIssueResult> issueCodec;
    private final IdempotencyResultCodec<CouponUseResult> useCodec;
    private final IdempotencyResultCodec<CouponCancelUseResult> cancelUseCodec;
    private final IdempotencyResultCodec<CouponCancelResult> cancelCodec;

    public CouponOperationExecutionService(
            IdempotencyExecutionService idempotencyExecutionService,
            IdempotentOperationService operationService,
            CouponIssueService couponIssueService,
            CouponIssuePolicyValidator couponIssuePolicyValidator,
            CouponUseService couponUseService,
            CouponCancelUseService couponCancelUseService,
            CouponCancelService couponCancelService,
            IdempotencyResultCodec<CouponIssueResult> issueCodec,
            IdempotencyResultCodec<CouponUseResult> useCodec,
            IdempotencyResultCodec<CouponCancelUseResult> cancelUseCodec,
            IdempotencyResultCodec<CouponCancelResult> cancelCodec
    ) {
        this.idempotencyExecutionService = idempotencyExecutionService;
        this.operationService = operationService;
        this.couponIssueService = couponIssueService;
        this.couponIssuePolicyValidator = couponIssuePolicyValidator;
        this.couponUseService = couponUseService;
        this.couponCancelUseService = couponCancelUseService;
        this.couponCancelService = couponCancelService;
        this.issueCodec = issueCodec;
        this.useCodec = useCodec;
        this.cancelUseCodec = cancelUseCodec;
        this.cancelCodec = cancelCodec;
    }

    public CouponIssueResult issue(
            Long couponRoundId,
            Long memberId,
            MembershipGrade membershipGrade,
            String idempotencyKey
    ) {
        return issueWithMetadata(
                couponRoundId,
                memberId,
                membershipGrade,
                idempotencyKey
        ).result();
    }

    /**
     * 기존 발급 응답과 DONE 멱등 응답 재사용 여부를 함께 반환합니다.
     *
     * <p>기존 {@link #issue(Long, Long, MembershipGrade, String)} 계약은 유지하며,
     * 관측 호출부처럼 replay 구분이 필요한 소비자만 이 메서드를 사용합니다.
     *
     * @param couponRoundId 쿠폰 회차 식별자
     * @param memberId 회원 식별자
     * @param membershipGrade 요청 시점 회원 등급
     * @param idempotencyKey UUID v4 멱등 키
     * @return 발급 응답과 replay 여부
     */
    public CouponIssueExecutionResult issueWithMetadata(
            Long couponRoundId,
            Long memberId,
            MembershipGrade membershipGrade,
            String idempotencyKey
    ) {
        return issueWithMetadata(
                couponRoundId,
                memberId,
                membershipGrade,
                idempotencyKey,
                IssueAttemptCallback.NO_OP
        );
    }

    /**
     * 멱등 선점 뒤 정책 사전검증과 발급 시도 알림을 거쳐 권위 발급을 실행합니다.
     *
     * <p>사전검증의 읽기 전용 트랜잭션이 끝난 뒤 callback을 호출하고, 그 다음 기존 원자 발급
     * 트랜잭션을 시작합니다. DONE replay는 선점된 실행 경로에 들어가지 않으므로 callback과
     * 권위 발급을 모두 건너뜁니다. callback 실패는 관측 실패이므로 업무 결과에 전파하지 않습니다.
     *
     * @param couponRoundId 쿠폰 회차 식별자
     * @param memberId 회원 식별자
     * @param membershipGrade 요청 시점 회원 등급
     * @param idempotencyKey UUID v4 멱등 키
     * @param attemptCallback 정책 사전검증 통과 알림
     * @return 발급 응답과 DONE replay 여부
     */
    public CouponIssueExecutionResult issueWithMetadata(
            Long couponRoundId,
            Long memberId,
            MembershipGrade membershipGrade,
            String idempotencyKey,
            IssueAttemptCallback attemptCallback
    ) {
        IdempotentExecutionResult<CouponIssueResult> execution =
                idempotencyExecutionService.executeWithMetadata(
                        idempotencyKey,
                        () -> CouponIssueCommand.canonicalRequest(
                                couponRoundId,
                                memberId,
                                membershipGrade
                        ),
                        CouponIssueErrorCode.INVALID_COUPON_ISSUE_REQUEST,
                        claimedAt -> issueClaimedRequest(
                                couponRoundId,
                                memberId,
                                membershipGrade,
                                idempotencyKey,
                                claimedAt,
                                attemptCallback
                        ),
                        issueCodec::read
                );
        return new CouponIssueExecutionResult(
                execution.value(),
                execution.replayed()
        );
    }

    /**
     * 선점된 요청을 사전검증한 뒤 관측 callback과 권위 발급 트랜잭션을 순서대로 실행합니다.
     *
     * @param couponRoundId 쿠폰 회차 식별자
     * @param memberId 회원 식별자
     * @param membershipGrade 요청 시점 회원 등급
     * @param idempotencyKey UUID v4 멱등 키
     * @param claimedAt 멱등 선점 시각
     * @param attemptCallback 정책 사전검증 통과 알림
     * @return 권위 발급 결과
     */
    private CouponIssueResult issueClaimedRequest(
            Long couponRoundId,
            Long memberId,
            MembershipGrade membershipGrade,
            String idempotencyKey,
            Instant claimedAt,
            IssueAttemptCallback attemptCallback
    ) {
        CouponIssueCommand command = new CouponIssueCommand(
                couponRoundId,
                memberId,
                membershipGrade,
                idempotencyKey,
                claimedAt
        );
        couponIssuePolicyValidator.validate(command);
        notifyPolicyPassed(attemptCallback);
        return operationService.execute(
                idempotencyKey,
                memberId,
                claimedAt,
                () -> CouponIssueResult.from(couponIssueService.issue(command)),
                issueCodec,
                CouponIssueResult::issuanceId
        );
    }

    /** 관측 callback의 RuntimeException을 발급 흐름에서 격리합니다. */
    private static void notifyPolicyPassed(
            IssueAttemptCallback attemptCallback
    ) {
        try {
            attemptCallback.onPolicyPassed();
        } catch (RuntimeException ignored) {
            // 관측 callback 실패는 뒤따르는 권위 발급 트랜잭션의 결과를 바꾸지 않는다.
        }
    }

    public CouponUseResult use(
            Long issuanceId,
            Long memberId,
            int orderAmount,
            String idempotencyKey
    ) {
        return idempotencyExecutionService.execute(
                idempotencyKey,
                () -> CouponUseCommand.canonicalRequest(
                        issuanceId, memberId, orderAmount
                ),
                CouponUseErrorCode.INVALID_COUPON_USE_REQUEST,
                claimedAt -> operationService.execute(
                        idempotencyKey,
                        memberId,
                        claimedAt,
                        () -> couponUseService.use(new CouponUseCommand(
                                issuanceId,
                                memberId,
                                orderAmount,
                                idempotencyKey,
                                claimedAt
                        )),
                        useCodec,
                        CouponUseResult::issuanceId
                ),
                useCodec::read
        );
    }

    public CouponCancelUseResult cancelUse(
            Long issuanceId,
            Long memberId,
            String idempotencyKey
    ) {
        return idempotencyExecutionService.execute(
                idempotencyKey,
                () -> CouponCancelUseCommand.canonicalRequest(
                        issuanceId, memberId
                ),
                CouponUseErrorCode.INVALID_COUPON_CANCEL_USE_REQUEST,
                claimedAt -> operationService.execute(
                        idempotencyKey,
                        memberId,
                        claimedAt,
                        () -> couponCancelUseService.cancelUse(
                                new CouponCancelUseCommand(
                                        issuanceId,
                                        memberId,
                                        idempotencyKey,
                                        claimedAt
                                )
                        ),
                        cancelUseCodec,
                        CouponCancelUseResult::issuanceId
                ),
                cancelUseCodec::read
        );
    }

    public CouponCancelResult cancel(
            Long issuanceId,
            Long memberId,
            String idempotencyKey
    ) {
        return idempotencyExecutionService.execute(
                idempotencyKey,
                () -> CouponCancelCommand.canonicalRequest(
                        issuanceId, memberId
                ),
                CouponUseErrorCode.INVALID_COUPON_CANCEL_REQUEST,
                claimedAt -> operationService.execute(
                        idempotencyKey,
                        memberId,
                        claimedAt,
                        () -> couponCancelService.cancel(
                                new CouponCancelCommand(
                                        issuanceId,
                                        memberId,
                                        idempotencyKey,
                                        claimedAt
                                )
                        ),
                        cancelCodec,
                        CouponCancelResult::issuanceId
                ),
                cancelCodec::read
        );
    }
}
