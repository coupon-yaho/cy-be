package com.kafkick.core.coupon.service;

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
        IdempotentExecutionResult<CouponIssueResult> execution =
                idempotencyExecutionService.executeWithMetadata(
                        idempotencyKey,
                        () -> CouponIssueCommand.canonicalRequest(
                                couponRoundId,
                                memberId,
                                membershipGrade
                        ),
                        CouponIssueErrorCode.INVALID_COUPON_ISSUE_REQUEST,
                        claimedAt -> operationService.execute(
                                idempotencyKey,
                                memberId,
                                claimedAt,
                                () -> CouponIssueResult.from(
                                        couponIssueService.issue(
                                                new CouponIssueCommand(
                                                        couponRoundId,
                                                        memberId,
                                                        membershipGrade,
                                                        idempotencyKey,
                                                        claimedAt
                                                )
                                        )
                                ),
                                issueCodec,
                                CouponIssueResult::issuanceId
                        ),
                        issueCodec::read
                );
        return new CouponIssueExecutionResult(
                execution.value(),
                execution.replayed()
        );
    }

    public CouponUseResult use(
            Long issuanceId,
            Long memberId,
            Long orderId,
            int orderAmount,
            String idempotencyKey
    ) {
        return idempotencyExecutionService.execute(
                idempotencyKey,
                () -> CouponUseCommand.canonicalRequest(
                        issuanceId, memberId, orderId, orderAmount
                ),
                CouponUseErrorCode.INVALID_COUPON_USE_REQUEST,
                claimedAt -> operationService.execute(
                        idempotencyKey,
                        memberId,
                        claimedAt,
                        () -> couponUseService.use(new CouponUseCommand(
                                issuanceId,
                                memberId,
                                orderId,
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
