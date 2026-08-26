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
import com.kafkick.core.coupon.service.idempotency.IdempotentOperationService;
import com.kafkick.core.coupon.service.result.CouponCancelResult;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
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
        return idempotencyExecutionService.execute(
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
