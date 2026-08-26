package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.kafkick.core.coupon.exception.CouponAlreadyIssuedException;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.service.command.CouponCancelCommand;
import com.kafkick.core.coupon.service.command.CouponCancelUseCommand;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.command.CouponUseCommand;
import com.kafkick.core.coupon.service.idempotency.IdempotencyExecutionService;
import com.kafkick.core.coupon.service.idempotency.IdempotencyKeyTakenException;
import com.kafkick.core.coupon.service.idempotency.IdempotencyKeys;
import com.kafkick.core.coupon.service.idempotency.IdempotentExecutionResult;
import com.kafkick.core.coupon.service.idempotency.IdempotentOperationService;
import com.kafkick.core.coupon.service.result.CouponCancelResult;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.service.result.CouponIssueExecutionResult;
import com.kafkick.core.coupon.service.result.CouponUseResult;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

@Service
public class CouponOperationExecutionService {

    private final IdempotencyExecutionService idempotencyExecutionService;
    private final IdempotentOperationService operationService;
    private final CouponIssueService couponIssueService;
    private final CouponIssuePreflightService issuePreflightService;
    private final TimeProvider timeProvider;
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
            CouponIssuePreflightService issuePreflightService,
            TimeProvider timeProvider,
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
        this.issuePreflightService = issuePreflightService;
        this.timeProvider = timeProvider;
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
     * 사전조회로 완료 여부와 정책을 확인한 뒤 발급 시도를 알리고 권위 발급을 실행합니다.
     *
     * <p>트랜잭션은 두 개입니다 — 읽기 전용 사전조회 하나, 권위 발급 하나. 발급은
     * {@code uk_coupon_member}가 멱등 선점과 같은 배제를 이미 제공하므로 IN_PROGRESS 선점을
     * 먼저 커밋하지 않습니다. 그 결과 실패 정리({@code release})와 진행 중 폴링이 필요 없습니다.
     *
     * <p>동시에 같은 키 또는 같은 회원으로 들어온 두 요청 중 진 쪽은 권위 트랜잭션이 롤백된 뒤
     * 커밋된 결과를 다시 읽어 재사용합니다. 그 결과가 없으면 원래의 업무 예외를 그대로 냅니다.
     *
     * <p>사용·취소는 자연 유일 제약이 없어 {@link IdempotencyExecutionService}의 2단계 쓰기를
     * 그대로 씁니다.
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
        IdempotencyKeys.validate(
                idempotencyKey,
                CouponIssueErrorCode.INVALID_COUPON_ISSUE_REQUEST
        );
        String requestHash = IdempotencyKeys.hash(
                CouponIssueCommand.canonicalRequest(
                        couponRoundId,
                        memberId,
                        membershipGrade
                )
        );
        Instant requestAt = timeProvider.instant()
                .truncatedTo(ChronoUnit.MICROS);
        CouponIssueCommand command = new CouponIssueCommand(
                couponRoundId,
                memberId,
                membershipGrade,
                idempotencyKey,
                requestAt
        );

        Optional<String> completed = issuePreflightService
                .inspect(command, requestHash)
                .completed();
        if (completed.isPresent()) {
            return new CouponIssueExecutionResult(
                    issueCodec.read(completed.get()),
                    true
            );
        }

        notifyPolicyPassed(attemptCallback);
        return issueAuthoritatively(command, requestHash);
    }

    /**
     * 권위 발급 트랜잭션을 실행하고, 경합에서 지면 저장된 결과 재사용으로 넘어갑니다.
     *
     * @param command 발급 요청
     * @param requestHash 요청 정규화 해시
     * @return 발급 응답과 재사용 여부
     */
    private CouponIssueExecutionResult issueAuthoritatively(
            CouponIssueCommand command,
            String requestHash
    ) {
        try {
            return new CouponIssueExecutionResult(
                    operationService.executeAndRecord(
                            command.idempotencyKey(),
                            command.memberId(),
                            requestHash,
                            command.issuedAt(),
                            () -> CouponIssueResult.from(
                                    couponIssueService.issue(command)
                            ),
                            issueCodec,
                            CouponIssueResult::issuanceId
                    ).result(),
                    false
            );
        } catch (IdempotencyKeyTakenException keyTaken) {
            // 같은 키가 먼저 확정됐다. 그 응답이 곧 이 요청의 응답이다.
            return replayCommitted(command, requestHash, null);
        } catch (CouponAlreadyIssuedException alreadyIssued) {
            // 경합에서 졌을 수 있다. 저장된 결과가 있으면 재사용하고, 없으면 진짜 중복 발급이다.
            return replayCommitted(command, requestHash, alreadyIssued);
        }
    }

    /**
     * 롤백 뒤 커밋된 멱등 결과를 다시 읽어 재사용합니다.
     *
     * @param command 발급 요청
     * @param requestHash 요청 정규화 해시
     * @param fallback 저장된 결과가 없을 때 다시 낼 중복 발급 예외. {@code null}이면 경합 충돌로 낸다
     * @return 저장된 응답 기반 결과
     */
    private CouponIssueExecutionResult replayCommitted(
            CouponIssueCommand command,
            String requestHash,
            CouponAlreadyIssuedException fallback
    ) {
        Optional<String> completed = issuePreflightService
                .findCompletedResponse(command.idempotencyKey(), requestHash);
        if (completed.isPresent()) {
            return new CouponIssueExecutionResult(
                    issueCodec.read(completed.get()),
                    true
            );
        }
        if (fallback != null) {
            throw fallback;
        }
        throw new BusinessException(
                CouponUseErrorCode.CONFLICT_IN_PROGRESS,
                "idempotencyKey=" + command.idempotencyKey()
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
