package com.kafkick.core.coupon.service;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
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
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponOperationExecutionServiceTest {

    private static final String KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final Instant AT = Instant.parse("2026-08-20T05:00:00Z");

    @Mock
    private IdempotencyExecutionService idempotencyExecutionService;
    @Mock
    private IdempotentOperationService operationService;
    @Mock
    private CouponIssueService couponIssueService;
    @Mock
    private CouponIssuePolicyValidator couponIssuePolicyValidator;
    @Mock
    private CouponUseService couponUseService;
    @Mock
    private CouponCancelUseService couponCancelUseService;
    @Mock
    private CouponCancelService couponCancelService;
    @Mock
    private IdempotencyResultCodec<CouponIssueResult> issueCodec;
    @Mock
    private IdempotencyResultCodec<CouponUseResult> useCodec;
    @Mock
    private IdempotencyResultCodec<CouponCancelUseResult> cancelUseCodec;
    @Mock
    private IdempotencyResultCodec<CouponCancelResult> cancelCodec;

    @Test
    void executesCouponIssueThroughCoreIdempotencyOrchestration() {
        CouponIssueResult expected = new CouponIssueResult(
                100L,
                10L,
                "ABCDEFGHJKLM2345",
                IssuanceStatus.ISSUED,
                AT,
                AT.plusSeconds(604_800)
        );
        when(idempotencyExecutionService.executeWithMetadata(
                eq(KEY), any(), any(), any(), any()
        )).thenAnswer(invocation -> {
            java.util.function.Function<Instant, CouponIssueResult> claimed =
                    invocation.getArgument(3);
            return new IdempotentExecutionResult<>(
                    claimed.apply(AT),
                    false
            );
        });
        when(operationService.execute(
                eq(KEY), eq(20L), eq(AT), any(), eq(issueCodec), any()
        )).thenAnswer(invocation -> {
            java.util.function.Supplier<CouponIssueResult> operation =
                    invocation.getArgument(3);
            CouponIssueResult result = operation.get();
            java.util.function.Function<CouponIssueResult, Long> targetId =
                    invocation.getArgument(5);
            assertThat(targetId.apply(result)).isEqualTo(100L);
            return result;
        });
        when(couponIssueService.issue(any())).thenReturn(Issuance.restore(
                100L,
                10L,
                20L,
                "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD,
                IssuanceStatus.ISSUED,
                AT,
                AT.plusSeconds(604_800),
                AT
        ));
        CouponOperationExecutionService service = service();

        CouponIssueResult actual = service.issue(
                10L,
                20L,
                MembershipGrade.GOLD,
                KEY
        );

        assertThat(actual).isEqualTo(expected);
        ArgumentCaptor<CouponIssueCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponIssueCommand.class);
        verify(couponIssueService).issue(commandCaptor.capture());
        CouponIssueCommand command = commandCaptor.getValue();
        assertThat(command.couponRoundId()).isEqualTo(10L);
        assertThat(command.memberId()).isEqualTo(20L);
        assertThat(command.membershipGrade()).isEqualTo(MembershipGrade.GOLD);
        assertThat(command.idempotencyKey()).isEqualTo(KEY);
        assertThat(command.issuedAt()).isEqualTo(AT);
    }

    @Test
    void replaysCompletedCouponIssueWithoutIssuingAgain() {
        CouponIssueResult expected = new CouponIssueResult(
                100L,
                10L,
                "ABCDEFGHJKLM2345",
                IssuanceStatus.ISSUED,
                AT,
                AT.plusSeconds(604_800)
        );
        when(idempotencyExecutionService.executeWithMetadata(
                eq(KEY), any(), any(), any(), any()
        )).thenAnswer(invocation -> {
            java.util.function.Function<String, CouponIssueResult> replay =
                    invocation.getArgument(4);
            return new IdempotentExecutionResult<>(
                    replay.apply("stored-result"),
                    true
            );
        });
        when(issueCodec.read("stored-result")).thenReturn(expected);
        CouponOperationExecutionService service = service();

        CouponIssueResult actual = service.issue(
                10L,
                20L,
                MembershipGrade.GOLD,
                KEY
        );

        assertThat(actual).isEqualTo(expected);
        verifyNoInteractions(operationService, couponIssueService);
    }

    @Test
    void returnsCouponIssueReplayMetadataWithoutChangingLegacyIssueApi() {
        CouponIssueResult expected = new CouponIssueResult(
                100L,
                10L,
                "ABCDEFGHJKLM2345",
                IssuanceStatus.ISSUED,
                AT,
                AT.plusSeconds(604_800)
        );
        when(idempotencyExecutionService.executeWithMetadata(
                eq(KEY), any(), any(), any(), any()
        )).thenReturn(new IdempotentExecutionResult<>(expected, true));
        CouponOperationExecutionService service = service();

        IssueAttemptCallback callback = org.mockito.Mockito.mock(
                IssueAttemptCallback.class
        );

        CouponIssueExecutionResult actual = service.issueWithMetadata(
                10L,
                20L,
                MembershipGrade.GOLD,
                KEY,
                callback
        );

        assertThat(actual).isEqualTo(new CouponIssueExecutionResult(
                expected,
                true
        ));
        verifyNoInteractions(
                couponIssuePolicyValidator,
                callback,
                operationService,
                couponIssueService
        );
    }

    @Test
    void prevalidatesFirstOrStaleClaimBeforeCallbackAndAuthoritativeTransaction() {
        CouponIssueResult expected = issueResult();
        when(idempotencyExecutionService.executeWithMetadata(
                eq(KEY), any(), any(), any(), any()
        )).thenAnswer(invocation -> {
            java.util.function.Function<Instant, CouponIssueResult> claimed =
                    invocation.getArgument(3);
            return new IdempotentExecutionResult<>(claimed.apply(AT), false);
        });
        when(operationService.execute(
                eq(KEY), eq(20L), eq(AT), any(), eq(issueCodec), any()
        )).thenAnswer(invocation -> {
            java.util.function.Supplier<CouponIssueResult> operation =
                    invocation.getArgument(3);
            return operation.get();
        });
        when(couponIssueService.issue(any())).thenReturn(issuance());
        IssueAttemptCallback callback = org.mockito.Mockito.mock(
                IssueAttemptCallback.class
        );
        org.mockito.Mockito.doAnswer(invocation -> {
            assertThat(TransactionSynchronizationManager
                    .isActualTransactionActive()).isFalse();
            return null;
        }).when(callback).onPolicyPassed();

        CouponIssueExecutionResult actual = service().issueWithMetadata(
                10L,
                20L,
                MembershipGrade.GOLD,
                KEY,
                callback
        );

        assertThat(actual).isEqualTo(new CouponIssueExecutionResult(
                expected,
                false
        ));
        InOrder order = inOrder(
                couponIssuePolicyValidator,
                callback,
                operationService,
                couponIssueService
        );
        order.verify(couponIssuePolicyValidator).validate(any());
        order.verify(callback).onPolicyPassed();
        order.verify(operationService).execute(
                eq(KEY), eq(20L), eq(AT), any(), eq(issueCodec), any()
        );
        order.verify(couponIssueService).issue(any());
    }

    @ParameterizedTest
    @EnumSource(value = CouponIssueErrorCode.class, names = {
            "NOT_OPENED",
            "CAMPAIGN_CLOSED",
            "GRADE_NOT_ELIGIBLE"
    })
    void policyRejectionDoesNotInvokeAttemptOrAuthoritativeTransaction(
            CouponIssueErrorCode errorCode
    ) {
        BusinessException rejected = new BusinessException(
                errorCode
        );
        when(idempotencyExecutionService.executeWithMetadata(
                eq(KEY), any(), any(), any(), any()
        )).thenAnswer(invocation -> {
            java.util.function.Function<Instant, CouponIssueResult> claimed =
                    invocation.getArgument(3);
            return new IdempotentExecutionResult<>(claimed.apply(AT), false);
        });
        org.mockito.Mockito.doThrow(rejected)
                .when(couponIssuePolicyValidator).validate(any());
        IssueAttemptCallback callback = org.mockito.Mockito.mock(
                IssueAttemptCallback.class
        );

        assertThatThrownBy(() -> service().issueWithMetadata(
                10L,
                20L,
                MembershipGrade.GOLD,
                KEY,
                callback
        )).isSameAs(rejected);

        verify(callback, never()).onPolicyPassed();
        verifyNoInteractions(operationService, couponIssueService);
    }

    @Test
    void callbackFailureDoesNotChangeTheAuthoritativeIssueResult() {
        CouponIssueResult expected = issueResult();
        when(idempotencyExecutionService.executeWithMetadata(
                eq(KEY), any(), any(), any(), any()
        )).thenAnswer(invocation -> {
            java.util.function.Function<Instant, CouponIssueResult> claimed =
                    invocation.getArgument(3);
            return new IdempotentExecutionResult<>(claimed.apply(AT), false);
        });
        when(operationService.execute(
                eq(KEY), eq(20L), eq(AT), any(), eq(issueCodec), any()
        )).thenAnswer(invocation -> {
            java.util.function.Supplier<CouponIssueResult> operation =
                    invocation.getArgument(3);
            return operation.get();
        });
        when(couponIssueService.issue(any())).thenReturn(issuance());
        IssueAttemptCallback callback = () -> {
            throw new IllegalStateException("observation unavailable");
        };

        CouponIssueExecutionResult actual = service().issueWithMetadata(
                10L,
                20L,
                MembershipGrade.GOLD,
                KEY,
                callback
        );

        assertThat(actual).isEqualTo(new CouponIssueExecutionResult(
                expected,
                false
        ));
        verify(couponIssueService).issue(any());
    }

    @Test
    void executesCouponUseThroughCoreIdempotencyOrchestration() {
        CouponUseResult expected = new CouponUseResult(
                100L, IssuanceStatus.USED, 30L, 5_000, AT
        );
        when(idempotencyExecutionService.execute(
                eq(KEY), any(), any(), any(), any()
        )).thenAnswer(invocation -> {
            java.util.function.Function<Instant, CouponUseResult> claimed =
                    invocation.getArgument(3);
            return claimed.apply(AT);
        });
        when(operationService.execute(
                eq(KEY), eq(20L), eq(AT), any(), eq(useCodec), any()
        )).thenAnswer(invocation -> {
            java.util.function.Supplier<CouponUseResult> operation =
                    invocation.getArgument(3);
            CouponUseResult result = operation.get();
            java.util.function.Function<CouponUseResult, Long> targetId =
                    invocation.getArgument(5);
            assertThat(targetId.apply(result)).isEqualTo(100L);
            return result;
        });
        when(couponUseService.use(any())).thenReturn(expected);
        CouponOperationExecutionService service = service();

        CouponUseResult actual = service.use(
                100L, 20L, 30L, 20_000, KEY
        );

        assertThat(actual).isEqualTo(expected);
        ArgumentCaptor<CouponUseCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponUseCommand.class);
        verify(couponUseService).use(commandCaptor.capture());
        CouponUseCommand command = commandCaptor.getValue();
        assertThat(command.issuanceId()).isEqualTo(100L);
        assertThat(command.memberId()).isEqualTo(20L);
        assertThat(command.orderId()).isEqualTo(30L);
        assertThat(command.orderAmount()).isEqualTo(20_000);
    }

    @Test
    void propagatesBusinessExceptionFromNestedCouponService() {
        BusinessException expected = new BusinessException(
                CouponIssueErrorCode.ALREADY_ISSUED,
                "memberId=20, couponRoundId=100"
        );
        when(idempotencyExecutionService.execute(
                eq(KEY), any(), any(), any(), any()
        )).thenAnswer(invocation -> {
            java.util.function.Function<Instant, CouponUseResult> claimed =
                    invocation.getArgument(3);
            return claimed.apply(AT);
        });
        when(operationService.execute(
                eq(KEY), eq(20L), eq(AT), any(), eq(useCodec), any()
        )).thenAnswer(invocation -> {
            java.util.function.Supplier<CouponUseResult> operation =
                    invocation.getArgument(3);
            return operation.get();
        });
        when(couponUseService.use(any())).thenThrow(expected);
        CouponOperationExecutionService service = service();

        assertThatThrownBy(() -> service.use(
                100L, 20L, 30L, 20_000, KEY
        )).isSameAs(expected);
    }

    private CouponOperationExecutionService service() {
        return new CouponOperationExecutionService(
                idempotencyExecutionService,
                operationService,
                couponIssueService,
                couponIssuePolicyValidator,
                couponUseService,
                couponCancelUseService,
                couponCancelService,
                issueCodec,
                useCodec,
                cancelUseCodec,
                cancelCodec
        );
    }

    private CouponIssueResult issueResult() {
        return new CouponIssueResult(
                100L,
                10L,
                "ABCDEFGHJKLM2345",
                IssuanceStatus.ISSUED,
                AT,
                AT.plusSeconds(604_800)
        );
    }

    private Issuance issuance() {
        return Issuance.restore(
                100L,
                10L,
                20L,
                "ABCDEFGHJKLM2345",
                MembershipGrade.GOLD,
                IssuanceStatus.ISSUED,
                AT,
                AT.plusSeconds(604_800),
                AT
        );
    }
}
