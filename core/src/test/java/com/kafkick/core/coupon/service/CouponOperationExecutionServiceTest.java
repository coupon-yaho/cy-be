package com.kafkick.core.coupon.service;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.exception.CouponAlreadyIssuedException;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.command.CouponUseCommand;
import com.kafkick.core.coupon.service.idempotency.IdempotencyExecutionService;
import com.kafkick.core.coupon.service.idempotency.IdempotencyKeyTakenException;
import com.kafkick.core.coupon.service.idempotency.IdempotentOperationService;
import com.kafkick.core.coupon.service.result.CouponCancelResult;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.service.result.CouponIssueExecutionResult;
import com.kafkick.core.coupon.service.result.CouponUseResult;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.TimeProvider;
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
    private CouponIssuePreflightService issuePreflightService;
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
    void executesCouponIssueThroughPreflightAndAuthoritativeTransaction() {
        CouponIssueResult expected = issueResult();
        pendingPreflight();
        recordingOperation();
        when(couponIssueService.issue(any())).thenReturn(issuance());
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
        CouponIssueResult expected = issueResult();
        when(issuePreflightService.inspect(any(), any()))
                .thenReturn(CouponIssuePreflight.completed("stored-body"));
        when(issueCodec.read("stored-body")).thenReturn(expected);
        IssueAttemptCallback callback = org.mockito.Mockito.mock(
                IssueAttemptCallback.class
        );

        CouponIssueExecutionResult actual = service().issueWithMetadata(
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
        verifyNoInteractions(callback, operationService, couponIssueService);
    }

    @Test
    void returnsCouponIssueReplayMetadataWithoutChangingLegacyIssueApi() {
        CouponIssueResult expected = issueResult();
        when(issuePreflightService.inspect(any(), any()))
                .thenReturn(CouponIssuePreflight.completed("stored-body"));
        when(issueCodec.read("stored-body")).thenReturn(expected);

        assertThat(service().issue(10L, 20L, MembershipGrade.GOLD, KEY))
                .isEqualTo(expected);
    }

    @Test
    void inspectsFirstThenNotifiesAttemptThenRunsAuthoritativeTransaction() {
        pendingPreflight();
        recordingOperation();
        when(couponIssueService.issue(any())).thenReturn(issuance());
        IssueAttemptCallback callback = org.mockito.Mockito.mock(
                IssueAttemptCallback.class
        );

        CouponIssueExecutionResult actual = service().issueWithMetadata(
                10L,
                20L,
                MembershipGrade.GOLD,
                KEY,
                callback
        );

        assertThat(actual).isEqualTo(new CouponIssueExecutionResult(
                issueResult(),
                false
        ));
        InOrder order = inOrder(
                issuePreflightService,
                callback,
                operationService,
                couponIssueService
        );
        order.verify(issuePreflightService).inspect(any(), any());
        order.verify(callback).onPolicyPassed();
        order.verify(operationService).executeAndRecord(
                eq(KEY), eq(20L), any(), eq(AT), any(), eq(issueCodec), any()
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
        BusinessException rejected = new BusinessException(errorCode);
        when(issuePreflightService.inspect(any(), any())).thenThrow(rejected);
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
        pendingPreflight();
        recordingOperation();
        when(couponIssueService.issue(any())).thenReturn(issuance());
        IssueAttemptCallback callback = org.mockito.Mockito.mock(
                IssueAttemptCallback.class
        );
        org.mockito.Mockito.doThrow(new IllegalStateException("attempt"))
                .when(callback).onPolicyPassed();

        assertThat(service().issueWithMetadata(
                10L,
                20L,
                MembershipGrade.GOLD,
                KEY,
                callback
        )).isEqualTo(new CouponIssueExecutionResult(issueResult(), false));
    }

    @Test
    void replaysCommittedResultWhenAnotherRequestTookTheKeyFirst() {
        CouponIssueResult expected = issueResult();
        pendingPreflight();
        when(operationService.executeAndRecord(
                eq(KEY), eq(20L), any(), eq(AT), any(), eq(issueCodec), any()
        )).thenThrow(new IdempotencyKeyTakenException(KEY));
        when(issuePreflightService.findCompletedResponse(eq(KEY), any()))
                .thenReturn(Optional.of("stored-body"));
        when(issueCodec.read("stored-body")).thenReturn(expected);

        assertThat(service().issueWithMetadata(
                10L,
                20L,
                MembershipGrade.GOLD,
                KEY,
                org.mockito.Mockito.mock(IssueAttemptCallback.class)
        )).isEqualTo(new CouponIssueExecutionResult(expected, true));
    }

    @Test
    void keepsAlreadyIssuedWhenTheDuplicateCameFromAnotherKey() {
        CouponAlreadyIssuedException alreadyIssued =
                new CouponAlreadyIssuedException("couponRoundId=10", null);
        pendingPreflight();
        when(operationService.executeAndRecord(
                eq(KEY), eq(20L), any(), eq(AT), any(), eq(issueCodec), any()
        )).thenThrow(alreadyIssued);
        when(issuePreflightService.findCompletedResponse(eq(KEY), any()))
                .thenReturn(Optional.empty());

        assertThatThrownBy(() -> service().issueWithMetadata(
                10L,
                20L,
                MembershipGrade.GOLD,
                KEY,
                org.mockito.Mockito.mock(IssueAttemptCallback.class)
        )).isSameAs(alreadyIssued);
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
                100L, 20L, 20_000, KEY
        );

        assertThat(actual).isEqualTo(expected);
        ArgumentCaptor<CouponUseCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponUseCommand.class);
        verify(couponUseService).use(commandCaptor.capture());
        CouponUseCommand command = commandCaptor.getValue();
        assertThat(command.issuanceId()).isEqualTo(100L);
        assertThat(command.memberId()).isEqualTo(20L);
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
                100L, 20L, 20_000, KEY
        )).isSameAs(expected);
    }

    private CouponOperationExecutionService service() {
        return new CouponOperationExecutionService(
                idempotencyExecutionService,
                operationService,
                couponIssueService,
                issuePreflightService,
                new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC)),
                couponUseService,
                couponCancelUseService,
                couponCancelService,
                issueCodec,
                useCodec,
                cancelUseCodec,
                cancelCodec
        );
    }

    private void pendingPreflight() {
        when(issuePreflightService.inspect(any(), any()))
                .thenReturn(CouponIssuePreflight.pending());
    }

    /** executeAndRecord 가 넘겨받은 작업을 그대로 실행하고 결과를 담아 돌려주게 한다. */
    private void recordingOperation() {
        when(operationService.executeAndRecord(
                eq(KEY), eq(20L), any(), eq(AT), any(), eq(issueCodec), any()
        )).thenAnswer(invocation -> {
            java.util.function.Supplier<CouponIssueResult> operation =
                    invocation.getArgument(4);
            return new IdempotentOperationService.RecordedExecution<>(
                    operation.get()
            );
        });
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
