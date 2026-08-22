package com.kafkick.core.coupon.service;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.command.CouponUseCommand;
import com.kafkick.core.coupon.service.idempotency.IdempotencyExecutionService;
import com.kafkick.core.coupon.service.idempotency.IdempotentOperationService;
import com.kafkick.core.coupon.service.result.CouponCancelResult;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.service.result.CouponUseResult;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
        when(idempotencyExecutionService.execute(
                eq(KEY), any(), any(), any(), any()
        )).thenAnswer(invocation -> {
            java.util.function.Function<Instant, CouponIssueResult> claimed =
                    invocation.getArgument(3);
            return claimed.apply(AT);
        });
        when(operationService.execute(
                eq(KEY), eq(20L), eq(10L), eq(AT), any(), eq(issueCodec)
        )).thenAnswer(invocation -> {
            java.util.function.Supplier<CouponIssueResult> operation =
                    invocation.getArgument(4);
            return operation.get();
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
        when(idempotencyExecutionService.execute(
                eq(KEY), any(), any(), any(), any()
        )).thenAnswer(invocation -> {
            java.util.function.Function<String, CouponIssueResult> replay =
                    invocation.getArgument(4);
            return replay.apply("stored-result");
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
                eq(KEY), eq(20L), eq(100L), eq(AT), any(), eq(useCodec)
        )).thenAnswer(invocation -> {
            java.util.function.Supplier<CouponUseResult> operation =
                    invocation.getArgument(4);
            return operation.get();
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
                eq(KEY), eq(20L), eq(100L), eq(AT), any(), eq(useCodec)
        )).thenAnswer(invocation -> {
            java.util.function.Supplier<CouponUseResult> operation =
                    invocation.getArgument(4);
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
                couponUseService,
                couponCancelUseService,
                couponCancelService,
                issueCodec,
                useCodec,
                cancelUseCodec,
                cancelCodec
        );
    }
}
