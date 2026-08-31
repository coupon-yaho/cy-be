package com.kafkick.api.observation.issuance;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionOperations;

import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.code.CouponCodeGenerator;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.coupon.v2.RequestTokenGenerator;
import com.kafkick.core.coupon.v2.V2CouponIssueResult;
import com.kafkick.core.coupon.v2.V2CouponIssueService;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.observation.EngineVersion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 발급 경로는 파손({@code -8})을 받아도 회수 스크립트를 부르지 않는다(13). */
class V2CorruptValueReclaimTest {

    @Test
    @SuppressWarnings("unchecked")
    void corruptValueDoesNotTriggerReclaim() {
        IssuanceGatePort gate = mock(IssuanceGatePort.class);
        when(gate.claim(any())).thenReturn(ClaimResult.rejected(ClaimOutcome.CORRUPT_VALUE));
        V2CouponIssueService service = new V2CouponIssueService(
                gate,
                mock(IssuanceRepository.class),
                mock(IssuanceHistoryRepository.class),
                mock(IdempotencyRepository.class),
                mock(com.kafkick.core.coupon.port.CouponStockRepository.class),
                mock(CouponCodeGenerator.class),
                mock(IdempotencyResultCodec.class),
                new RequestTokenGenerator("api-1"),
                mock(TransactionOperations.class)
        );

        V2CouponIssueResult result = service.issue(
                new CouponIssueCommand(10L, 20L, MembershipGrade.GOLD,
                        "550e8400-e29b-41d4-a716-446655440000",
                        Instant.parse("2026-08-28T05:00:00Z")),
                new CouponRoundIssuanceDefinition(10L, 30, EngineVersion.V2));

        assertThat(result.claimResult().outcome()).isEqualTo(ClaimOutcome.CORRUPT_VALUE);
        verify(gate, never()).reclaimCorrupt(anyLong(), anyLong(), anyBoolean(), anyLong());
        verify(gate, never()).compensate(anyLong(), anyLong(), any());
    }
}
