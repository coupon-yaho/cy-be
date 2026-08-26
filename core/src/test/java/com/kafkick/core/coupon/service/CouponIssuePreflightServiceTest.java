package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponIssuePreflightServiceTest {

    private static final String KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final String REQUEST_HASH = "a".repeat(64);
    private static final String OTHER_REQUEST_HASH = "b".repeat(64);

    @Mock
    private IdempotencyRepository idempotencyRepository;
    @Mock
    private CouponIssuePolicyValidator policyValidator;

    @Test
    void rejectsConcurrentReuseWhenCommittedRequestHashIsDifferent() {
        when(idempotencyRepository.findByKey(KEY)).thenReturn(Optional.of(
                new IdempotencyRecord(
                        KEY,
                        20L,
                        30L,
                        OTHER_REQUEST_HASH,
                        IdempotencyStatus.DONE,
                        "stored-body",
                        Instant.parse("2026-08-20T05:00:00Z")
                )
        ));

        assertThatThrownBy(() -> service().findCompletedResponse(
                KEY,
                REQUEST_HASH
        )).isInstanceOfSatisfying(
                BusinessException.class,
                exception -> org.assertj.core.api.Assertions.assertThat(
                        exception.getErrorCode()
                ).isEqualTo(CouponUseErrorCode.IDEMPOTENCY_KEY_REUSED)
        );
    }

    private CouponIssuePreflightService service() {
        return new CouponIssuePreflightService(
                idempotencyRepository,
                policyValidator
        );
    }
}
