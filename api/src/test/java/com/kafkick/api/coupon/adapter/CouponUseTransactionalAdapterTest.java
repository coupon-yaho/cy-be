// 멱등키 최초 처리·동일 응답 재생·다른 요청 재사용 거부를 검증합니다.
package com.kafkick.api.coupon.adapter;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.coupon.dto.CouponUseRequest;
import com.kafkick.api.coupon.dto.CouponUseResponse;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.service.CouponUseCommand;
import com.kafkick.core.coupon.service.CouponUseResult;
import com.kafkick.core.coupon.service.CouponUseService;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponUseTransactionalAdapterTest {

    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final Instant USED_AT =
            Instant.parse("2026-08-20T05:00:00Z");

    @Mock
    private CouponUseService couponUseService;

    @Mock
    private IdempotencyRepository idempotencyRepository;

    @Mock
    private TimeProvider timeProvider;

    @Mock
    private ObjectMapper objectMapper;

    private CouponUseTransactionalAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new CouponUseTransactionalAdapter(
                couponUseService,
                idempotencyRepository,
                timeProvider,
                objectMapper
        );
    }

    @Test
    @DisplayName("최초 멱등키는 사용 처리 후 응답과 대상을 DONE으로 저장한다")
    void processFirstRequest() throws Exception {
        when(timeProvider.instant()).thenReturn(USED_AT);
        CouponUseRequest request = new CouponUseRequest(30L, 20_000);
        CouponUseResult result = result();
        when(idempotencyRepository.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(USED_AT)
        )).thenReturn(true);
        when(couponUseService.use(any(CouponUseCommand.class)))
                .thenReturn(result);
        when(objectMapper.writeValueAsString(any(CouponUseResponse.class)))
                .thenReturn("stored-response");

        CouponUseResponse response = adapter.use(
                100L,
                20L,
                IDEMPOTENCY_KEY,
                request
        );

        assertThat(response.discountAmount()).isEqualTo(5_000);
        ArgumentCaptor<CouponUseCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponUseCommand.class);
        verify(couponUseService).use(commandCaptor.capture());
        assertThat(commandCaptor.getValue().issuanceId()).isEqualTo(100L);
        assertThat(commandCaptor.getValue().memberId()).isEqualTo(20L);
        assertThat(commandCaptor.getValue().orderId()).isEqualTo(30L);
        assertThat(commandCaptor.getValue().orderAmount()).isEqualTo(20_000);
        assertThat(commandCaptor.getValue().idempotencyKey())
                .isEqualTo(IDEMPOTENCY_KEY);
        verify(idempotencyRepository).complete(
                IDEMPOTENCY_KEY,
                20L,
                100L,
                "stored-response"
        );
    }

    @Test
    @DisplayName("같은 멱등키와 요청은 최초 저장 응답을 그대로 반환한다")
    void replayCompletedRequest() throws Exception {
        when(timeProvider.instant()).thenReturn(USED_AT);
        CouponUseRequest request = new CouponUseRequest(30L, 20_000);
        CouponUseResponse storedResponse = CouponUseResponse.from(result());
        AtomicReference<String> requestHash = new AtomicReference<>();
        when(idempotencyRepository.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(USED_AT)
        )).thenAnswer(invocation -> {
            requestHash.set(invocation.getArgument(1));
            return false;
        });
        when(idempotencyRepository.findByKey(IDEMPOTENCY_KEY))
                .thenAnswer(invocation -> Optional.of(new IdempotencyRecord(
                        IDEMPOTENCY_KEY,
                        20L,
                        100L,
                        requestHash.get(),
                        IdempotencyStatus.DONE,
                        "stored-response",
                        USED_AT
                )));
        when(objectMapper.readValue(
                "stored-response",
                CouponUseResponse.class
        )).thenReturn(storedResponse);

        CouponUseResponse response = adapter.use(
                100L,
                20L,
                IDEMPOTENCY_KEY,
                request
        );

        assertThat(response).isEqualTo(storedResponse);
        verify(couponUseService, never()).use(any());
        verify(idempotencyRepository, never()).complete(
                anyString(), any(), any(), anyString()
        );
    }

    @Test
    @DisplayName("같은 멱등키를 다른 요청에 재사용하면 422로 거부한다")
    void rejectReusedKeyForDifferentRequest() {
        when(timeProvider.instant()).thenReturn(USED_AT);
        when(idempotencyRepository.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(USED_AT)
        )).thenReturn(false);
        when(idempotencyRepository.findByKey(IDEMPOTENCY_KEY))
                .thenReturn(Optional.of(new IdempotencyRecord(
                        IDEMPOTENCY_KEY,
                        20L,
                        100L,
                        "0".repeat(64),
                        IdempotencyStatus.DONE,
                        "stored-response",
                        USED_AT
                )));

        assertThatThrownBy(() -> adapter.use(
                100L,
                20L,
                IDEMPOTENCY_KEY,
                new CouponUseRequest(30L, 20_000)
        )).isInstanceOfSatisfying(
                BusinessException.class,
                exception -> assertThat(exception.getErrorCode())
                        .isEqualTo(
                                CouponUseErrorCode.IDEMPOTENCY_KEY_REUSED
                        )
        );
        verify(couponUseService, never()).use(any());
    }

    @Test
    @DisplayName("UUID v4가 아닌 멱등키는 처리 전에 거부한다")
    void rejectInvalidIdempotencyKey() {
        assertThatThrownBy(() -> adapter.use(
                100L,
                20L,
                "not-a-uuid",
                new CouponUseRequest(30L, 20_000)
        )).isInstanceOfSatisfying(
                BusinessException.class,
                exception -> assertThat(exception.getErrorCode())
                        .isEqualTo(
                                CouponUseErrorCode.INVALID_COUPON_USE_REQUEST
                        )
        );
        verify(idempotencyRepository, never()).tryStart(
                anyString(), anyString(), any()
        );
    }

    private CouponUseResult result() {
        return new CouponUseResult(
                100L,
                IssuanceStatus.USED,
                30L,
                5_000,
                USED_AT
        );
    }
}
