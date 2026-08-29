package com.kafkick.core.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.notification.domain.NotificationFailure;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

@ExtendWith(MockitoExtension.class)
class NotificationQueryServiceTest {
    private static final Instant AT = Instant.parse("2026-08-29T00:00:00Z");

    @Mock NotificationRepository notifications;
    @Mock CouponRoundRepository couponRounds;
    private NotificationQueryService service;

    @BeforeEach
    void setUp() {
        service = new NotificationQueryService(notifications, couponRounds,
                new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC)));
    }

    @Test
    void calculatesGlobalSummaryFromAuthoritativeCounts() {
        when(notifications.countAll()).thenReturn(10L);
        when(notifications.countByStatusIn(List.of(NotificationStatus.SENT))).thenReturn(6L);
        when(notifications.countByStatusIn(List.of(NotificationStatus.FAILED, NotificationStatus.DEAD)))
                .thenReturn(3L);
        when(notifications.countByStatusIn(List.of(NotificationStatus.PENDING, NotificationStatus.SENDING)))
                .thenReturn(1L);

        NotificationSummary summary = service.getSummary(null);

        assertThat(summary.totalRequests().value()).isEqualTo(10L);
        assertThat(summary.sentCount().value()).isEqualTo(6L);
        assertThat(summary.sentRate().value()).isEqualTo(0.6d);
        assertThat(summary.sentRate().state()).isEqualTo(SourceStatus.VALID);
        assertThat(summary.snapshotAt()).isEqualTo(AT);
    }

    @Test
    void existingCouponWithoutNotificationsReturnsNoTrafficCountsAndUndefinedRate() {
        when(couponRounds.findById(9L)).thenReturn(Optional.of(org.mockito.Mockito.mock(CouponRound.class)));
        when(notifications.countByCouponId(9L)).thenReturn(0L);
        when(notifications.countByCouponIdAndStatusIn(9L, List.of(NotificationStatus.SENT))).thenReturn(0L);
        when(notifications.countByCouponIdAndStatusIn(9L,
                List.of(NotificationStatus.FAILED, NotificationStatus.DEAD))).thenReturn(0L);
        when(notifications.countByCouponIdAndStatusIn(9L,
                List.of(NotificationStatus.PENDING, NotificationStatus.SENDING))).thenReturn(0L);

        NotificationSummary summary = service.getSummary(9L);

        assertThat(summary.totalRequests().state()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(summary.totalRequests().value()).isZero();
        assertThat(summary.sentRate().state()).isEqualTo(SourceStatus.N_A);
        assertThat(summary.sentRate().value()).isNull();
        assertThat(summary.snapshotAt()).isEqualTo(AT);
    }

    @Test
    void nonexistentCouponReturnsNaWithoutInventingSnapshot() {
        when(couponRounds.findById(9L)).thenReturn(Optional.empty());

        NotificationSummary summary = service.getSummary(9L);

        assertThat(summary.totalRequests().state()).isEqualTo(SourceStatus.N_A);
        assertThat(summary.totalRequests().value()).isNull();
        assertThat(summary.snapshotAt()).isNull();
    }

    @Test
    void repositoryFailureReturnsUnavailable() {
        when(notifications.countAll()).thenThrow(new IllegalStateException("db down"));

        NotificationSummary summary = service.getSummary(null);

        assertThat(summary.totalRequests().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(summary.sentRate().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(summary.snapshotAt()).isNull();
    }

    @Test
    void failurePageUsesLimitPlusOneAndCursorFromLastPublicItem() {
        NotificationFailure newest = failure(30L);
        NotificationFailure middle = failure(20L);
        NotificationFailure older = failure(10L);
        when(notifications.findFailuresBeforeId(null, 3))
                .thenReturn(List.of(newest, middle, older));

        NotificationFailurePage page = service.getFailures(null, 2);

        assertThat(page.items()).containsExactly(newest, middle);
        assertThat(page.hasOlder()).isTrue();
        assertThat(page.nextBeforeId()).isEqualTo(20L);
    }

    @Test
    void exactLastPageHasNoCursor() {
        NotificationFailure item = failure(10L);
        when(notifications.findFailuresBeforeId(20L, 201)).thenReturn(List.of(item));

        NotificationFailurePage page = service.getFailures(20L, 200);

        assertThat(page.items()).containsExactly(item);
        assertThat(page.hasOlder()).isFalse();
        assertThat(page.nextBeforeId()).isNull();
    }

    @Test
    void absentSummaryMetricRejectsEitherStrayValueOrTimestamp() {
        assertThatThrownBy(() -> new NotificationSummary.Metric<>(
                1L, SourceStatus.N_A, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NotificationSummary.Metric<Long>(
                null, SourceStatus.N_A, AT)).isInstanceOf(IllegalArgumentException.class);
    }

    private static NotificationFailure failure(long id) {
        return new NotificationFailure(id, 1L, 2L, NotifyFailureReason.SEND_TIMEOUT, 4, AT);
    }
}
