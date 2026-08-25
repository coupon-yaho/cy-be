package com.kafkick.storage.db.admin.issuancehistory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery.HistoryPosition;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.storage.db.coupon.repository.IssuanceHistoryJpaRepository;

@ExtendWith(MockitoExtension.class)
class JpaAdminIssuanceHistoryReaderTest {
    @Test
    void passesFiltersKeysetAndLimitPlusOneToRepository() {
        IssuanceHistoryJpaRepository repository = mock(IssuanceHistoryJpaRepository.class);
        AdminIssuanceHistoryRowProjection row = mock(AdminIssuanceHistoryRowProjection.class);
        AdminIssuanceHistorySummaryProjection summary = mock(AdminIssuanceHistorySummaryProjection.class);
        Instant snapshotAt = Instant.parse("2026-08-26T00:00:00Z");
        Instant beforeAt = Instant.parse("2026-08-25T00:00:00Z");
        when(row.getHistoryId()).thenReturn(10L); when(row.getIssuanceId()).thenReturn(20L);
        when(row.getIssuanceCode()).thenReturn("ABCD1234EFGH5678"); when(row.getCouponId()).thenReturn(30L);
        when(row.getToStatus()).thenReturn(IssuanceStatus.ISSUED); when(row.getEventType()).thenReturn(IssuanceEventType.ISSUE);
        when(row.getOccurredAt()).thenReturn(snapshotAt); when(repository.findAdminHistoryRows(any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(List.of(row));
        when(repository.summarizeAdminHistoryRows(any(), any(), any(), any(), any())).thenReturn(summary);
        AdminIssuanceHistoryQuery query = new AdminIssuanceHistoryQuery(30L, beforeAt.minusSeconds(10), snapshotAt,
                IssuanceEventType.ISSUE, new HistoryPosition(beforeAt, 9L), 7);

        JpaAdminIssuanceHistoryReader reader = new JpaAdminIssuanceHistoryReader(repository);
        assertThat(reader.read(query, snapshotAt).candidates()).hasSize(1);

        ArgumentCaptor<Pageable> pageable = ArgumentCaptor.forClass(Pageable.class);
        verify(repository).findAdminHistoryRows(eq(30L), eq(beforeAt.minusSeconds(10)), eq(snapshotAt),
                eq(IssuanceEventType.ISSUE), eq(snapshotAt), eq(beforeAt), eq(9L), pageable.capture());
        verify(repository).summarizeAdminHistoryRows(eq(30L), eq(beforeAt.minusSeconds(10)), eq(snapshotAt),
                eq(IssuanceEventType.ISSUE), eq(snapshotAt));
        assertThat(pageable.getValue().getPageSize()).isEqualTo(8);
    }
}
