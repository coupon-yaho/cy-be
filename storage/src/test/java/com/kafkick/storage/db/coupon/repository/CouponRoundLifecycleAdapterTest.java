package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.exception.CouponPersistenceException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponRoundLifecycleAdapterTest {

    @Mock
    private CouponRoundJpaRepository couponRoundJpaRepository;

    @Test
    @DisplayName("선택한 OPEN 회차와 실제 종료 건수가 다르면 거부한다")
    void rejectWhenSelectedAndUpdatedCountsDiffer() {
        Instant asOf = Instant.parse("2026-09-08T07:00:00Z");
        List<Long> selectedIds = List.of(11L, 12L);
        when(couponRoundJpaRepository.findClosableOpenRoundIds(asOf))
                .thenReturn(selectedIds);
        when(couponRoundJpaRepository.closeOpenRoundsByIds(selectedIds, asOf))
                .thenReturn(1);
        CouponRoundLifecycleAdapter adapter =
                new CouponRoundLifecycleAdapter(couponRoundJpaRepository);

        assertThatThrownBy(() -> adapter.closeOpenRounds(asOf))
                .isInstanceOf(CouponPersistenceException.class);
    }
}
