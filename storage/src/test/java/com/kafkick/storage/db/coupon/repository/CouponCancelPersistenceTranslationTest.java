package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

import jakarta.persistence.EntityManager;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.dao.DataAccessResourceFailureException;

import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceHistory;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponPersistenceException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

// storage가 업무 이벤트와 무관한 공통 영속성 예외만 반환하는지 검증합니다.

@ExtendWith(MockitoExtension.class)
class CouponCancelPersistenceTranslationTest {

    private static final Instant CANCELED_AT =
            Instant.parse("2026-08-20T05:00:00Z");

    @Mock
    private IssuanceJpaRepository issuanceJpaRepository;

    @Mock
    private IssuanceHistoryJpaRepository historyJpaRepository;

    @Mock
    private EntityManager entityManager;

    @Test
    @DisplayName("CANCEL 상태 저장 실패를 공통 쿠폰 영속성 예외로 변환한다")
    void translateCancelStatusPersistenceFailure() {
        IssuanceRepositoryImpl repository = new IssuanceRepositoryImpl(
                issuanceJpaRepository,
                entityManager
        );
        when(issuanceJpaRepository.updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.ISSUED.name(),
                IssuanceStatus.CANCELLED.name(),
                CANCELED_AT
        )).thenThrow(new DataAccessResourceFailureException("db failed"));

        assertThatThrownBy(() -> repository.updateStatusIfCurrent(
                100L,
                20L,
                IssuanceStatus.ISSUED,
                IssuanceStatus.CANCELLED,
                CANCELED_AT
        )).isInstanceOf(CouponPersistenceException.class);
    }

    @Test
    @DisplayName("CANCEL 이력 저장 실패를 공통 쿠폰 영속성 예외로 변환한다")
    void translateCancelHistoryPersistenceFailure() {
        IssuanceHistoryRepositoryImpl repository =
                new IssuanceHistoryRepositoryImpl(historyJpaRepository);
        when(historyJpaRepository.saveAndFlush(any()))
                .thenThrow(new DataAccessResourceFailureException(
                        "db failed"
                ));
        IssuanceHistory history = new IssuanceHistory(
                null,
                100L,
                IssuanceEventType.CANCEL,
                IssuanceStatus.ISSUED,
                IssuanceStatus.CANCELLED,
                "회원 요청으로 발급 취소",
                "550e8400-e29b-41d4-a716-446655440002",
                CANCELED_AT
        );

        assertThatThrownBy(() -> repository.save(history))
                .isInstanceOf(CouponPersistenceException.class);
    }
}
