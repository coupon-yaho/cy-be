package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponRoundScheduleConflictException;
import com.kafkick.core.coupon.exception.CouponRoundAlreadyExistsException;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponRoundScheduleLockPort;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponRoundCreationServiceTest {

    private static final Instant OPEN_AT =
            Instant.parse("2026-08-25T00:00:00Z");
    private static final Instant CLOSE_AT =
            Instant.parse("2026-08-25T03:00:00Z");
    private static final Instant GENERATED_AT =
            Instant.parse("2026-08-20T00:00:00Z");

    @Mock
    private CouponRoundRepository couponRoundRepository;

    @Mock
    private CouponRoundScheduleLockPort scheduleLockPort;

    private CouponRoundCreationService service;

    @BeforeEach
    void setUp() {
        service = new CouponRoundCreationService(
                couponRoundRepository,
                scheduleLockPort
        );
    }

    @Test
    @DisplayName("전역 잠금과 충돌 확인 후 회차와 초기 재고를 저장한다")
    void createAfterLockAndConflictCheck() {
        CouponRound round = scheduledRound();
        CouponStock stock = CouponStock.initialize(100, GENERATED_AT);
        CouponRound saved = restoredRound(1L);
        when(couponRoundRepository.existsOverlappingSchedule(
                OPEN_AT,
                CLOSE_AT
        )).thenReturn(false);
        when(couponRoundRepository.saveWithInitialStock(round, stock))
                .thenReturn(saved);

        CouponRound result = service.create(round, stock);

        assertThat(result).isEqualTo(saved);
        InOrder order = inOrder(scheduleLockPort, couponRoundRepository);
        order.verify(scheduleLockPort).lock();
        order.verify(couponRoundRepository).existsByTemplateIdAndOpenAt(
                round.templateId(),
                round.openAt()
        );
        order.verify(couponRoundRepository).existsOverlappingSchedule(
                OPEN_AT,
                CLOSE_AT
        );
        order.verify(couponRoundRepository).saveWithInitialStock(round, stock);
    }

    @Test
    @DisplayName("다른 브랜드의 예약과 시간이 겹치면 저장하지 않는다")
    void rejectOverlappingGlobalSchedule() {
        CouponRound round = scheduledRound();
        CouponStock stock = CouponStock.initialize(100, GENERATED_AT);
        when(couponRoundRepository.existsOverlappingSchedule(
                OPEN_AT,
                CLOSE_AT
        )).thenReturn(true);

        assertThatThrownBy(() -> service.create(round, stock))
                .isInstanceOf(CouponRoundScheduleConflictException.class);

        verify(scheduleLockPort).lock();
        verify(couponRoundRepository, never())
                .saveWithInitialStock(round, stock);
    }

    @Test
    @DisplayName("이미 생성된 같은 템플릿 회차는 충돌이 아니라 중복으로 구분한다")
    void distinguishExistingOccurrenceFromScheduleConflict() {
        CouponRound round = scheduledRound();
        CouponStock stock = CouponStock.initialize(100, GENERATED_AT);
        when(couponRoundRepository.existsByTemplateIdAndOpenAt(
                round.templateId(), round.openAt()
        )).thenReturn(true);

        assertThatThrownBy(() -> service.create(round, stock))
                .isInstanceOf(CouponRoundAlreadyExistsException.class);

        verify(couponRoundRepository, never())
                .existsOverlappingSchedule(OPEN_AT, CLOSE_AT);
        verify(couponRoundRepository, never())
                .saveWithInitialStock(round, stock);
    }

    private CouponRound scheduledRound() {
        return new CouponRound(
                null,
                10L,
                2L,
                "브랜드 2 단발성 쿠폰",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                Set.of(MembershipGrade.GOLD),
                OPEN_AT,
                CLOSE_AT,
                CouponRoundStatus.SCHEDULED,
                GENERATED_AT
        );
    }

    private CouponRound restoredRound(Long id) {
        CouponRound round = scheduledRound();
        return CouponRound.restore(
                id,
                round.templateId(),
                round.brandId(),
                round.name(),
                round.policyType(),
                round.discountRate(),
                round.maxDiscountAmount(),
                round.discountAmount(),
                round.validDays(),
                round.eligibleGrades(),
                round.openAt(),
                round.closeAt(),
                round.status(),
                round.generatedAt()
        );
    }
}
