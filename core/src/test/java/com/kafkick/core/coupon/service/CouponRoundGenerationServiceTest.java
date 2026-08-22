package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupontemplate.domain.CouponDayOfWeek;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponRoundAlreadyExistsException;
import com.kafkick.core.coupon.exception.CouponRoundScheduleConflictException;
import com.kafkick.core.coupon.service.result.CouponRoundGenerationResult;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// 활성 템플릿 반복 규칙의 회차 계산, 중복과 전역 충돌 건너뛰기를 검증합니다.
@ExtendWith(MockitoExtension.class)
class CouponRoundGenerationServiceTest {

    @Mock
    private CouponTemplateRepository couponTemplateRepository;

    @Mock
    private CouponRoundCreationService couponRoundCreationService;

    private CouponRoundGenerationService couponRoundGenerationService;

    @BeforeEach
    void setUp() {
        couponRoundGenerationService = new CouponRoundGenerationService(
                couponTemplateRepository,
                couponRoundCreationService,
                ZoneId.of("Asia/Seoul"),
                30
        );
    }

    @Test
    @DisplayName("기간 안의 매월 두 번째 화요일 회차와 최초 재고를 생성한다")
    void generateCouponRoundsInDateRange() {
        CouponTemplate template = template(100L, true);
        when(couponTemplateRepository.findAllActiveByIdAsc())
                .thenReturn(List.of(template));
        Instant generatedAt = Instant.parse("2026-08-18T00:00:00Z");

        CouponRoundGenerationResult result = couponRoundGenerationService
                .generate(
                        LocalDate.of(2026, 8, 10),
                        LocalDate.of(2026, 9, 9),
                        generatedAt
                );

        ArgumentCaptor<CouponRound> roundCaptor =
                ArgumentCaptor.forClass(CouponRound.class);
        ArgumentCaptor<CouponStock> stockCaptor =
                ArgumentCaptor.forClass(CouponStock.class);
        verify(couponRoundCreationService, org.mockito.Mockito.times(2))
                .create(
                        roundCaptor.capture(),
                        stockCaptor.capture()
                );

        assertThat(roundCaptor.getAllValues())
                .extracting(CouponRound::openAt)
                .containsExactly(
                        Instant.parse("2026-08-11T05:00:00Z"),
                        Instant.parse("2026-09-08T05:00:00Z")
                );
        assertThat(roundCaptor.getAllValues())
                .extracting(CouponRound::closeAt)
                .containsExactly(
                        Instant.parse("2026-08-11T07:00:00Z"),
                        Instant.parse("2026-09-08T07:00:00Z")
                );
        assertThat(roundCaptor.getAllValues())
                .allSatisfy(round -> {
                    assertThat(round.templateId()).isEqualTo(100L);
                    assertThat(round.discountAmount()).isEqualTo(5_000);
                    assertThat(round.eligibleGrades()).containsExactly(
                            MembershipGrade.GOLD,
                            MembershipGrade.VIP
                    );
                });
        assertThat(stockCaptor.getAllValues())
                .allSatisfy(stock -> {
                    assertThat(stock.totalQuantity()).isEqualTo(10_000);
                    assertThat(stock.activeCount()).isZero();
                    assertThat(stock.updatedAt()).isEqualTo(generatedAt);
                });
        assertThat(result).isEqualTo(
                new CouponRoundGenerationResult(2, 2, 0, 0)
        );
    }

    @Test
    @DisplayName("DB 유니크 제약으로 감지한 기존 회차는 정상적으로 건너뛴다")
    void skipDuplicateCouponRound() {
        when(couponTemplateRepository.findAllActiveByIdAsc())
                .thenReturn(List.of(template(100L, true)));
        doThrow(new CouponRoundAlreadyExistsException("중복", null))
                .when(couponRoundCreationService)
                .create(any(), any());

        CouponRoundGenerationResult result = couponRoundGenerationService
                .generate(
                        LocalDate.of(2026, 9, 1),
                        LocalDate.of(2026, 9, 30),
                        Instant.parse("2026-08-18T00:00:00Z")
                );

        assertThat(result).isEqualTo(
                new CouponRoundGenerationResult(1, 0, 1, 0)
        );
    }

    @Test
    @DisplayName("다른 브랜드 회차와 시간이 겹치는 반복 예약은 정상적으로 건너뛴다")
    void skipConflictingCouponRound() {
        when(couponTemplateRepository.findAllActiveByIdAsc())
                .thenReturn(List.of(template(100L, true)));
        doThrow(new CouponRoundScheduleConflictException("시간 충돌"))
                .when(couponRoundCreationService)
                .create(any(), any());

        CouponRoundGenerationResult result = couponRoundGenerationService
                .generate(
                        LocalDate.of(2026, 9, 1),
                        LocalDate.of(2026, 9, 30),
                        Instant.parse("2026-08-18T00:00:00Z")
                );

        assertThat(result).isEqualTo(
                new CouponRoundGenerationResult(1, 0, 0, 1)
        );
    }

    @Test
    @DisplayName("생성 종료일이 시작일보다 빠르면 거부한다")
    void rejectInvalidDateRange() {
        assertThatThrownBy(() -> couponRoundGenerationService.generate(
                LocalDate.of(2026, 9, 2),
                LocalDate.of(2026, 9, 1),
                Instant.parse("2026-08-18T00:00:00Z")
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("회차 생성 종료일은 시작일보다 빠를 수 없습니다.");
    }

    @Test
    @DisplayName("최대 생성 기간 30일을 초과하는 범위를 거부한다")
    void rejectTooLongDateRange() {
        assertThatThrownBy(() -> couponRoundGenerationService.generate(
                LocalDate.of(2026, 8, 18),
                LocalDate.of(2026, 9, 18),
                Instant.parse("2026-08-18T00:00:00Z")
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("회차 생성 기간이 허용 범위를 초과했습니다.");
    }

    @Test
    @DisplayName("정확히 30일 이후까지의 생성 범위를 허용한다")
    void acceptMaximumDateRange() {
        when(couponTemplateRepository.findAllActiveByIdAsc())
                .thenReturn(List.of());

        CouponRoundGenerationResult result = couponRoundGenerationService
                .generate(
                        LocalDate.of(2026, 8, 18),
                        LocalDate.of(2026, 9, 17),
                        Instant.parse("2026-08-18T00:00:00Z")
                );

        assertThat(result).isEqualTo(
                new CouponRoundGenerationResult(0, 0, 0, 0)
        );
    }

    private CouponTemplate template(Long id, boolean active) {
        return CouponTemplate.restore(
                id,
                1L,
                "골드 VIP 5천원 할인",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                2,
                CouponDayOfWeek.TUE,
                LocalTime.of(14, 0),
                2,
                10_000,
                Set.of(MembershipGrade.GOLD, MembershipGrade.VIP),
                active
        );
    }
}
