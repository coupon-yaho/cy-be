package com.kafkick.api.coupon.controller;

import java.time.Instant;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.support.auth.admin.AdminRequestHeaders;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponRoundErrorCode;
import com.kafkick.core.coupon.service.command.CouponRoundReservationCommand;
import com.kafkick.core.coupon.service.CouponRoundReservationService;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(CouponRoundAdminController.class)
class CouponRoundAdminControllerTest {

    private static final Instant GENERATED_AT =
            Instant.parse("2026-08-20T00:00:00Z");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CouponRoundReservationService reservationService;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    @DisplayName("단발성 쿠폰 회차를 예약하면 201과 Location을 반환한다")
    void reserveOneTimeCouponRound() throws Exception {
        when(timeProvider.instant()).thenReturn(GENERATED_AT);
        when(reservationService.reserve(
                any(CouponRoundReservationCommand.class)
        )).thenReturn(savedRound());

        mockMvc.perform(post(
                        "/api/v1/admin/coupon-templates/10/rounds"
                )
                        .header(AdminRequestHeaders.USER_ROLE, "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "openAt": "2026-08-25T00:00:00Z",
                                  "closeAt": "2026-08-25T03:00:00Z"
                                }
                                """))
                .andExpect(status().isCreated())
                .andExpect(header().string(
                        "Location",
                        "/api/v1/admin/coupon-rounds/20"
                ))
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.id").value(20))
                .andExpect(jsonPath("$.data.templateId").value(10))
                .andExpect(jsonPath("$.data.brandId").value(2))
                .andExpect(jsonPath("$.data.status").value("SCHEDULED"))
                .andExpect(jsonPath("$.data.openAt")
                        .value("2026-08-25T00:00:00Z"))
                .andExpect(jsonPath("$.data.closeAt")
                        .value("2026-08-25T03:00:00Z"));

        ArgumentCaptor<CouponRoundReservationCommand> commandCaptor =
                ArgumentCaptor.forClass(
                        CouponRoundReservationCommand.class
                );
        verify(reservationService).reserve(commandCaptor.capture());
        assertThat(commandCaptor.getValue().templateId()).isEqualTo(10L);
        assertThat(commandCaptor.getValue().openAt()).isEqualTo(
                Instant.parse("2026-08-25T00:00:00Z")
        );
        assertThat(commandCaptor.getValue().closeAt()).isEqualTo(
                Instant.parse("2026-08-25T03:00:00Z")
        );
        assertThat(commandCaptor.getValue().generatedAt())
                .isEqualTo(GENERATED_AT);
    }

    @Test
    @DisplayName("종료 시각이 시작 시각과 같으면 400을 반환한다")
    void rejectInvalidOneTimeSchedule() throws Exception {
        when(timeProvider.instant()).thenReturn(GENERATED_AT);

        mockMvc.perform(post(
                        "/api/v1/admin/coupon-templates/10/rounds"
                )
                        .header(AdminRequestHeaders.USER_ROLE, "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "openAt": "2026-08-25T00:00:00Z",
                                  "closeAt": "2026-08-25T00:00:00Z"
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));
    }

    @Test
    @DisplayName("다른 회차와 예약 시간이 겹치면 409를 반환한다")
    void rejectOverlappingOneTimeSchedule() throws Exception {
        when(timeProvider.instant()).thenReturn(GENERATED_AT);
        when(reservationService.reserve(
                any(CouponRoundReservationCommand.class)
        )).thenThrow(new BusinessException(
                CouponRoundErrorCode.COUPON_ROUND_SCHEDULE_CONFLICT
        ));

        mockMvc.perform(post(
                        "/api/v1/admin/coupon-templates/10/rounds"
                )
                        .header(AdminRequestHeaders.USER_ROLE, "ADMIN")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "openAt": "2026-08-25T00:00:00Z",
                                  "closeAt": "2026-08-25T03:00:00Z"
                                }
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code")
                        .value("COUPON_ROUND-202"));
    }

    private CouponRound savedRound() {
        return CouponRound.restore(
                20L,
                10L,
                2L,
                "브랜드 2 단발성 쿠폰",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                7,
                Set.of(MembershipGrade.GOLD),
                Instant.parse("2026-08-25T00:00:00Z"),
                Instant.parse("2026-08-25T03:00:00Z"),
                CouponRoundStatus.SCHEDULED,
                GENERATED_AT
        );
    }
}
