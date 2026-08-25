package com.kafkick.api.coupon.controller;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.query.BrandDayCalendarEntry;
import com.kafkick.core.coupon.query.BrandDaySchedule;
import com.kafkick.core.coupon.service.BrandDayCalendarQueryService;
import com.kafkick.core.coupon.service.BrandDayQueryService;
import com.kafkick.core.coupontemplate.domain.CouponDayOfWeek;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.TimeProvider;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(BrandDayController.class)
class BrandDayControllerTest {

    private static final Instant AS_OF =
            Instant.parse("2026-08-10T00:00:00Z");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private BrandDayQueryService brandDayQueryService;

    @MockitoBean
    private BrandDayCalendarQueryService calendarQueryService;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    @DisplayName("브랜드 데이 반복 규칙 목록을 조회한다")
    void findBrandDays() throws Exception {
        when(brandDayQueryService.findAll()).thenReturn(List.of(
                new BrandDaySchedule(
                        1L, 2L, "골드 할인", 2,
                        CouponDayOfWeek.MON, LocalTime.of(10, 0), 2,
                        Set.of(MembershipGrade.GOLD)
                )
        ));

        mockMvc.perform(get("/api/v1/brand-days"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].templateId").value(1))
                .andExpect(jsonPath("$.data[0].dayOfWeek").value("MON"))
                .andExpect(jsonPath("$.data[0].eligibleGradesMask").value(4));
    }

    @Test
    @DisplayName("기간 내 브랜드 데이 달력을 조회한다")
    void findCalendar() throws Exception {
        LocalDate from = LocalDate.of(2026, 8, 3);
        LocalDate to = LocalDate.of(2026, 8, 30);
        when(timeProvider.instant()).thenReturn(AS_OF);
        when(calendarQueryService.findBetween(from, to, AS_OF))
                .thenReturn(List.of(new BrandDayCalendarEntry(
                        1L, 2L, "골드 할인",
                        CouponPolicyType.FIXED_AMOUNT,
                        null, null, 5_000,
                        Set.of(MembershipGrade.GOLD),
                        Instant.parse("2026-08-10T01:00:00Z"),
                        Instant.parse("2026-08-10T03:00:00Z"),
                        CouponRoundStatus.OPEN,
                        100L, 100, 20
                )));

        mockMvc.perform(get("/api/v1/calendar")
                        .param("from", "2026-08-03")
                        .param("to", "2026-08-30"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].couponRoundId").value(100))
                .andExpect(jsonPath("$.data[0].activeCount").value(20))
                .andExpect(jsonPath("$.data[0].dataGrantMb").isEmpty())
                .andExpect(jsonPath("$.data[0].queueActive").value(false));
    }

    @Test
    @DisplayName("날짜 형식이 잘못된 달력 요청을 거부한다")
    void rejectInvalidDateFormat() throws Exception {
        mockMvc.perform(get("/api/v1/calendar")
                        .param("from", "2026/08/03")
                        .param("to", "2026-08-30"))
                .andExpect(status().isBadRequest());
    }
}
