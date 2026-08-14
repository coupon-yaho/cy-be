// 쿠폰 템플릿 생성 및 단건 조회 API의 응답 계약을 테스트합니다.
package com.kafkick.api.coupon.controller;

import com.kafkick.api.coupon.dto.CouponTemplateCreateRequest;
import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupon.service.CouponTemplateCreateCommand;
import com.kafkick.core.coupon.service.CouponTemplateCreateService;
import com.kafkick.core.coupon.service.CouponTemplateQueryService;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalTime;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(CouponTemplateController.class)
class CouponTemplateControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CouponTemplateCreateService couponTemplateCreateService;

    @MockitoBean
    private CouponTemplateQueryService couponTemplateQueryService;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    @DisplayName("쿠폰 템플릿을 생성하면 201과 생성 결과를 반환한다")
    void createCouponTemplate() throws Exception {
        CouponTemplate savedCouponTemplate = CouponTemplate.restore(
                100L,
                1L,
                "모카빈 20% 할인",
                CouponPolicyType.PERCENT_CAPPED,
                20,
                20_000,
                null,
                30,
                1,
                CouponDayOfWeek.TUE,
                LocalTime.of(14, 0),
                2,
                10_000,
                Set.of(
                        MembershipGrade.WELCOME,
                        MembershipGrade.SILVER,
                        MembershipGrade.GOLD,
                        MembershipGrade.VIP
                ),
                true
        );

        when(couponTemplateCreateService.create(
                any(CouponTemplateCreateCommand.class)
        )).thenReturn(savedCouponTemplate);

        String requestBody = """
                {
                  "brandId": 1,
                  "name": "모카빈 20% 할인",
                  "policyType": "PERCENT_CAPPED",
                  "discountRate": 20,
                  "maxDiscountAmount": 20000,
                  "discountAmount": null,
                  "validDays": 30,
                  "nthWeek": 1,
                  "dayOfWeek": "TUE",
                  "startTime": "14:00:00",
                  "durationHours": 2,
                  "stockPerOccurrence": 10000,
                  "eligibleGrades": [
                    "WELCOME",
                    "SILVER",
                    "GOLD",
                    "VIP"
                  ]
                }
                """;

        mockMvc.perform(post("/api/v1/admin/coupon-templates")
                .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isCreated())
                .andExpect(header().string(
                        "Location",
                        "/api/v1/admin/coupon-templates/100"
                ))
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.id").value(100))
                .andExpect(jsonPath("$.data.brandId").value(1))
                .andExpect(jsonPath("$.data.name")
                        .value("모카빈 20% 할인"))
                .andExpect(jsonPath("$.data.policyType")
                        .value("PERCENT_CAPPED"))
                .andExpect(jsonPath("$.data.discountRate").value(20))
                .andExpect(jsonPath("$.data.maxDiscountAmount")
                        .value(20_000))
                .andExpect(jsonPath("$.data.discountAmount")
                        .doesNotExist())
                .andExpect(jsonPath("$.data.minOrderAmount")
                        .doesNotExist())
                .andExpect(jsonPath("$.data.stockPerOccurrence")
                        .value(10_000))
                .andExpect(jsonPath("$.data.eligibleGrades[0]")
                        .value("WELCOME"))
                .andExpect(jsonPath("$.data.eligibleGrades[1]")
                        .value("SILVER"))
                .andExpect(jsonPath("$.data.eligibleGrades[2]")
                        .value("GOLD"))
                .andExpect(jsonPath("$.data.eligibleGrades[3]")
                        .value("VIP"))
                .andExpect(jsonPath("$.data.active").value(true));

        verify(couponTemplateCreateService)
                .create(any(CouponTemplateCreateCommand.class));
    }

    @Test
    @DisplayName("필수 요청값이 잘못되면 400을 반환한다")
    void rejectInvalidRequest() throws Exception {
        String requestBody = """
                {
                  "brandId": 0,
                  "name": "",
                  "policyType": "FIXED_AMOUNT",
                  "discountAmount": 5000,
                  "validDays": 30,
                  "nthWeek": 1,
                  "dayOfWeek": "TUE",
                  "startTime": "14:00:00",
                  "durationHours": 2,
                  "stockPerOccurrence": 10000,
                  "eligibleGrades": ["VIP"]
                }
                """;

        mockMvc.perform(post("/api/v1/admin/coupon-templates")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(400))
                .andExpect(jsonPath("$.error.code")
                        .value("COMMON-001"))
                .andExpect(jsonPath("$.error.message").exists());

        verifyNoInteractions(couponTemplateCreateService);
    }

    @Test
    @DisplayName("도메인 할인 정책 검증에 실패하면 400을 반환한다")
    void rejectInvalidDiscountPolicy() throws Exception {
        when(couponTemplateCreateService.create(
                any(CouponTemplateCreateCommand.class)
        )).thenThrow(new BusinessException(
                CouponTemplateErrorCode.INVALID_COUPON_TEMPLATE,
                "퍼센트 할인에는 정액 할인 금액을 입력할 수 없습니다."
        ));

        String requestBody = """
                {
                  "brandId": 1,
                  "name": "잘못된 쿠폰",
                  "policyType": "PERCENT_CAPPED",
                  "discountRate": 20,
                  "maxDiscountAmount": 20000,
                  "discountAmount": 5000,
                  "validDays": 30,
                  "nthWeek": 1,
                  "dayOfWeek": "TUE",
                  "startTime": "14:00:00",
                  "durationHours": 2,
                  "stockPerOccurrence": 10000,
                  "eligibleGrades": ["VIP"]
                }
                """;

        mockMvc.perform(post("/api/v1/admin/coupon-templates")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(400))
                .andExpect(jsonPath("$.error.code")
                        .value("COUPON-101"))
                .andExpect(jsonPath("$.error.message")
                        .value("쿠폰 템플릿 값이 올바르지 않습니다."));

    }

    @Test
    @DisplayName("알 수 없는 Enum 값이면 400을 반환한다")
    void rejectUnknownEnumValue() throws Exception {
        String requestBody = """
                {
                  "brandId": 1,
                  "name": "잘못된 정책",
                  "policyType": "PERCENT",
                  "discountRate": 20,
                  "maxDiscountAmount": 20000,
                  "validDays": 30,
                  "nthWeek": 1,
                  "dayOfWeek": "TUE",
                  "startTime": "14:00:00",
                  "durationHours": 2,
                  "stockPerOccurrence": 10000,
                  "eligibleGrades": ["VIP"]
                }
                """;

        mockMvc.perform(post("/api/v1/admin/coupon-templates")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(400))
                .andExpect(jsonPath("$.error.code")
                        .value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("잘못된 요청입니다."));

        verifyNoInteractions(couponTemplateCreateService);
    }

    @Test
    @DisplayName("쿠폰 템플릿 ID로 단건 조회하면 200과 조회 결과를 반환한다")
    void findCouponTemplateById() throws Exception {
        CouponTemplate couponTemplate = CouponTemplate.restore(
                        100L,
                        1L,
                        "골드 VIP 20% 할인",
                        CouponPolicyType.PERCENT_CAPPED,
                        20,
                        10_000,
                        null,
                        7,
                        2,
                        CouponDayOfWeek.WED,
                        LocalTime.of(10, 0),
                        2,
                        100,
                        Set.of(
                                MembershipGrade.GOLD,
                                MembershipGrade.VIP
                        ),
                        true
                );

        when(couponTemplateQueryService.findById(100L))
                .thenReturn(couponTemplate);

        mockMvc.perform(get("/api/v1/admin/coupon-templates/{id}", 100L))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.id").value(100))
                .andExpect(jsonPath("$.data.brandId").value(1))
                .andExpect(jsonPath("$.data.name")
                        .value("골드 VIP 20% 할인"))
                .andExpect(jsonPath("$.data.policyType")
                        .value("PERCENT_CAPPED"))
                .andExpect(jsonPath("$.data.minOrderAmount")
                        .doesNotExist())
                .andExpect(jsonPath("$.data.eligibleGrades[0]")
                        .value("GOLD"))
                .andExpect(jsonPath("$.data.eligibleGrades[1]")
                        .value("VIP"))
                .andExpect(jsonPath("$.data.active").value(true));

        verify(couponTemplateQueryService).findById(100L);
    }

    @Test
    @DisplayName("존재하지 않는 쿠폰 템플릿을 조회하면 404를 반환한다")
    void rejectMissingCouponTemplate() throws Exception {
        when(couponTemplateQueryService.findById(999L))
                .thenThrow(new BusinessException(
                        CouponTemplateErrorCode.COUPON_TEMPLATE_NOT_FOUND,
                        "couponTemplateId=999"
                ));

        mockMvc.perform(get("/api/v1/admin/coupon-templates/{id}", 999L))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(404))
                .andExpect(jsonPath("$.error.code")
                        .value("COUPON-102"))
                .andExpect(jsonPath("$.error.message")
                        .value("쿠폰 템플릿을 찾을 수 없습니다."));

        verify(couponTemplateQueryService).findById(999L);
    }

    @Test
    @DisplayName("쿠폰 템플릿 ID가 0 이하면 400을 반환한다")
    void rejectNonPositiveCouponTemplateId() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupon-templates/{id}", 0L))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(400))
                .andExpect(jsonPath("$.error.code")
                        .value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("쿠폰 템플릿 ID는 0보다 커야 합니다."));

        verifyNoInteractions(couponTemplateQueryService);
    }
}
