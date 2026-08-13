// 쿠폰 템플릿 생성 API의 성공 응답과 요청 검증 오류를 테스트합니다.
package com.kafkick.api.coupon.controller;

import com.kafkick.api.coupon.dto.CouponTemplateCreateRequest;
import com.kafkick.api.coupon.dto.CouponTemplateCreateResponse;
import com.kafkick.api.coupon.service.CouponTemplateCreateService;
import com.kafkick.core.coupon.CouponDayOfWeek;
import com.kafkick.core.coupon.CouponPolicyType;
import com.kafkick.core.coupon.MembershipGrade;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.coupon.exception.CouponErrorCode;
import com.kafkick.core.support.exception.BusinessException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(CouponTemplateController.class)
class CouponTemplateControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CouponTemplateCreateService couponTemplateCreateService;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    @DisplayName("쿠폰 템플릿을 생성하면 201과 생성 결과를 반환한다")
    void createCouponTemplate() throws Exception {
        CouponTemplateCreateResponse response = new CouponTemplateCreateResponse(
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
                List.of(
                        MembershipGrade.WELCOME,
                        MembershipGrade.SILVER,
                        MembershipGrade.GOLD,
                        MembershipGrade.VIP
                ),
                true
        );

        when(couponTemplateCreateService.create(
                any(CouponTemplateCreateRequest.class)
        )).thenReturn(response);

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
                .andExpect(header().doesNotExist("Location"))
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
                .create(any(CouponTemplateCreateRequest.class));
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
                any(CouponTemplateCreateRequest.class)
        )).thenThrow(new BusinessException(
                CouponErrorCode.INVALID_COUPON_TEMPLATE,
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
                        .value("COUPON-001"))
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
}
