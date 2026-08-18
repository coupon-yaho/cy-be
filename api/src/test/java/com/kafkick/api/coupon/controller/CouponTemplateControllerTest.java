// 쿠폰 템플릿 생성·조회·수정 API의 응답 계약을 테스트합니다.
package com.kafkick.api.coupon.controller;

import com.kafkick.api.coupon.adapter.CouponTemplateUpdateTransactionalAdapter;
import com.kafkick.api.coupon.dto.CouponTemplateCreateRequest;
import com.kafkick.api.support.AdminRequestHeaders;
import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.core.coupon.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupon.port.CouponTemplatePage;
import com.kafkick.core.coupon.service.CouponTemplateCreateCommand;
import com.kafkick.core.coupon.service.CouponTemplateCreateService;
import com.kafkick.core.coupon.service.CouponTemplateQueryService;
import com.kafkick.core.coupon.service.CouponTemplateUpdateCommand;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalTime;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
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
    private CouponTemplateUpdateTransactionalAdapter
            couponTemplateUpdateTransactionalAdapter;

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
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        )
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
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        )
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
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        )
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
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        )
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

        mockMvc.perform(get("/api/v1/admin/coupon-templates/{id}", 100L)
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        ))
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

        mockMvc.perform(get("/api/v1/admin/coupon-templates/{id}", 999L)
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        ))
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
        mockMvc.perform(get("/api/v1/admin/coupon-templates/{id}", 0L)
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        ))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(400))
                .andExpect(jsonPath("$.error.code")
                        .value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("쿠폰 템플릿 ID는 0보다 커야 합니다."));

        verifyNoInteractions(couponTemplateQueryService);
    }

    @Test
    @DisplayName("쿠폰 템플릿 목록을 조회하면 200과 기본 페이지 결과를 반환한다")
    void findCouponTemplatePage() throws Exception {
        CouponTemplate firstCouponTemplate = CouponTemplate.restore(
                1L,
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
                Set.of(MembershipGrade.GOLD, MembershipGrade.VIP),
                true
        );
        CouponTemplate secondCouponTemplate = CouponTemplate.restore(
                2L,
                1L,
                "웰컴 실버 5천원 할인",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                5,
                3,
                CouponDayOfWeek.FRI,
                LocalTime.of(12, 0),
                3,
                50,
                Set.of(MembershipGrade.WELCOME, MembershipGrade.SILVER),
                true
        );

        when(couponTemplateQueryService.findPage(0, 20))
                .thenReturn(new CouponTemplatePage(
                        List.of(
                        firstCouponTemplate,
                        secondCouponTemplate
                        ),
                        0,
                        20,
                        2,
                        1
                ));

        mockMvc.perform(get("/api/v1/admin/coupon-templates")
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        ))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.content.length()").value(2))
                .andExpect(jsonPath("$.data.content[0].id").value(1))
                .andExpect(jsonPath("$.data.content[0].policyType")
                        .value("PERCENT_CAPPED"))
                .andExpect(jsonPath("$.data.content[0].eligibleGrades[0]")
                        .value("GOLD"))
                .andExpect(jsonPath("$.data.content[0].eligibleGrades[1]")
                        .value("VIP"))
                .andExpect(jsonPath("$.data.content[1].id").value(2))
                .andExpect(jsonPath("$.data.content[1].policyType")
                        .value("FIXED_AMOUNT"))
                .andExpect(jsonPath("$.data.content[1].eligibleGrades[0]")
                        .value("WELCOME"))
                .andExpect(jsonPath("$.data.content[1].eligibleGrades[1]")
                        .value("SILVER"))
                .andExpect(jsonPath("$.data.page").value(0))
                .andExpect(jsonPath("$.data.size").value(20))
                .andExpect(jsonPath("$.data.totalElements").value(2))
                .andExpect(jsonPath("$.data.totalPages").value(1));

        verify(couponTemplateQueryService).findPage(0, 20);
    }

    @Test
    @DisplayName("쿠폰 템플릿이 없으면 200과 빈 페이지를 반환한다")
    void findEmptyCouponTemplatePage() throws Exception {
        when(couponTemplateQueryService.findPage(0, 20))
                .thenReturn(new CouponTemplatePage(
                        List.of(),
                        0,
                        20,
                        0,
                        0
                ));

        mockMvc.perform(get("/api/v1/admin/coupon-templates")
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        ))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.content").isArray())
                .andExpect(jsonPath("$.data.content").isEmpty())
                .andExpect(jsonPath("$.data.totalElements").value(0))
                .andExpect(jsonPath("$.data.totalPages").value(0));

        verify(couponTemplateQueryService).findPage(0, 20);
    }

    @Test
    @DisplayName("요청한 페이지 번호와 크기로 쿠폰 템플릿을 조회한다")
    void findRequestedCouponTemplatePage() throws Exception {
        when(couponTemplateQueryService.findPage(1, 10))
                .thenReturn(new CouponTemplatePage(
                        List.of(),
                        1,
                        10,
                        12,
                        2
                ));

        mockMvc.perform(get("/api/v1/admin/coupon-templates")
                        .param("page", "1")
                        .param("size", "10")
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        ))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.page").value(1))
                .andExpect(jsonPath("$.data.size").value(10));

        verify(couponTemplateQueryService).findPage(1, 10);
    }

    @Test
    @DisplayName("페이지 번호가 음수이면 400을 반환한다")
    void rejectNegativePageNumber() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupon-templates")
                        .param("page", "-1")
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        ))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("페이지 번호는 0 이상이어야 합니다."));

        verifyNoInteractions(couponTemplateQueryService);
    }

    @Test
    @DisplayName("페이지 크기가 0이면 400을 반환한다")
    void rejectNonPositivePageSize() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupon-templates")
                        .param("size", "0")
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        ))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("페이지 크기는 1 이상이어야 합니다."));

        verifyNoInteractions(couponTemplateQueryService);
    }

    @Test
    @DisplayName("페이지 크기가 최대값을 초과하면 400을 반환한다")
    void rejectPageSizeOverMaximum() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupon-templates")
                        .param("size", "101")
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        ))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("페이지 크기는 100 이하여야 합니다."));

        verifyNoInteractions(couponTemplateQueryService);
    }

    @Test
    @DisplayName("쿠폰 템플릿을 전체 수정하면 200과 수정 결과를 반환한다")
    void updateCouponTemplate() throws Exception {
        CouponTemplate updatedCouponTemplate = CouponTemplate.restore(
                100L,
                2L,
                "수정된 정액 쿠폰",
                CouponPolicyType.FIXED_AMOUNT,
                null,
                null,
                5_000,
                14,
                3,
                CouponDayOfWeek.FRI,
                LocalTime.of(12, 0),
                3,
                50,
                Set.of(MembershipGrade.WELCOME, MembershipGrade.SILVER),
                true
        );

        when(couponTemplateUpdateTransactionalAdapter.update(
                eq(100L),
                any(CouponTemplateUpdateCommand.class)
        )).thenReturn(updatedCouponTemplate);

        String requestBody = """
                {
                  "brandId": 2,
                  "name": "수정된 정액 쿠폰",
                  "policyType": "FIXED_AMOUNT",
                  "discountRate": null,
                  "maxDiscountAmount": null,
                  "discountAmount": 5000,
                  "validDays": 14,
                  "nthWeek": 3,
                  "dayOfWeek": "FRI",
                  "startTime": "12:00:00",
                  "durationHours": 3,
                  "stockPerOccurrence": 50,
                  "eligibleGrades": ["WELCOME", "SILVER"]
                }
                """;

        mockMvc.perform(put(
                        "/api/v1/admin/coupon-templates/{id}",
                        100L
                )
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        )
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.id").value(100))
                .andExpect(jsonPath("$.data.brandId").value(2))
                .andExpect(jsonPath("$.data.name")
                        .value("수정된 정액 쿠폰"))
                .andExpect(jsonPath("$.data.policyType")
                        .value("FIXED_AMOUNT"))
                .andExpect(jsonPath("$.data.discountRate")
                        .doesNotExist())
                .andExpect(jsonPath("$.data.discountAmount")
                        .value(5_000))
                .andExpect(jsonPath("$.data.eligibleGrades[0]")
                        .value("WELCOME"))
                .andExpect(jsonPath("$.data.eligibleGrades[1]")
                        .value("SILVER"))
                .andExpect(jsonPath("$.data.active").value(true));

        ArgumentCaptor<CouponTemplateUpdateCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponTemplateUpdateCommand.class);
        verify(couponTemplateUpdateTransactionalAdapter).update(
                eq(100L),
                commandCaptor.capture()
        );

        CouponTemplateUpdateCommand command = commandCaptor.getValue();
        assertThat(command.brandId()).isEqualTo(2L);
        assertThat(command.name()).isEqualTo("수정된 정액 쿠폰");
        assertThat(command.policyType())
                .isEqualTo(CouponPolicyType.FIXED_AMOUNT);
        assertThat(command.discountRate()).isNull();
        assertThat(command.maxDiscountAmount()).isNull();
        assertThat(command.discountAmount()).isEqualTo(5_000);
        assertThat(command.validDays()).isEqualTo(14);
        assertThat(command.nthWeek()).isEqualTo(3);
        assertThat(command.dayOfWeek()).isEqualTo(CouponDayOfWeek.FRI);
        assertThat(command.startTime()).isEqualTo(LocalTime.of(12, 0));
        assertThat(command.durationHours()).isEqualTo(3);
        assertThat(command.stockPerOccurrence()).isEqualTo(50);
        assertThat(command.eligibleGrades()).containsExactlyInAnyOrder(
                MembershipGrade.WELCOME,
                MembershipGrade.SILVER
        );
    }

    @Test
    @DisplayName("존재하지 않는 쿠폰 템플릿 수정은 404를 반환한다")
    void rejectUpdatingMissingCouponTemplate() throws Exception {
        when(couponTemplateUpdateTransactionalAdapter.update(
                eq(999L),
                any(CouponTemplateUpdateCommand.class)
        )).thenThrow(new BusinessException(
                CouponTemplateErrorCode.COUPON_TEMPLATE_NOT_FOUND,
                "couponTemplateId=999"
        ));

        String requestBody = validUpdateRequestBody();

        mockMvc.perform(put(
                        "/api/v1/admin/coupon-templates/{id}",
                        999L
                )
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        )
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error.code").value("COUPON-102"));
    }

    @Test
    @DisplayName("쿠폰 템플릿 수정 ID가 0 이하면 400을 반환한다")
    void rejectNonPositiveCouponTemplateIdWhenUpdating() throws Exception {
        mockMvc.perform(put(
                        "/api/v1/admin/coupon-templates/{id}",
                        0L
                )
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        )
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validUpdateRequestBody()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"))
                .andExpect(jsonPath("$.error.message")
                        .value("쿠폰 템플릿 ID는 0보다 커야 합니다."));

        verifyNoInteractions(couponTemplateUpdateTransactionalAdapter);
    }

    @Test
    @DisplayName("쿠폰 템플릿 수정 필수값이 없으면 400을 반환한다")
    void rejectInvalidCouponTemplateUpdateRequest() throws Exception {
        String requestBody = """
                {
                  "brandId": 2,
                  "name": "",
                  "policyType": "FIXED_AMOUNT",
                  "discountAmount": 5000,
                  "validDays": 14,
                  "nthWeek": 3,
                  "dayOfWeek": "FRI",
                  "startTime": "12:00:00",
                  "durationHours": 3,
                  "stockPerOccurrence": 50,
                  "eligibleGrades": []
                }
                """;

        mockMvc.perform(put(
                        "/api/v1/admin/coupon-templates/{id}",
                        100L
                )
                        .header(
                                AdminRequestHeaders.USER_ROLE,
                                AdminRequestHeaders.ADMIN_ROLE
                        )
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));

        verifyNoInteractions(couponTemplateUpdateTransactionalAdapter);
    }

    @Test
    @DisplayName("관리자 역할 헤더가 없으면 403을 반환한다")
    void rejectRequestWithoutAdminRoleHeader() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupon-templates/{id}", 100L))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(403))
                .andExpect(jsonPath("$.error.code").value("COMMON-005"))
                .andExpect(jsonPath("$.error.message")
                        .value("접근 권한이 없습니다."));

        verifyNoInteractions(couponTemplateQueryService);
    }

    @Test
    @DisplayName("관리자가 아닌 역할 헤더이면 403을 반환한다")
    void rejectRequestWithNonAdminRoleHeader() throws Exception {
        mockMvc.perform(get("/api/v1/admin/coupon-templates/{id}", 100L)
                        .header(AdminRequestHeaders.USER_ROLE, "MEMBER"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.status").value(403))
                .andExpect(jsonPath("$.error.code").value("COMMON-005"))
                .andExpect(jsonPath("$.error.message")
                        .value("접근 권한이 없습니다."));

        verifyNoInteractions(couponTemplateQueryService);
    }

    private String validUpdateRequestBody() {
        return """
                {
                  "brandId": 2,
                  "name": "수정된 정액 쿠폰",
                  "policyType": "FIXED_AMOUNT",
                  "discountRate": null,
                  "maxDiscountAmount": null,
                  "discountAmount": 5000,
                  "validDays": 14,
                  "nthWeek": 3,
                  "dayOfWeek": "FRI",
                  "startTime": "12:00:00",
                  "durationHours": 3,
                  "stockPerOccurrence": 50,
                  "eligibleGrades": ["WELCOME", "SILVER"]
                }
                """;
    }
}
