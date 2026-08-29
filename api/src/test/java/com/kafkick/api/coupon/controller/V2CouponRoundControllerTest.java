package com.kafkick.api.coupon.controller;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.coupon.query.V2IssuableCouponRoundQuery;
import com.kafkick.core.coupon.v2.query.CouponDefinition;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.support.TimeProvider;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(V2CouponRoundController.class)
class V2CouponRoundControllerTest {

    private static final Instant AS_OF = Instant.parse("2026-08-28T00:00:00Z");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private V2IssuableCouponRoundQuery query;

    @MockitoBean
    private TimeProvider timeProvider;

    @Test
    void returnsOpenDefinitionsWithoutMemberSpecificHeaders() throws Exception {
        when(timeProvider.instant()).thenReturn(AS_OF);
        when(query.findOpenDefinitions(AS_OF)).thenReturn(List.of(new CouponDefinition(
                7L, 2L, "영화 할인", CouponPolicyType.FIXED_AMOUNT,
                null, null, 3_000, 30, AS_OF.minusSeconds(1), AS_OF.plusSeconds(60),
                CouponRoundStatus.OPEN)));

        mockMvc.perform(get("/api/v2/coupon-rounds"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.length()").value(1))
                .andExpect(jsonPath("$.data[0].couponRoundId").value(7))
                .andExpect(jsonPath("$.data[0].remainingQuantity").doesNotExist());

        verify(query).findOpenDefinitions(AS_OF);
    }
}
