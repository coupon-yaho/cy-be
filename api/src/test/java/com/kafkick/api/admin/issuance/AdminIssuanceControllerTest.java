package com.kafkick.api.admin.issuance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import com.jayway.jsonpath.JsonPath;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery.HistoryPosition;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryService;
import com.kafkick.core.admin.issuancehistory.IssuanceCodeMasker;
import com.kafkick.core.admin.issuancehistory.IssuanceHistoryCalculator;
import com.kafkick.core.admin.issuancehistory.mock.AdminIssuanceHistoryMockDataFactory;
import com.kafkick.core.support.TimeProvider;

/** 회원 발급 문의와 발급 이력 조회의 필터·기간·cursor Validation을 검증합니다. */
class AdminIssuanceControllerTest {

    private static final Clock CLOCK = Clock.fixed(
            Instant.parse("2026-08-23T00:00:00Z"), ZoneOffset.UTC);
    private final IssuanceHistoryCursorCodec cursorCodec = new IssuanceHistoryCursorCodec();
    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(
            new AdminIssuanceController(historyService(), cursorCodec));

    /** 발급 문의의 필수 회원 식별자를 생략하면 400으로 거부되는지 검증합니다. */
    @Test
    @DisplayName("발급 문의 조회는 memberId가 없으면 400 실패 봉투를 반환한다")
    void issuanceInquiriesRejectMissingMemberId() throws Exception {
        mockMvc.perform(get("/api/v1/admin/members/issuance-inquiries"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** 유효한 문의 필터가 바인딩된 뒤 미연결 상태를 ADMIN-001로 반환하는지 검증합니다. */
    @Test
    @DisplayName("발급 문의 조회는 유효 요청에 ADMIN-001 선구축 오류를 반환한다")
    void issuanceInquiriesReturnNotImplementedEnvelope() throws Exception {
        mockMvc.perform(get("/api/v1/admin/members/issuance-inquiries")
                        .param("memberId", "1")
                        .param("limit", "50"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }

    /** 유효한 발급 이력 조회가 실제 Core 결과와 성공 봉투를 반환하는지 검증합니다. */
    @Test
    @DisplayName("발급 이력 조회는 실제 Core 이력과 요약을 200 성공 봉투로 반환한다")
    void issuanceHistoriesReturnSuccessfulCoreResult() throws Exception {
        mockMvc.perform(get("/api/v1/admin/issuance-histories"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.items.length()").value(8))
                .andExpect(jsonPath("$.data.hasOlder").value(false))
                .andExpect(jsonPath("$.data.summary.totalCount").value(8));
    }

    /** 쿠폰·이벤트·KST 날짜 필터가 함께 적용된 모집단과 요약을 검증합니다. */
    @Test
    @DisplayName("발급 이력 조회는 couponId·eventType·from/to 필터를 함께 적용한다")
    void issuanceHistoriesApplyCouponEventAndKstDateFilters() throws Exception {
        mockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("couponId", "102")
                        .param("eventType", "ISSUE")
                        .param("from", "2026-08-23")
                        .param("to", "2026-08-23"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.items.length()").value(2))
                .andExpect(jsonPath("$.data.items[0].couponId").value(102))
                .andExpect(jsonPath("$.data.items[0].eventType").value("ISSUE"))
                .andExpect(jsonPath("$.data.items[1].couponId").value(102))
                .andExpect(jsonPath("$.data.items[1].eventType").value("ISSUE"))
                .andExpect(jsonPath("$.data.summary.totalCount").value(2))
                .andExpect(jsonPath("$.data.summary.issueCount").value(2));
    }

    /** 첫 페이지 cursor로 조회한 다음 페이지에 앞 페이지 항목이 반복되지 않는지 검증합니다. */
    @Test
    @DisplayName("첫 페이지의 nextBeforeCursor는 중복 없는 다음 과거 페이지를 반환한다")
    void issuanceHistoriesPageBackwardWithoutDuplicates() throws Exception {
        MvcResult firstResult = mockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("limit", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.hasOlder").value(true))
                .andExpect(jsonPath("$.data.nextBeforeCursor").isNotEmpty())
                .andReturn();
        String firstJson = firstResult.getResponse().getContentAsString();
        String nextBeforeCursor = JsonPath.read(firstJson, "$.data.nextBeforeCursor");
        List<Integer> firstIds = JsonPath.read(firstJson, "$.data.items[*].issuanceId");

        MvcResult secondResult = mockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("limit", "2")
                        .param("beforeCursor", nextBeforeCursor))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andReturn();
        List<Integer> secondIds = JsonPath.read(
                secondResult.getResponse().getContentAsString(), "$.data.items[*].issuanceId");

        assertThat(secondIds).doesNotContainAnyElementsOf(firstIds);
    }

    /** 서로 다른 Factory와 진행하는 요청 시각에서도 동률 이력이 한 번씩 반환되는지 검증합니다. */
    @Test
    @DisplayName("다른 Service와 later Clock에서도 limit 1 cursor가 1008·1007·1006을 빠짐없이 반환한다")
    void issuanceHistoriesKeepRowsStableAcrossFactoriesServicesAndLaterClock() throws Exception {
        TimeProvider firstTimeProvider = new TimeProvider(CLOCK);
        MockMvc firstInstanceMockMvc = AdminControllerContractTestSupport.mockMvc(
                new AdminIssuanceController(
                        new AdminIssuanceHistoryService(
                                firstTimeProvider,
                                new AdminIssuanceHistoryMockDataFactory(),
                                new IssuanceHistoryCalculator(new IssuanceCodeMasker())),
                        cursorCodec));
        AdvancingClock laterClock = new AdvancingClock(
                CLOCK.instant().plus(Duration.ofMinutes(1)), ZoneOffset.UTC);
        TimeProvider laterTimeProvider = new TimeProvider(laterClock);
        MockMvc laterInstanceMockMvc = AdminControllerContractTestSupport.mockMvc(
                new AdminIssuanceController(
                        new AdminIssuanceHistoryService(
                                laterTimeProvider,
                                new AdminIssuanceHistoryMockDataFactory(),
                                new IssuanceHistoryCalculator(new IssuanceCodeMasker())),
                        cursorCodec));

        MvcResult firstResult = firstInstanceMockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("limit", "1"))
                .andExpect(status().isOk())
                .andReturn();
        String firstJson = firstResult.getResponse().getContentAsString();
        String firstCursor = JsonPath.read(firstJson, "$.data.nextBeforeCursor");

        MvcResult secondResult = laterInstanceMockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("limit", "1")
                        .param("beforeCursor", firstCursor))
                .andExpect(status().isOk())
                .andReturn();
        String secondJson = secondResult.getResponse().getContentAsString();
        String secondCursor = JsonPath.read(secondJson, "$.data.nextBeforeCursor");
        laterClock.advance(Duration.ofMinutes(1));

        MvcResult thirdResult = laterInstanceMockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("limit", "1")
                        .param("beforeCursor", secondCursor))
                .andExpect(status().isOk())
                .andReturn();

        List<Integer> issuanceIds = List.of(
                JsonPath.read(firstJson, "$.data.items[0].issuanceId"),
                JsonPath.read(secondJson, "$.data.items[0].issuanceId"),
                JsonPath.read(thirdResult.getResponse().getContentAsString(),
                        "$.data.items[0].issuanceId"));
        HistoryPosition firstPosition = cursorCodec.decode(firstCursor);
        HistoryPosition secondPosition = cursorCodec.decode(secondCursor);

        assertThat(firstPosition.historyId()).isEqualTo(1_008L);
        assertThat(secondPosition.historyId()).isEqualTo(1_007L);
        assertThat(secondPosition.occurredAt()).isEqualTo(firstPosition.occurredAt());
        assertThat(issuanceIds).containsExactly(5_004, 6_003, 6_002).doesNotHaveDuplicates();
    }

    /** LocalDate의 다음 날 계산 범위를 넘는 to 입력을 400으로 통일하는지 검증합니다. */
    @Test
    @DisplayName("발급 이력 조회는 LocalDate.MAX to를 400 COMMON-001로 거부한다")
    void issuanceHistoriesRejectMaximumToDateAsInvalidInput() throws Exception {
        mockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("to", "+999999999-12-31"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));
    }

    /** decode할 수 없는 cursor를 HTTP 400 공통 입력 오류로 변환하는지 검증합니다. */
    @Test
    @DisplayName("발급 이력 조회는 잘못된 cursor를 400 COMMON-001로 거부한다")
    void issuanceHistoriesRejectInvalidCursor() throws Exception {
        mockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("beforeCursor", "invalid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("COMMON-001"));
    }

    /** HTTP 상태 필터가 표준 범위 100~599 밖이면 400으로 거부되는지 검증합니다. */
    @Test
    @DisplayName("발급 문의 조회는 HTTP 상태 코드 범위 밖 값을 400 실패 봉투로 거부한다")
    void issuanceInquiriesRejectInvalidHttpStatus() throws Exception {
        mockMvc.perform(get("/api/v1/admin/members/issuance-inquiries")
                        .param("memberId", "1")
                        .param("httpStatus", "99"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** 발급 이력의 시작일이 종료일보다 늦은 기간 조건을 400으로 거부하는지 검증합니다. */
    @Test
    @DisplayName("발급 이력 조회는 역전된 기간을 400 실패 봉투로 거부한다")
    void issuanceHistoriesRejectReversedRange() throws Exception {
        mockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("from", "2026-08-16")
                        .param("to", "2026-01-01"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** 페이지 크기의 최솟값과 최댓값 바깥을 모두 400으로 거부하는지 검증합니다. */
    @ParameterizedTest
    @ValueSource(strings = {"0", "201"})
    @DisplayName("발급 이력 조회는 limit 0과 201을 400으로 거부한다")
    void issuanceHistoriesRejectOutOfRangeLimit(String limit) throws Exception {
        mockMvc.perform(get("/api/v1/admin/issuance-histories")
                        .param("limit", limit))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** 고정 시각의 Mock 원천을 사용하는 실제 Core 발급 이력 Service를 구성합니다. */
    private static AdminIssuanceHistoryService historyService() {
        TimeProvider timeProvider = new TimeProvider(CLOCK);
        return new AdminIssuanceHistoryService(
                timeProvider,
                new AdminIssuanceHistoryMockDataFactory(),
                new IssuanceHistoryCalculator(new IssuanceCodeMasker()));
    }

    /** 테스트 요청 사이에서만 명시적으로 진행시킬 수 있는 Clock입니다. */
    private static final class AdvancingClock extends Clock {

        private Instant current;
        private final ZoneId zone;

        /** 지정한 시작 Instant와 Zone으로 진행 가능한 Clock을 만듭니다. */
        private AdvancingClock(Instant current, ZoneId zone) {
            this.current = current;
            this.zone = zone;
        }

        /** 현재 Clock의 Zone을 반환합니다. */
        @Override
        public ZoneId getZone() {
            return zone;
        }

        /** 같은 현재 Instant를 유지하면서 새 Zone을 사용하는 Clock을 반환합니다. */
        @Override
        public Clock withZone(ZoneId requestedZone) {
            return new AdvancingClock(current, requestedZone);
        }

        /** 현재 테스트 Instant를 반환합니다. */
        @Override
        public Instant instant() {
            return current;
        }

        /** 다음 HTTP 요청 전에 테스트 시각을 지정한 기간만큼 진행합니다. */
        private void advance(Duration duration) {
            current = current.plus(duration);
        }
    }
}
