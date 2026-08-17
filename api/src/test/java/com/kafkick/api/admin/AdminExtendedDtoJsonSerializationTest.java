package com.kafkick.api.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.DayOfWeek;
import java.time.LocalTime;
import java.util.List;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.admin.benchmark.dto.BenchmarkCommandAcceptedResponse;
import com.kafkick.api.admin.benchmark.dto.BenchmarkDetailResponse;
import com.kafkick.api.admin.campaign.dto.BrandListResponse;
import com.kafkick.api.admin.campaign.dto.CampaignListResponse;
import com.kafkick.api.admin.campaign.dto.TemplateListResponse;
import com.kafkick.api.admin.measurement.dto.MeasurementSessionResponse;
import com.kafkick.api.admin.notification.dto.NotificationFailurePageResponse;
import com.kafkick.api.admin.runtimeconfig.dto.RuntimeConfigResponse;
import com.kafkick.api.admin.verification.dto.VerificationRunAcceptedResponse;
import com.kafkick.api.admin.verification.dto.VerificationRunDetailResponse;
import com.kafkick.api.admin.verification.dto.VerificationRunPageResponse;
import com.kafkick.core.admin.BenchmarkRunState;
import com.kafkick.core.admin.BrandCategory;
import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.MeasurementState;
import com.kafkick.core.admin.VerificationRunState;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;

/** 목록·운영 명령·상세 응답 DTO의 확정 enum과 nullable JSON 계약을 고정합니다. */
class AdminExtendedDtoJsonSerializationTest {

    private static final Instant AT = Instant.parse("2026-08-16T00:00:00Z");
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 과거 방향 목록이 빈 배열, nullable cursor, 이전 데이터 존재 여부를 일관되게 직렬화하는지 검증합니다. */
    @Test
    void historicalPagesKeepEmptyArraysNullableCursorsAndDirectionFlag() throws Exception {
        assertThat(objectMapper.writeValueAsString(new CampaignListResponse(List.of(), null, false)))
                .isEqualTo("{\"items\":[],\"nextBeforeCursor\":null,\"hasOlder\":false}");
        assertThat(objectMapper.writeValueAsString(new BrandListResponse(List.of(), null, false)))
                .isEqualTo("{\"items\":[],\"nextBeforeCursor\":null,\"hasOlder\":false}");
        assertThat(objectMapper.writeValueAsString(new TemplateListResponse(List.of(), null, false)))
                .isEqualTo("{\"items\":[],\"nextBeforeCursor\":null,\"hasOlder\":false}");
        assertThat(objectMapper.writeValueAsString(new NotificationFailurePageResponse(List.of(), null, false)))
                .isEqualTo("{\"items\":[],\"nextBeforeCursor\":null,\"hasOlder\":false}");
        assertThat(objectMapper.writeValueAsString(new VerificationRunPageResponse(List.of(), null, false)))
                .isEqualTo("{\"items\":[],\"nextBeforeCursor\":null,\"hasOlder\":false}");
    }

    /** 브랜드 카테고리와 쿠폰 정책이 임의 문자열이 아닌 제한된 enum 이름으로 노출되는지 검증합니다. */
    @Test
    void campaignCatalogUsesBoundedCategoryAndPolicyEnumNames() throws Exception {
        BrandListResponse brands = new BrandListResponse(
                List.of(new BrandListResponse.BrandSummary(1L, "브랜드", BrandCategory.CAFE, true)), null, false);
        TemplateListResponse templates = new TemplateListResponse(
                List.of(new TemplateListResponse.TemplateSummary(
                        2L, 1L, "정률 할인", CouponPolicyType.PERCENT_CAPPED, 1,
                        DayOfWeek.MONDAY, LocalTime.of(10, 0), 2, 100, 15, true)), null, false);

        assertThat(objectMapper.writeValueAsString(brands)).contains("\"category\":\"CAFE\"");
        assertThat(objectMapper.writeValueAsString(templates)).contains("\"policyType\":\"PERCENT_CAPPED\"");
    }

    /** 런타임 설정 enum 이름과 문자열 변경 주체를 JSON 계약으로 고정합니다. */
    @Test
    void runtimeConfigSerializesCanonicalEnumsAndStringUpdater() throws Exception {
        RuntimeConfigResponse response = new RuntimeConfigResponse(
                3, EngineVersion.V3, ReleaseStage.V2_2, QueueMode.ADAPTIVE,
                AT, "admin:17", SourceStatus.VALID);

        assertThat(objectMapper.writeValueAsString(response))
                .isEqualTo("{\"revision\":3,\"engineVersion\":\"V3\",\"releaseStage\":\"V2_2\","
                        + "\"queueMode\":\"ADAPTIVE\",\"updatedAt\":\"2026-08-16T00:00:00Z\","
                        + "\"updatedBy\":\"admin:17\",\"sourceStatus\":\"VALID\"}");
    }

    /** Benchmark·계측·검증 명령 응답이 각 수명주기 enum을 사용하는지 검증합니다. */
    @Test
    void operationResponsesUseExplicitLifecycleEnums() throws Exception {
        assertThat(objectMapper.writeValueAsString(
                new BenchmarkCommandAcceptedResponse(1L, BenchmarkRunState.RUNNING, AT)))
                .contains("\"state\":\"RUNNING\"");
        assertThat(objectMapper.writeValueAsString(
                new MeasurementSessionResponse(1L, MeasurementState.STOPPED, AT)))
                .contains("\"state\":\"STOPPED\"");
        assertThat(objectMapper.writeValueAsString(
                new VerificationRunAcceptedResponse(1L, VerificationRunState.REQUESTED, AT)))
                .contains("\"status\":\"REQUESTED\"");
    }

    /** 실행 중 상세 응답의 미확정 판정·종료 시각과 빈 결과 목록이 그대로 보존되는지 검증합니다. */
    @Test
    void detailedResponsesPreserveNullableVerdictsAndEmptyCollections() throws Exception {
        BenchmarkDetailResponse benchmark = new BenchmarkDetailResponse(
                1L, EngineVersion.V1, ReleaseStage.V1, QueueMode.OFF, "BASELINE",
                BenchmarkRunState.RUNNING, null, AT, null, null, List.of());
        VerificationRunDetailResponse verification = new VerificationRunDetailResponse(
                2L, null, 0, 0, List.of());

        assertThat(objectMapper.writeValueAsString(benchmark))
                .contains("\"verdict\":null", "\"finishedAt\":null", "\"serverSamples\":[]");
        assertThat(objectMapper.writeValueAsString(verification))
                .contains("\"verdict\":null", "\"findings\":[]");
    }
}
