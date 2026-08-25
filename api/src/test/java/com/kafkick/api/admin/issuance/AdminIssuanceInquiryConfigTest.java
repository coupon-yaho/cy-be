package com.kafkick.api.admin.issuance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryReadResult;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryService;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySourceReader;
import com.kafkick.core.admin.inquiry.IssuanceInquiryCalculator;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

/** 관측 스위치에 따라 실제 Reader 또는 명시적인 503 대체 Reader만 남는지 검증합니다. */
class AdminIssuanceInquiryConfigTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(
                    AdminIssuanceInquiryConfig.class,
                    AdminIssuanceInquiryService.class,
                    IssuanceInquiryCalculator.class)
            .withBean(TimeProvider.class, () -> new TimeProvider(
                    Clock.fixed(Instant.parse("2026-08-25T00:00:00Z"), ZoneOffset.UTC)));

    /** 관측이 꺼졌거나 미설정이면 Mock 데이터가 아니라 ADMIN-003을 반환합니다. */
    @Test
    void usesUnavailableReaderWhenObservationIsDisabled() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(AdminIssuanceInquirySourceReader.class);
            assertThat(context).hasSingleBean(AdminIssuanceInquiryService.class);
            assertThatThrownBy(() -> context.getBean(AdminIssuanceInquiryService.class)
                    .getInquiries(query()))
                    .isInstanceOfSatisfying(BusinessException.class, exception ->
                            assertThat(exception.getErrorCode())
                                    .isEqualTo(AdminApiErrorCode.OBSERVATION_DISABLED));
        });
    }

    /** 관측이 켜졌을 때는 Storage가 제공한 실제 Reader를 fallback이 가리지 않습니다. */
    @Test
    void keepsJdbcReaderWhenObservationIsEnabled() {
        AdminIssuanceInquirySourceReader jdbcReader = (query, snapshotAt) ->
                AdminIssuanceInquiryReadResult.available(
                        new AdminIssuanceInquirySource(List.of(), List.of(), List.of()));

        runner.withPropertyValues("observation.datasource.enabled=true")
                .withBean("jdbcReader", AdminIssuanceInquirySourceReader.class, () -> jdbcReader)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(AdminIssuanceInquirySourceReader.class);
                    assertThat(context.getBean(AdminIssuanceInquirySourceReader.class))
                            .isSameAs(jdbcReader);
                });
    }

    private static AdminIssuanceInquiryQuery query() {
        return new AdminIssuanceInquiryQuery(1L, null, null, null, null, 50);
    }
}
