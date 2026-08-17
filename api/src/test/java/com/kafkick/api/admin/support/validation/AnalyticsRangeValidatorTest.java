package com.kafkick.api.admin.support.validation;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.dashboard.dto.AnalyticsQuery;

/** 관리자 분석 조회 기간의 최대 1년 경계 조건을 단위 검증합니다. */
class AnalyticsRangeValidatorTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    /** 시작일과 종료일의 간격이 1년을 넘으면 Validation 위반이 발생하는지 확인합니다. */
    @Test
    void rejectsRangeLongerThanOneYear() {
        AnalyticsQuery query = new AnalyticsQuery(
                LocalDate.of(2025, 1, 1),
                LocalDate.of(2026, 1, 2),
                null,
                null
        );

        assertThat(validator.validate(query))
                .anyMatch(violation -> violation.getMessage().equals("조회 기간은 최대 1년입니다."));
    }

    /** 정확히 1년인 조회 범위는 허용해 경계값을 과도하게 차단하지 않는지 확인합니다. */
    @Test
    void acceptsRangeOfExactlyOneYear() {
        AnalyticsQuery query = new AnalyticsQuery(
                LocalDate.of(2025, 1, 1),
                LocalDate.of(2026, 1, 1),
                null,
                null
        );

        assertThat(validator.validate(query)).isEmpty();
    }
}
