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

    /** 평년 시작일 기준 포함 1년의 다음 날짜를 요청하면 Validation 위반이 발생하는지 확인합니다. */
    @Test
    void rejectsRangeLongerThanOneYear() {
        AnalyticsQuery query = new AnalyticsQuery(
                LocalDate.of(2025, 1, 1),
                LocalDate.of(2026, 1, 1),
                null,
                null
        );

        assertThat(validator.validate(query))
                .anyMatch(violation -> violation.getMessage().equals("조회 기간은 최대 1년입니다."));
    }

    /** 평년 시작일과 종료일을 모두 포함한 마지막 날짜까지 허용하는지 확인합니다. */
    @Test
    void acceptsRangeOfExactlyOneYear() {
        AnalyticsQuery query = new AnalyticsQuery(
                LocalDate.of(2025, 1, 1),
                LocalDate.of(2025, 12, 31),
                null,
                null
        );

        assertThat(validator.validate(query)).isEmpty();
    }

    /** 윤년 전체를 포함하는 366일 범위의 마지막 날짜까지 허용하는지 확인합니다. */
    @Test
    void acceptsInclusiveLeapYearRange() {
        AnalyticsQuery query = new AnalyticsQuery(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 12, 31),
                null,
                null
        );

        assertThat(validator.validate(query)).isEmpty();
    }

    /** 윤년 전체 범위 다음 날짜부터 최대 1년 위반으로 거부하는지 확인합니다. */
    @Test
    void rejectsDayAfterInclusiveLeapYearRange() {
        AnalyticsQuery query = new AnalyticsQuery(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2025, 1, 1),
                null,
                null
        );

        assertThat(validator.validate(query))
                .anyMatch(violation -> violation.getMessage().equals("조회 기간은 최대 1년입니다."));
    }
}
