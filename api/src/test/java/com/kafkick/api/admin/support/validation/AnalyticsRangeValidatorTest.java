package com.kafkick.api.admin.support.validation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorFactory;
import jakarta.validation.Validation;
import jakarta.validation.Validator;

import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.dashboard.dto.AnalyticsQuery;
import com.kafkick.api.admin.support.config.AdminAnalyticsProperties;

/** 관리자 분석 조회 기간의 최대 1년 경계 조건을 단위 검증합니다. */
class AnalyticsRangeValidatorTest {

    private final Validator validator = validator(1);

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

    /** 2월 29일 시작 범위는 다음 해 2월 28일까지 한 해로 인정합니다. */
    @Test
    void acceptsLeapDayThroughLastDayOfFollowingFebruary() {
        AnalyticsQuery query = new AnalyticsQuery(
                LocalDate.of(2024, 2, 29),
                LocalDate.of(2025, 2, 28),
                null,
                null);

        assertThat(validator.validate(query)).isEmpty();
    }

    /** 2월 29일 시작 범위도 다음 해 3월 1일부터는 한 해를 초과합니다. */
    @Test
    void rejectsDayAfterLeapDayAnniversaryFallback() {
        AnalyticsQuery query = new AnalyticsQuery(
                LocalDate.of(2024, 2, 29),
                LocalDate.of(2025, 3, 1),
                null,
                null);

        assertThat(validator.validate(query))
                .anyMatch(violation -> violation.getMessage().equals("조회 기간은 최대 1년입니다."));
    }

    /** 설정된 연도 수가 실제 최대 조회 범위와 오류 메시지에 함께 적용됩니다. */
    @Test
    void configuredMaximumYearsChangesValidationBoundary() {
        Validator twoYearValidator = validator(2);
        AnalyticsQuery accepted = new AnalyticsQuery(
                LocalDate.of(2024, 1, 1), LocalDate.of(2025, 12, 31), null, null);
        AnalyticsQuery rejected = new AnalyticsQuery(
                LocalDate.of(2024, 1, 1), LocalDate.of(2026, 1, 1), null, null);

        assertThat(twoYearValidator.validate(accepted)).isEmpty();
        assertThat(twoYearValidator.validate(rejected))
                .anyMatch(violation -> violation.getMessage().equals("조회 기간은 최대 2년입니다."));
    }

    @Test
    void rejectsNonPositiveConfiguredMaximumYears() {
        assertThatThrownBy(() -> new AdminAnalyticsProperties(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private Validator validator(int maxRangeYears) {
        AnalyticsRangeValidator configured = new AnalyticsRangeValidator(
                new AdminAnalyticsProperties(maxRangeYears));
        return Validation.byDefaultProvider()
                .configure()
                .constraintValidatorFactory(new ConfiguredConstraintValidatorFactory(configured))
                .buildValidatorFactory()
                .getValidator();
    }

    private record ConfiguredConstraintValidatorFactory(
            AnalyticsRangeValidator analyticsRangeValidator) implements ConstraintValidatorFactory {

        @Override
        public <T extends ConstraintValidator<?, ?>> T getInstance(Class<T> key) {
            if (key == AnalyticsRangeValidator.class) {
                return key.cast(analyticsRangeValidator);
            }
            try {
                return key.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException exception) {
                throw new IllegalStateException(exception);
            }
        }

        @Override
        public void releaseInstance(ConstraintValidator<?, ?> instance) {
            // 테스트가 소유한 무상태 Validator라 정리할 자원이 없습니다.
        }
    }
}
