package com.kafkick.api.admin.support.validation;

import java.time.LocalDate;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import com.kafkick.api.admin.dashboard.dto.AnalyticsQuery;
import com.kafkick.api.admin.support.config.AdminAnalyticsProperties;

/**
 * 관리자 분석 기간이 시간 순서대로 정렬되고 설정된 최대 연도 수를 넘지 않는지 검증합니다.
 * 필수값 누락은 record component의 {@code @NotNull}이 담당하므로 여기서는 중복 위반을 만들지 않습니다.
 */
public final class AnalyticsRangeValidator implements ConstraintValidator<ValidAnalyticsRange, AnalyticsQuery> {

    private final AdminAnalyticsProperties properties;

    public AnalyticsRangeValidator(AdminAnalyticsProperties properties) {
        this.properties = properties;
    }

    /**
     * 시작일·종료일이 모두 있을 때만 교차 필드 규칙을 적용합니다.
     * 역전과 설정 기간 초과를 서로 다른 메시지로 반환해 운영자가 잘못된 입력 원인을 구분할 수 있게 합니다.
     *
     * @param query 검증할 관리자 분석 Query
     * @param context Jakarta Validation constraint 실행 문맥
     * @return 날짜 순서와 설정된 최대 범위를 만족하면 true
     */
    @Override
    public boolean isValid(AnalyticsQuery query, ConstraintValidatorContext context) {
        if (query == null || query.from() == null || query.to() == null) {
            return true;
        }
        if (query.from().isAfter(query.to())) {
            return violation(context, "from은 to보다 늦을 수 없습니다.");
        }
        if (query.to().isAfter(maximumInclusiveEnd(query.from()))) {
            return violation(context,
                    "조회 기간은 최대 " + properties.maxRangeYears() + "년입니다.");
        }
        return true;
    }

    private LocalDate maximumInclusiveEnd(LocalDate from) {
        LocalDate anniversary = from.plusYears(properties.maxRangeYears());
        boolean leapDayWasClamped = from.getMonthValue() == 2
                && from.getDayOfMonth() == 29
                && anniversary.getMonthValue() == 2
                && anniversary.getDayOfMonth() == 28;
        return leapDayWasClamped ? anniversary : anniversary.minusDays(1);
    }

    private boolean violation(ConstraintValidatorContext context, String message) {
        // annotation의 포괄 메시지 대신 실제로 위반한 규칙 하나를 응답에 노출합니다.
        context.disableDefaultConstraintViolation();
        context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
        return false;
    }
}
