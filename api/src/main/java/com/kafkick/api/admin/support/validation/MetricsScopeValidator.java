package com.kafkick.api.admin.support.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import com.kafkick.api.admin.observability.dto.MetricsQuery;

/**
 * 관측 지표 요청이 쿠폰 범위와 Benchmark 실행 범위를 동시에 지정하지 않았는지 검증합니다.
 * 두 식별자가 모두 null인 GLOBAL 범위와 둘 중 하나만 지정한 범위는 유효합니다.
 */
public final class MetricsScopeValidator
        implements ConstraintValidator<MutuallyExclusiveMetricsScope, MetricsQuery> {

    /**
     * Query가 null이거나 두 범위 식별자 중 최대 하나만 존재하면 유효합니다.
     *
     * @param query 검증할 관리자 관측 지표 Query
     * @param context Jakarta Validation constraint 실행 문맥
     * @return 두 범위를 동시에 지정하지 않았으면 true
     */
    @Override
    public boolean isValid(MetricsQuery query, ConstraintValidatorContext context) {
        // null Query는 다른 binding/필수값 검증이 담당하므로 이 constraint에서는 유효로 취급합니다.
        return query == null || query.couponId() == null || query.benchmarkRunId() == null;
    }
}
