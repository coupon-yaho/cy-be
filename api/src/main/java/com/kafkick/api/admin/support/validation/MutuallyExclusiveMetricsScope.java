package com.kafkick.api.admin.support.validation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

/**
 * 관리자 지표 조회에서 {@code couponId}와 {@code benchmarkRunId}를 동시에 지정하지 못하게 하는
 * record 수준 Jakarta Validation constraint입니다.
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = MetricsScopeValidator.class)
public @interface MutuallyExclusiveMetricsScope {

    /**
     * constraint 위반 시 응답에 사용할 기본 메시지입니다.
     *
     * @return 기본 Validation 메시지
     */
    String message() default "couponId와 benchmarkRunId는 함께 지정할 수 없습니다.";

    /**
     * 이 constraint가 속할 Jakarta Validation 그룹입니다.
     *
     * @return Validation 그룹 타입
     */
    Class<?>[] groups() default {};

    /**
     * constraint 위반 메타데이터에 첨부할 payload 타입입니다.
     *
     * @return Validation payload 타입
     */
    Class<? extends Payload>[] payload() default {};
}
