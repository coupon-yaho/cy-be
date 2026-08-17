package com.kafkick.api.admin.support.validation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

/**
 * 관리자 분석 Query의 {@code from}/{@code to} 순서와 최대 1년 범위를 검증하는
 * record 수준 Jakarta Validation constraint입니다.
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = AnalyticsRangeValidator.class)
public @interface ValidAnalyticsRange {

    /**
     * constraint 위반 시 응답에 사용할 기본 메시지입니다.
     *
     * @return 기본 Validation 메시지
     */
    String message() default "조회 기간은 최대 1년이며 from은 to보다 늦을 수 없습니다.";

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
