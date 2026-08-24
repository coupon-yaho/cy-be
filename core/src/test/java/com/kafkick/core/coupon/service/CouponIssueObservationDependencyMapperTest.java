package com.kafkick.core.coupon.service;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.dao.TransientDataAccessResourceException;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;

class CouponIssueObservationDependencyMapperTest {

    private final CouponIssueObservationDependencyMapper mapper =
            new CouponIssueObservationDependencyMapper();

    @ParameterizedTest
    @MethodSource("couponErrors")
    void mapsEveryCouponIssueError(
            CouponIssueErrorCode errorCode,
            ReasonCode reasonCode,
            Dependency dependency
    ) {
        BusinessException failure = new BusinessException(errorCode);

        assertThat(errorCode.reasonCode()).contains(reasonCode);
        assertThat(errorCode.dependency()).isEqualTo(dependency);
        assertThat(mapper.reasonCode(failure)).isEqualTo(reasonCode);
        assertThat(mapper.dependency(failure)).isEqualTo(dependency);
    }

    @Test
    void mapsUnexpectedRuntimeFailureToInternalApplicationError() {
        RuntimeException failure = new RuntimeException("unexpected");

        assertThat(mapper.reasonCode(failure))
                .isEqualTo(ReasonCode.INTERNAL_ERROR);
        assertThat(mapper.dependency(failure)).isEqualTo(Dependency.NONE);
    }

    @Test
    void mapsDataAccessFailureToInternalMySqlError() {
        RuntimeException failure = new TransientDataAccessResourceException(
                "database unavailable"
        );

        assertThat(mapper.reasonCode(failure))
                .isEqualTo(ReasonCode.INTERNAL_ERROR);
        assertThat(mapper.dependency(failure)).isEqualTo(Dependency.MYSQL);
    }

    @Test
    void findsWrappedDataAccessFailure() {
        RuntimeException failure = new RuntimeException(
                "storage wrapper",
                new TransientDataAccessResourceException(
                        "database unavailable"
                )
        );

        assertThat(mapper.dependency(failure)).isEqualTo(Dependency.MYSQL);
    }

    private static Stream<Arguments> couponErrors() {
        return Stream.of(
                Arguments.of(
                        CouponIssueErrorCode.INVALID_COUPON_ISSUE_REQUEST,
                        ReasonCode.UNMAPPED,
                        Dependency.NONE
                ),
                Arguments.of(
                        CouponIssueErrorCode.COUPON_ROUND_NOT_FOUND,
                        ReasonCode.UNMAPPED,
                        Dependency.NONE
                ),
                Arguments.of(
                        CouponIssueErrorCode.NOT_OPENED,
                        ReasonCode.NOT_OPENED,
                        Dependency.NONE
                ),
                Arguments.of(
                        CouponIssueErrorCode.CAMPAIGN_CLOSED,
                        ReasonCode.CAMPAIGN_CLOSED,
                        Dependency.NONE
                ),
                Arguments.of(
                        CouponIssueErrorCode.GRADE_NOT_ELIGIBLE,
                        ReasonCode.GRADE_NOT_ELIGIBLE,
                        Dependency.NONE
                ),
                Arguments.of(
                        CouponIssueErrorCode.ALREADY_ISSUED,
                        ReasonCode.ALREADY_ISSUED,
                        Dependency.NONE
                ),
                Arguments.of(
                        CouponIssueErrorCode.SOLD_OUT,
                        ReasonCode.STOCK_EXHAUSTED,
                        Dependency.NONE
                ),
                Arguments.of(
                        CouponIssueErrorCode.COUPON_ISSUE_SAVE_FAILED,
                        ReasonCode.INTERNAL_ERROR,
                        Dependency.MYSQL
                ),
                Arguments.of(
                        CouponIssueErrorCode.COUPON_STOCK_NOT_FOUND,
                        ReasonCode.INTERNAL_ERROR,
                        Dependency.MYSQL
                ),
                Arguments.of(
                        CouponIssueErrorCode.MEMBER_NOT_FOUND,
                        ReasonCode.UNMAPPED,
                        Dependency.NONE
                ),
                Arguments.of(
                        CouponIssueErrorCode.INVALID_TRANSITION,
                        ReasonCode.UNMAPPED,
                        Dependency.NONE
                )
        );
    }
}
