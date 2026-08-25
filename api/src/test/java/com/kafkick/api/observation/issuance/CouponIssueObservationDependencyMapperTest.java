package com.kafkick.api.observation.issuance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.dao.TransientDataAccessResourceException;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;

class CouponIssueObservationDependencyMapperTest {

    private final CouponIssueObservationDependencyMapper mapper =
            new CouponIssueObservationDependencyMapper();

    @Test
    void mappingCasesCoverEveryCouponIssueErrorCode() {
        assertThat(CouponIssueErrorCode.values()).containsExactlyInAnyOrder(
                CouponIssueErrorCode.INVALID_COUPON_ISSUE_REQUEST,
                CouponIssueErrorCode.COUPON_ROUND_NOT_FOUND,
                CouponIssueErrorCode.NOT_OPENED,
                CouponIssueErrorCode.CAMPAIGN_CLOSED,
                CouponIssueErrorCode.GRADE_NOT_ELIGIBLE,
                CouponIssueErrorCode.ALREADY_ISSUED,
                CouponIssueErrorCode.SOLD_OUT,
                CouponIssueErrorCode.COUPON_ISSUE_SAVE_FAILED,
                CouponIssueErrorCode.COUPON_STOCK_NOT_FOUND,
                CouponIssueErrorCode.MEMBER_NOT_FOUND,
                CouponIssueErrorCode.INVALID_TRANSITION
        );
    }

    @ParameterizedTest
    @CsvSource({
            "INVALID_COUPON_ISSUE_REQUEST, 400, UNMAPPED, NONE",
            "COUPON_ROUND_NOT_FOUND, 404, UNMAPPED, NONE",
            "NOT_OPENED, 409, NOT_OPENED, NONE",
            "CAMPAIGN_CLOSED, 409, CAMPAIGN_CLOSED, NONE",
            "GRADE_NOT_ELIGIBLE, 403, GRADE_NOT_ELIGIBLE, NONE",
            "ALREADY_ISSUED, 409, ALREADY_ISSUED, NONE",
            "SOLD_OUT, 409, STOCK_EXHAUSTED, NONE",
            "COUPON_ISSUE_SAVE_FAILED, 500, INTERNAL_ERROR, MYSQL",
            "COUPON_STOCK_NOT_FOUND, 500, INTERNAL_ERROR, MYSQL",
            "MEMBER_NOT_FOUND, 404, UNMAPPED, NONE",
            "INVALID_TRANSITION, 409, INVALID_TRANSITION, NONE"
    })
    void mapsEveryCouponIssueError(
            CouponIssueErrorCode errorCode,
            int expectedHttpStatus,
            ReasonCode expectedReasonCode,
            Dependency expectedDependency
    ) {
        BusinessException failure = new BusinessException(errorCode);
        CouponIssueObservationFailure classified = mapper.classify(failure);

        assertThat(classified.httpStatus()).isEqualTo(expectedHttpStatus);
        assertThat(classified.reasonCode()).isEqualTo(expectedReasonCode);
        assertThat(classified.dependency()).isEqualTo(expectedDependency);
        assertThat(mapper.reasonCode(failure)).isEqualTo(expectedReasonCode);
        assertThat(mapper.dependency(failure)).isEqualTo(expectedDependency);
    }

    @Test
    void mapsUnexpectedRuntimeFailureToInternalApplicationError() {
        RuntimeException failure = new RuntimeException("unexpected");

        assertThat(mapper.reasonCode(failure))
                .isEqualTo(ReasonCode.INTERNAL_ERROR);
        assertThat(mapper.dependency(failure)).isEqualTo(Dependency.NONE);
        assertThat(mapper.classify(failure).httpStatus()).isEqualTo(500);
    }

    @Test
    void mapsDataAccessFailureToInternalMySqlError() {
        RuntimeException failure = new TransientDataAccessResourceException(
                "database unavailable"
        );

        assertThat(mapper.reasonCode(failure))
                .isEqualTo(ReasonCode.INTERNAL_ERROR);
        assertThat(mapper.dependency(failure)).isEqualTo(Dependency.MYSQL);
        assertThat(mapper.classify(failure).httpStatus()).isEqualTo(500);
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

}
