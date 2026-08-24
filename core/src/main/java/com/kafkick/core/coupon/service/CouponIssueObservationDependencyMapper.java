package com.kafkick.core.coupon.service;

import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.ErrorCode;

@Service
public class CouponIssueObservationDependencyMapper {

    /**
     * 발급 실패를 응답 상태·사유·의존성의 단일 관측 결과로 분류합니다.
     *
     * @param failure 발급 흐름에서 발생한 예외
     * @return Session에 그대로 전달할 최종 관측 분류
     */
    public CouponIssueObservationFailure classify(
            RuntimeException failure
    ) {
        int httpStatus = failure instanceof BusinessException businessException
                ? businessException.getErrorCode().getStatus()
                : 500;
        return new CouponIssueObservationFailure(
                httpStatus,
                reasonCode(failure),
                dependency(failure)
        );
    }

    /**
     * 발급 실패의 관측 사유를 업무 오류 매핑 또는 내부 오류로 분류합니다.
     *
     * @param failure 발급 흐름에서 발생한 예외
     * @return 관측 이벤트 사유
     */
    public ReasonCode reasonCode(RuntimeException failure) {
        if (failure instanceof BusinessException businessException) {
            ErrorCode errorCode = businessException.getErrorCode();
            return errorCode.reasonCode().orElseGet(() ->
                    errorCode.getStatus() >= 500
                            ? ReasonCode.INTERNAL_ERROR
                            : ReasonCode.UNMAPPED
            );
        }
        return ReasonCode.INTERNAL_ERROR;
    }

    /**
     * 발급 실패의 직접 의존성을 오류 코드와 원인 예외에서 분류합니다.
     *
     * @param failure 발급 흐름에서 발생한 예외
     * @return MySQL 계열이면 {@link Dependency#MYSQL}, 아니면 오류 코드 또는 NONE
     */
    public Dependency dependency(RuntimeException failure) {
        if (failure instanceof BusinessException businessException) {
            ErrorCode errorCode = businessException.getErrorCode();
            Dependency mapped = errorCode.dependency();
            // CouponIssueErrorCode의 NONE도 확정 매핑이며 DB cause가 덮어쓰지 않는다.
            if (errorCode instanceof CouponIssueErrorCode) {
                return mapped;
            }
            if (mapped != Dependency.NONE) {
                return mapped;
            }
        }
        return hasDataAccessCause(failure)
                ? Dependency.MYSQL
                : Dependency.NONE;
    }

    /** 원인 체인에서 Spring DataAccess 계열 예외를 찾습니다. */
    private static boolean hasDataAccessCause(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof DataAccessException) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                return false;
            }
            current = cause;
        }
        return false;
    }
}
