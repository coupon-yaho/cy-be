package com.kafkick.core.coupon.domain;

import java.time.Instant;

import com.kafkick.core.membership.domain.MembershipGrade;
import java.time.temporal.ChronoUnit;

import com.kafkick.core.coupon.exception.CouponExpiredException;

public record Issuance(
        Long id,
        Long couponRoundId,
        Long memberId,
        String code,
        MembershipGrade issuedGrade,
        IssuanceStatus status,
        Instant issuedAt,
        Instant expiresAt,
        Instant updatedAt
) {

    private static final int CODE_LENGTH = 16;

    public Issuance {
        validateId(id, "발급 ID", true);
        validateId(couponRoundId, "쿠폰 회차 ID", false);
        validateId(memberId, "회원 ID", false);
        if (code == null || code.length() != CODE_LENGTH) {
            throw new IllegalArgumentException(
                    "쿠폰 코드는 16자리여야 합니다."
            );
        }
        if (issuedGrade == null) {
            throw new IllegalArgumentException(
                    "발급 시점 멤버십 등급은 필수입니다."
            );
        }
        if (status == null) {
            throw new IllegalArgumentException(
                    "쿠폰 발급 상태는 필수입니다."
            );
        }
        if (issuedAt == null || expiresAt == null
                || !expiresAt.isAfter(issuedAt)) {
            throw new IllegalArgumentException(
                    "쿠폰 만료 시각은 발급 시각보다 늦어야 합니다."
            );
        }
        if (updatedAt == null || updatedAt.isBefore(issuedAt)) {
            throw new IllegalArgumentException(
                    "쿠폰 갱신 시각은 발급 시각보다 빠를 수 없습니다."
            );
        }
    }

    public static Issuance issue(
            Long couponRoundId,
            Long memberId,
            String code,
            MembershipGrade issuedGrade,
            int validDays,
            Instant issuedAt
    ) {
        if (issuedAt == null) {
            throw new IllegalArgumentException(
                    "쿠폰 발급 시각은 필수입니다."
            );
        }
        if (validDays <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 유효기간은 0보다 커야 합니다."
            );
        }

        Instant expiresAt = issuedAt.plus(
                validDays,
                ChronoUnit.DAYS
        );

        return new Issuance(
                null,
                couponRoundId,
                memberId,
                code,
                issuedGrade,
                CouponStateMachine.transition(
                        null,
                        IssuanceEventType.ISSUE,
                        expiresAt,
                        issuedAt
                ),
                issuedAt,
                expiresAt,
                issuedAt
        );
    }

    public static Issuance restore(
            Long id,
            Long couponRoundId,
            Long memberId,
            String code,
            MembershipGrade issuedGrade,
            IssuanceStatus status,
            Instant issuedAt,
            Instant expiresAt,
            Instant updatedAt
    ) {
        if (id == null) {
            throw new IllegalArgumentException(
                    "복원할 발급 ID는 필수입니다."
            );
        }
        return new Issuance(
                id,
                couponRoundId,
                memberId,
                code,
                issuedGrade,
                status,
                issuedAt,
                expiresAt,
                updatedAt
        );
    }

    public Issuance use(Instant usedAt) {
        if (usedAt == null) {
            throw new IllegalArgumentException(
                    "쿠폰 사용 시각은 필수입니다."
            );
        }
        if (usedAt.isAfter(expiresAt)) {
            throw new CouponExpiredException(id);
        }

        return new Issuance(
                id,
                couponRoundId,
                memberId,
                code,
                issuedGrade,
                CouponStateMachine.transition(
                        status,
                        IssuanceEventType.USE,
                        expiresAt,
                        usedAt
                ),
                issuedAt,
                expiresAt,
                usedAt
        );
    }

    public Issuance cancelUse(Instant canceledAt) {
        if (canceledAt == null) {
            throw new IllegalArgumentException(
                    "쿠폰 사용 취소 시각은 필수입니다."
            );
        }

        return new Issuance(
                id,
                couponRoundId,
                memberId,
                code,
                issuedGrade,
                CouponStateMachine.transition(
                        status,
                        IssuanceEventType.CANCEL_USE,
                        expiresAt,
                        canceledAt
                ),
                issuedAt,
                expiresAt,
                canceledAt
        );
    }

    public Issuance cancel(Instant canceledAt) {
        if (canceledAt == null) {
            throw new IllegalArgumentException(
                    "쿠폰 발급 취소 시각은 필수입니다."
            );
        }
        if (canceledAt.isAfter(expiresAt)) {
            throw new CouponExpiredException(id);
        }

        return new Issuance(
                id,
                couponRoundId,
                memberId,
                code,
                issuedGrade,
                CouponStateMachine.transition(
                        status,
                        IssuanceEventType.CANCEL,
                        expiresAt,
                        canceledAt
                ),
                issuedAt,
                expiresAt,
                canceledAt
        );
    }

    public Issuance expire(Instant expiredAt) {
        if (expiredAt == null) {
            throw new IllegalArgumentException(
                    "쿠폰 만료 처리 시각은 필수입니다."
            );
        }

        return new Issuance(
                id,
                couponRoundId,
                memberId,
                code,
                issuedGrade,
                CouponStateMachine.transition(
                        status,
                        IssuanceEventType.EXPIRE,
                        expiresAt,
                        expiredAt
                ),
                issuedAt,
                expiresAt,
                expiredAt
        );
    }

    private static void validateId(
            Long value,
            String fieldName,
            boolean nullable
    ) {
        if ((!nullable && value == null) || (value != null && value <= 0)) {
            throw new IllegalArgumentException(
                    fieldName + "는 0보다 커야 합니다."
            );
        }
    }
}
