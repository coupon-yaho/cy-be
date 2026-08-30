package com.kafkick.api.support.auth;

import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/** 게이트웨이 등급 헤더와 기존 직접 호출 헤더를 하나의 값으로 정규화합니다. */
public final class MemberGradeHeaderResolver {

    private MemberGradeHeaderResolver() {
    }

    public static MembershipGrade resolve(
            MembershipGrade memberGrade,
            MembershipGrade legacyMembershipGrade
    ) {
        if (memberGrade == null && legacyMembershipGrade == null) {
            throw invalid("회원 등급 헤더가 없습니다.");
        }
        if (memberGrade != null
                && legacyMembershipGrade != null
                && memberGrade != legacyMembershipGrade) {
            throw invalid("회원 등급 헤더 값이 서로 다릅니다.");
        }
        return memberGrade != null ? memberGrade : legacyMembershipGrade;
    }

    private static BusinessException invalid(String detail) {
        return new BusinessException(CommonErrorCode.INVALID_INPUT, detail);
    }
}
