package com.kafkick.api.support.auth;

import java.util.List;

import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/**
 * 게이트웨이 등급 헤더와 기존 직접 호출 헤더를 하나의 값으로 정규화합니다.
 *
 * <p>헤더별 값이 여러 개이거나 두 헤더가 모두 없거나 서로 다르면 요청을 HTTP 400으로 거부합니다.
 * 따라서 프록시와 API가 서로 다른 등급을 선택하는 모호한 요청도 함께 거부됩니다.</p>
 */
public final class MemberGradeHeaderResolver {

    private MemberGradeHeaderResolver() {
    }

    public static MembershipGrade resolve(
            List<String> memberGradeValues,
            List<String> legacyMembershipGradeValues
    ) {
        MembershipGrade memberGrade = singleValue(memberGradeValues);
        MembershipGrade legacyMembershipGrade = singleValue(legacyMembershipGradeValues);
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

    private static MembershipGrade singleValue(List<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        if (values.size() != 1) {
            throw invalid("회원 등급 헤더는 하나의 값만 허용합니다.");
        }
        try {
            return MembershipGrade.valueOf(values.getFirst().trim());
        } catch (IllegalArgumentException exception) {
            throw invalid("지원하지 않는 회원 등급입니다.");
        }
    }

    private static BusinessException invalid(String detail) {
        return new BusinessException(CommonErrorCode.INVALID_INPUT, detail);
    }
}
