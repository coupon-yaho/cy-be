package com.kafkick.api.support.auth;

import java.util.List;

import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/**
 * 회원 등급 헤더 하나를 값으로 바꾸면서 모호한 요청을 걷어냅니다.
 *
 * <p>헤더가 없거나, 값이 여러 개이거나, 아는 등급이 아니면 HTTP 400 으로 거부합니다.
 * {@code @RequestHeader MembershipGrade} 로 바로 받으면 값이 둘 실렸을 때 스프링이
 * <b>앞의 것을 조용히</b> 고릅니다 — 어느 값으로 판정했는지가 응답에도 로그에도 안 남습니다.
 *
 * <h2>이름은 하나다</h2>
 *
 * <p>이 서버가 회원 등급 판정에 사용하는 이름은 {@code X-Member-Grade} 하나입니다.
 * 이 헤더가 없거나 값이 여러 개이면 아래 {@link #resolve(List)}가 요청을 거부합니다.
 *
 * <p><b>다만 CORS 허용 목록에는 옛 이름이 남아 있습니다</b>
 * ({@link MemberRequestHeaders#LEGACY_MEMBER_GRADE}). 이 이름이 요청 헤더에 포함돼도 브라우저의
 * CORS 프리플라이트를 통과하게 하는 호환 경계이며, 이 리졸버는 그 값을 읽지 않습니다.
 */
public final class MemberGradeHeaderResolver {

    private MemberGradeHeaderResolver() {
    }

    /**
     * 등급 헤더 값 목록을 등급 하나로 바꿉니다.
     *
     * @param values {@code X-Member-Grade} 로 실려 온 값들. 없으면 {@code null} 또는 빈 목록
     * @return 판정된 등급
     * @throws BusinessException 헤더가 없거나 값이 여럿이거나 아는 등급이 아닐 때
     */
    public static MembershipGrade resolve(List<String> values) {
        if (values == null || values.isEmpty()) {
            throw invalid("회원 등급 헤더가 없습니다.");
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
