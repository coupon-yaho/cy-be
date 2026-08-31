package com.kafkick.api.support.auth;

import java.util.List;

import com.kafkick.core.membership.domain.MembershipGrade;

/**
 * 회원 등급 헤더 하나를 값으로 바꾸면서 모호한 요청을 걷어냅니다.
 *
 * <p>헤더가 없거나, 값이 여러 개이거나, 아는 등급이 아니면 HTTP 400 으로 거부합니다.
 * {@code @RequestHeader MembershipGrade} 로 바로 받으면 값이 둘 실렸을 때 스프링이
 * <b>앞의 것을 조용히</b> 고릅니다 — 어느 값으로 판정했는지가 응답에도 로그에도 안 남습니다.
 *
 * <h2>이름은 하나다</h2>
 *
 * <p>서버가 읽는 이름은 {@code X-Member-Grade} 뿐입니다. 대기열 게이트웨이는 헤더를
 * <b>넣지도 지우지도 않고</b> 이 헤더가 없으면 요청을 거부합니다
 * (cy-waiting 의 {@code MemberIdentityFilter}). 따라서 게이트웨이를 거치든 직행이든
 * 서버의 회원 등급 판정은 같은 헤더 하나를 사용합니다.
 *
 */
public final class MemberGradeHeaderResolver {

    private MemberGradeHeaderResolver() {
    }

    /**
     * 등급 헤더 값 목록을 등급 하나로 바꿉니다.
     *
     * @param values {@code X-Member-Grade} 로 실려 온 값들. 없으면 {@code null} 또는 빈 목록
     * @return 판정된 등급
     * @throws RequestHeaderContractException 헤더가 없거나 값이 여럿이거나 아는 등급이 아닐 때.
     *         그 문구는 <b>응답에 그대로 나갑니다</b> — 호출자가 무엇을 고쳐야 하는지
     *         응답만 보고 알아야 하는 자리라 카탈로그 문구로 뭉개지 않습니다
     */
    public static MembershipGrade resolve(List<String> values) {
        if (values == null || values.isEmpty()) {
            throw invalid(RequestHeaderContractException.Reason.MISSING_MEMBER_GRADE);
        }
        if (values.size() != 1) {
            throw invalid(RequestHeaderContractException.Reason.MULTIPLE_MEMBER_GRADE);
        }
        try {
            return MembershipGrade.valueOf(values.getFirst().trim());
        } catch (IllegalArgumentException exception) {
            throw invalid(RequestHeaderContractException.Reason.UNKNOWN_MEMBER_GRADE);
        }
    }

    private static RequestHeaderContractException invalid(
            RequestHeaderContractException.Reason reason) {
        return new RequestHeaderContractException(reason);
    }
}
