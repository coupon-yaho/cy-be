package com.kafkick.api.support.auth;

/**
 * 요청 헤더 이름의 정본.
 *
 * <p><b>둘은 한 쌍이라 접두를 같이 갑니다</b> — {@code X-Member-Id} · {@code X-Member-Grade}.
 * 대기열 게이트웨이도 이 두 이름을 그대로 검사합니다.
 *
 * <p>등급 헤더는 한때 {@code X-Membership-Grade} 였고 게이트웨이 도입 때 호환용으로 둘 다
 * 받으려 했는데, 재 보니 <b>옛 이름을 보내는 클라이언트가 없습니다.</b> 근거는
 * {@link MemberGradeHeaderResolver} 주석에 적어 뒀습니다.
 */
public final class MemberRequestHeaders {

    public static final String MEMBER_ID = "X-Member-Id";
    public static final String MEMBER_GRADE = "X-Member-Grade";

    private MemberRequestHeaders() {
    }
}
