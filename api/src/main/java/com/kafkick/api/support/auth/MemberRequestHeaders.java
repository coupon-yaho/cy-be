package com.kafkick.api.support.auth;

/**
 * 요청 헤더 이름의 정본.
 *
 * <p><b>둘은 한 쌍이라 접두를 같이 갑니다</b> — {@code X-Member-Id} · {@code X-Member-Grade}.
 * 대기열 게이트웨이도 이 두 이름을 그대로 검사합니다.
 *
 * <p>서버가 읽는 등급 헤더의 정본은 {@code X-Member-Grade} 하나다. 옛 이름인
 * {@code X-Membership-Grade}는 프론트 전환기 요청의 CORS 프리플라이트를 허용하기 위해
 * 상수로만 남아 있으며, {@link MemberGradeHeaderResolver}는 그 값을 읽지 않는다.
 */
public final class MemberRequestHeaders {

    public static final String MEMBER_ID = "X-Member-Id";
    public static final String MEMBER_GRADE = "X-Member-Grade";

    private MemberRequestHeaders() {
    }
}
