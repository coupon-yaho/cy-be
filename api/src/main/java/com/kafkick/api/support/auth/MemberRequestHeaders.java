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

    /**
     * 옛 등급 헤더 이름. <b>서버는 이 값을 안 읽는다.</b>
     *
     * <p>남아 있는 자리는 {@code ApiCorsConfig} 의 허용 목록 하나뿐이다. 프론트가 게이트웨이
     * 전환기 동안 두 이름으로 함께 보내는데, 허용 목록에서 빼면 그 요청이 프리플라이트에서
     * 막혀 화면이 통째로 죽는다 — <b>값을 무시하는 것과 보낼 수 있는 것은 다른 층이다.</b>
     *
     * <p>프론트가 옛 이름을 그만 보내면 이 상수와 허용 목록의 그 줄을 함께 지운다.
     */
    public static final String LEGACY_MEMBER_GRADE = "X-Membership-Grade";

    private MemberRequestHeaders() {
    }
}
