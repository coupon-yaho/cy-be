package com.kafkick.api.support.auth;

public final class MemberRequestHeaders {

    public static final String MEMBER_ID = "X-Member-Id";
    /** 대기열 게이트웨이 v0.4.0이 전달하는 회원 등급 헤더입니다. */
    public static final String MEMBER_GRADE = "X-Member-Grade";
    /** 기존 직접 호출자를 위한 호환 헤더입니다. */
    public static final String MEMBERSHIP_GRADE = "X-Membership-Grade";

    private MemberRequestHeaders() {
    }
}
