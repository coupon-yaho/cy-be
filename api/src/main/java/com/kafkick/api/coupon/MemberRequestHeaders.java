// 로그인 대신 가상 회원의 식별자와 멤버십 등급을 전달하는 헤더 규격입니다.
package com.kafkick.api.coupon;

public final class MemberRequestHeaders {

    public static final String MEMBER_ID = "X-Member-Id";
    public static final String MEMBERSHIP_GRADE = "X-Membership-Grade";

    private MemberRequestHeaders() {
    }
}
