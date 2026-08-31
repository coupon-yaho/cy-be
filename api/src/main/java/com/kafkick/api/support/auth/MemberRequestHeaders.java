package com.kafkick.api.support.auth;

/**
 * 요청 헤더 이름의 정본.
 *
 * <p><b>둘은 한 쌍이라 접두를 같이 간다</b> — {@code X-Member-Id} · {@code X-Member-Grade}.
 * 한때 등급만 {@code X-Membership-Grade} 였는데, 앞에 서는 대기열 게이트웨이(cy-waiting)가
 * {@code X-Member-Grade} 로 보내고 있어서 게이트웨이를 통과한 요청이 발급에서 400 으로
 * 떨어졌다. 그 400 은 어느 헤더가 비었는지 안 알려 줘서 원인 규명이 오래 걸린다.
 */
public final class MemberRequestHeaders {

    public static final String MEMBER_ID = "X-Member-Id";
    public static final String MEMBER_GRADE = "X-Member-Grade";

    private MemberRequestHeaders() {
    }
}
