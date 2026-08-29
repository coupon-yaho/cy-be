package com.kafkick.core.coupon.service;

/** 정책 사전검증을 통과한 발급 시도를 트랜잭션 밖에서 알리는 계약입니다. */
@FunctionalInterface
public interface IssueAttemptCallback {

    IssueAttemptCallback NO_OP = () -> { };

    /** 정책 검증이 끝나고 권위 발급 트랜잭션을 시작하기 전에 호출합니다. */
    void onPolicyPassed();
}
