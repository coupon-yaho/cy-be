package com.kafkick.core.coupon.query;

import com.kafkick.core.coupon.domain.CouponRound;

/**
 * 발급 사전검증이 한 번의 조회로 받아오는 회차 정책과 기존 발급 여부입니다.
 *
 * <p>회차 조회와 1인 1매 존재 조회를 따로 하면 같은 트랜잭션 안에서 JDBC 왕복이 두 번 생깁니다.
 * 커넥션 풀이 병목인 조건에서는 왕복 하나가 곧 커넥션 점유 시간이므로 한 쿼리로 묶습니다.
 *
 * @param couponRound 발급 정책을 평가할 회차
 * @param alreadyIssued 같은 회차에 이 회원의 발급건이 이미 있으면 {@code true}
 */
public record CouponIssuePolicySnapshot(
        CouponRound couponRound,
        boolean alreadyIssued
) {
}
