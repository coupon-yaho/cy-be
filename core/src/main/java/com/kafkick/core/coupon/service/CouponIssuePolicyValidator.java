package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.query.CouponIssuePolicySnapshot;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.support.exception.BusinessException;

@Service
public class CouponIssuePolicyValidator {

    private final CouponRoundRepository couponRoundRepository;

    public CouponIssuePolicyValidator(
            CouponRoundRepository couponRoundRepository
    ) {
        this.couponRoundRepository = Objects.requireNonNull(
                couponRoundRepository
        );
    }

    /**
     * 짧은 읽기 전용 트랜잭션에서 발급 정책과 기존 발급 여부를 사전검증합니다.
     *
     * <p>회차 정책과 1인 1매 여부를 <b>한 번의 조회</b>로 읽습니다. 두 번으로 나누면 같은 트랜잭션
     * 안에서 JDBC 왕복이 두 번 생기고, 커넥션 풀이 병목인 조건에서는 그만큼 커넥션을 오래 잡습니다.
     *
     * <p>동시 요청은 조회 뒤 함께 통과할 수 있으므로 실제 발급 트랜잭션의 정책 재검증과
     * DB unique 제약이 최종 권위로 남습니다.
     *
     * @param command 발급 요청
     * @throws BusinessException 요청 또는 발급 정책을 충족하지 못한 경우
     */
    @Transactional(readOnly = true)
    public void validate(CouponIssueCommand command) {
        CouponIssuePolicy.validateCommand(command);
        CouponIssuePolicySnapshot snapshot = couponRoundRepository
                .findIssuePolicySnapshot(
                        command.couponRoundId(),
                        command.memberId()
                )
                .orElseThrow(() -> new BusinessException(
                        CouponIssueErrorCode.COUPON_ROUND_NOT_FOUND,
                        "couponRoundId=" + command.couponRoundId()
                ));
        CouponIssuePolicy.validateIssuable(snapshot.couponRound(), command);
        if (snapshot.alreadyIssued()) {
            throw new BusinessException(
                    CouponIssueErrorCode.ALREADY_ISSUED,
                    "couponRoundId=" + command.couponRoundId()
                            + ", memberId=" + command.memberId()
            );
        }
    }
}
