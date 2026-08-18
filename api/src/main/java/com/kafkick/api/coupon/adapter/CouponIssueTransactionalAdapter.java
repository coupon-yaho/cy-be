// 발급건 선점, 재고 점유, ISSUE 이력을 하나의 트랜잭션으로 묶습니다.
package com.kafkick.api.coupon.adapter;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.core.coupon.service.CouponIssueCommand;
import com.kafkick.core.coupon.service.CouponIssueService;
import com.kafkick.core.support.TimeProvider;

@Component
public class CouponIssueTransactionalAdapter {

    private final CouponIssueService couponIssueService;
    private final TimeProvider timeProvider;

    public CouponIssueTransactionalAdapter(
            CouponIssueService couponIssueService,
            TimeProvider timeProvider
    ) {
        this.couponIssueService = couponIssueService;
        this.timeProvider = timeProvider;
    }

    @Transactional
    public Issuance issue(
            Long couponRoundId,
            Long memberId,
            MembershipGrade membershipGrade,
            String requestId
    ) {
        return couponIssueService.issue(new CouponIssueCommand(
                couponRoundId,
                memberId,
                membershipGrade,
                normalizeRequestId(requestId),
                timeProvider.instant()
        ));
    }

    private static String normalizeRequestId(String requestId) {
        if (requestId == null || requestId.length() > 36) {
            return null;
        }
        return requestId;
    }
}
