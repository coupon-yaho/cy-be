// 쿠폰 회차와 최초 재고를 하나의 원자 단위로 저장하는 계약을 정의합니다.
package com.kafkick.core.coupon.port;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponStock;

public interface CouponRoundRepository {

    CouponRound saveWithInitialStock(
            CouponRound couponRound,
            CouponStock initialStock
    );
}
