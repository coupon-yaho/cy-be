package com.kafkick.core.coupon.v2.query;

import java.time.Instant;
import java.util.List;

/** V2 조회가 L1/L2에 보관할 수 있는 불변 회차 정의의 읽기 포트다. */
public interface CouponDefinitionQueryPort {

    List<CouponDefinition> findCandidates(Instant asOf);
}
