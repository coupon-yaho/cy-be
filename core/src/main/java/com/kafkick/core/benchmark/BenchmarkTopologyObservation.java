package com.kafkick.core.benchmark;

import java.util.Optional;

/** 벤치마크 시작 전에 DB가 소유한 런타임 토폴로지와 재고를 관측하는 포트다. */
public interface BenchmarkTopologyObservation {

    Integer connectionLimit();

    Optional<CouponStock> couponStock(long couponId);

    record CouponStock(int totalQuantity, int activeCount) {
    }
}
