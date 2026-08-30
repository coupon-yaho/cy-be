package com.kafkick.core.admin.couponroundsource;

/** 쿠폰 회차를 열기 전에 확정해야 하는 필수 준비 항목입니다. */
public enum PreparationItem {

    COUPON_ROUND_CONFIGURATION,
    DATABASE_STOCK,
    ENGINE_CONFIGURATION,
    ISSUANCE_PATH,
    REDIS_WARMUP,
    REDIS_GATE
}
