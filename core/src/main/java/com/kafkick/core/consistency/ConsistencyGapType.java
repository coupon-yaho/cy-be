package com.kafkick.core.consistency;

/** 기존 {@code db_gap}/{@code list_gap}을 대체하는 정합성 gap 축입니다. */
public enum ConsistencyGapType {

    /** {@code (totalQuantity - redisRemaining) - dbActiveCount}. */
    ACTIVE_DB_GAP,

    /** {@code redisIssuedEverCount - redisMemberEverCount}. */
    LUA_GAP,

    /** {@code redisIssuedEverCount - dbIssuedEverCount}. */
    PERSIST_GAP,

    /** {@code dbActiveCount - storedActiveCount}. */
    DB_COUNTER_GAP
}
