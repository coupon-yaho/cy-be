package com.kafkick.core.consistency;

/** AB-G0에서 legacy db_gap/list_gap을 폐기하고 확정한 네 축이다. */
public enum ConsistencyGapType {

    ACTIVE_DB_GAP,
    LUA_GAP,
    PERSIST_GAP,
    DB_COUNTER_GAP
}
