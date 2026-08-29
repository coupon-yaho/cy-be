package com.kafkick.core.consistency;

import java.util.Objects;

import com.kafkick.core.observation.EngineVersion;

/** 기존 {@code db_gap}/{@code list_gap}을 대체하는 정합성 gap 축입니다. */
public enum ConsistencyGapType {

    /** {@code (totalQuantity - redisRemaining) - dbActiveCount}. */
    ACTIVE_DB_GAP,

    /** {@code redisIssuedEverCount - redisMemberEverCount}. */
    LUA_GAP,

    /** {@code redisIssuedEverCount - dbIssuedEverCount}. */
    PERSIST_GAP,

    /** {@code dbActiveCount - storedActiveCount}. */
    DB_COUNTER_GAP;

    /**
     * 발급 엔진 버전에서 이 gap을 FINAL 판정에 사용하는지 확인합니다.
     *
     * <p>V1은 Redis 기반 발급 경로를 쓰지 않아 DB 카운터 대조만 적용하고, V2·V3은 네 gap 모두를
     * 적용합니다. 모든 엔진 버전을 나열한 switch이므로 새 버전이 추가되면 정책 누락을 컴파일 단계에서
     * 발견합니다.</p>
     *
     * @param engineVersion FINAL 판정에 사용할 발급 엔진 버전
     * @return 이 gap을 FINAL 정합성 판정에 사용하면 true
     * @throws NullPointerException engineVersion이 null인 경우
     */
    public boolean isApplicable(EngineVersion engineVersion) {
        Objects.requireNonNull(engineVersion, "engineVersion");
        return switch (engineVersion) {
            case V1 -> this == DB_COUNTER_GAP;
            case V2, V3 -> true;
        };
    }
}
