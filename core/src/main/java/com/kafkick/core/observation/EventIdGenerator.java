package com.kafkick.core.observation;

import java.util.UUID;

@FunctionalInterface
public interface EventIdGenerator {

    /** 새 관측 이벤트마다 새로운 식별자를 생성한다. */
    UUID generate();
}
