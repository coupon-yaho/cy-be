// 시각에 기대는 테스트가 쓰는 고정 시계입니다.
package com.kafkick.batch.config;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * <b>운영 {@code TimeConfig} 가 {@code systemUTC} 다.</b> 테스트가 기본 타임존으로 시계를
 * 고정하면 CI 와 로컬이 다른 값을 내고 <b>고정한 의미가 없어진다.</b> 그 결합을 한 곳에 둔다.
 *
 * <p>값(고정할 "지금")은 테스트마다 다르므로 공유하지 않는다 — 공유하는 것은 <b>UTC 로
 * 맞춘다는 결정</b> 하나다. 그것이 세 곳에 복붙돼 있었다.
 */
public final class FixedClock {

    private FixedClock() {
    }

    public static Clock at(LocalDateTime now) {
        return Clock.fixed(now.toInstant(ZoneOffset.UTC), ZoneOffset.UTC);
    }
}
