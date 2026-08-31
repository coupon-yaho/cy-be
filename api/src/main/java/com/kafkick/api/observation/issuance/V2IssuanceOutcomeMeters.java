package com.kafkick.api.observation.issuance;

import java.util.Objects;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.stereotype.Component;

import com.kafkick.api.observation.MeterNames;

/**
 * v2 발급의 중복·재시도 카운터 세 종.
 *
 * <p><b>합치지 않는다.</b> {@code dupPerMember} 는 다른 시도가 이미 그 회원으로 받은 거절이고
 * 나머지 둘은 같은 멱등키의 재시도다. 한 미터에 태그로 얹으면 대시보드에서는 갈리지만
 * 합산 패널·경보 규칙이 한 번만 잘못 쓰여도 {@code dupPerMember} 급증이 재시도 물결에
 * 묻힌다 — 이상 신호를 놓치는 방향이다.
 */
@Component
public final class V2IssuanceOutcomeMeters {

    private final Counter dupPerMember;
    private final Counter replayDone;
    private final Counter replayPending;

    public V2IssuanceOutcomeMeters(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry, "meterRegistry");
        dupPerMember = Counter.builder(MeterNames.ISSUANCE_V2_DUP_PER_MEMBER)
                .description("다른 시도가 이미 그 회원으로 발급받아 거절된 v2 요청")
                .register(meterRegistry);
        replayDone = Counter.builder(MeterNames.ISSUANCE_V2_REPLAY_DONE)
                .description("같은 멱등키의 재시도에 최초 응답을 재사용한 v2 요청")
                .register(meterRegistry);
        replayPending = Counter.builder(MeterNames.ISSUANCE_V2_REPLAY_PENDING)
                .description("같은 멱등키가 아직 처리 중이라 409로 떨어진 v2 요청")
                .register(meterRegistry);
    }

    public void recordDupPerMember() {
        dupPerMember.increment();
    }

    public void recordReplayDone() {
        replayDone.increment();
    }

    public void recordReplayPending() {
        replayPending.increment();
    }
}
