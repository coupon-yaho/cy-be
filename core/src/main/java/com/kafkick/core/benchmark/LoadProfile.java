package com.kafkick.core.benchmark;

import com.kafkick.core.support.exception.BusinessException;

/**
 * 회차에 건 부하의 입력값. <b>램프가 아니라 스파이크다.</b>
 *
 * <p>선착순이라 오픈 0초에 전원이 몰린다. 천천히 올리는 부하는 서버에 워밍업 시간을 줘서
 * <b>재고가 소진되는 첫 1초의 거동을 놓친다</b> — 그 1초가 측정 대상이다.
 *
 * <p>{@code offeredRps} 는 총량이 아니라 <b>도착률</b>이다. 2만 명이 오픈 순간 각자 한 번 쏘면
 * offered 20,000 RPS 이고, 낮은 도착률로 오래 쏘면 총량만 채워지고 그 순간은 재현되지 않는다.
 *
 * @param offeredRps 도착률. 스파이크 높이
 * @param loadHoldSeconds 스파이크 유지 시간. {@code offeredRps × 이 값} 이 회차 총 요청 수다
 * @param observationHoldSeconds 부하 종료 뒤 회복을 관측하는 시간. v3 의 수렴 구간이 여기 들어간다
 * @param stockTotal 이 회차 쿠폰의 재고. 회차마다 새 쿠폰을 쓰므로 재고 리셋 절차가 없다
 * @param generatorIdleRttMillis 회차 전에 한 번 잰 유휴 RTT. 나중에 서버 지연과 네트워크를
 *        가르는 근거다 — 생성기가 측정 대상 밖 별도 머신에 있으므로 이 값이 없으면 구분이 안 된다
 */
public record LoadProfile(
        int offeredRps,
        int loadHoldSeconds,
        int observationHoldSeconds,
        Integer stockTotal,
        Double generatorIdleRttMillis
) {

    public LoadProfile {
        if (offeredRps <= 0) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,"offeredRps 는 0 보다 커야 한다: " + offeredRps);
        }
        if (loadHoldSeconds <= 0) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,"loadHoldSeconds 는 0 보다 커야 한다: " + loadHoldSeconds);
        }
        if (observationHoldSeconds < 0) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,
                    "observationHoldSeconds 는 0 이상이어야 한다: " + observationHoldSeconds);
        }
        if (stockTotal != null && stockTotal < 0) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,"stockTotal 은 0 이상이어야 한다: " + stockTotal);
        }
        if (generatorIdleRttMillis != null && generatorIdleRttMillis < 0) {
            throw new BusinessException(BenchmarkErrorCode.INVALID_RUN_CONDITION,
                    "generatorIdleRttMillis 는 0 이상이어야 한다: " + generatorIdleRttMillis);
        }
    }
}
