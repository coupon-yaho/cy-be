package com.kafkick.core.benchmark;

import java.time.Instant;

/**
 * 종단(부하 생성기)에서 관측한 <b>공식</b> 요약. 세 버전 비교표의 p95 · p99 · TPS 가 이 값이다.
 *
 * <p><b>부하 생성기 원본 표본으로 다시 계산한 값을 넣는다.</b> 도구가 내주는 요약값을 그대로 쓰지
 * 않는다 — 분산 실행 시 k6 OSS 는 노드별 요약을 병합하지 않아 합쳐도 전체 백분위가 아니고,
 * Locust master 는 응답시간을 두 자리 유효숫자로 버킷화한다. 도구마다 산식이 달라서, 요약값을
 * 그대로 실으면 도구를 바꾸는 순간 과거 회차와의 비교가 끊긴다.
 *
 * <p>{@link ServerLoadSummary} 와 <b>다른 타입인 것이 요점이다.</b> 두 관측 위치의 값이 한 번
 * 섞이면 비교표 전체가 무의미해지는데, 같은 타입이면 인자 순서 하나로 섞을 수 있다.
 *
 * @param requestCount 보낸 요청 수
 * @param failureCount timeout · 연결 실패를 포함한 실패 수
 * @param droppedIterations <b>회차 유효 조건은 0 이다.</b> open model 은 서버가 느려지면 VU 가 묶여
 *        도착률이 조용히 목표 아래로 떨어진다. 그걸 서버 한계로 착각하면 회차 전체가 거짓이 된다
 * @param tps 원본 표본으로 계산한 처리량
 * @param p95Millis 원본 표본으로 계산한 p95
 * @param p99Millis 원본 표본으로 계산한 p99
 * @param measuredAt 이 요약이 확정된 시각
 */
public record ClientLoadSummary(
        long requestCount,
        long failureCount,
        long droppedIterations,
        double tps,
        double p95Millis,
        double p99Millis,
        Instant measuredAt
) {

    public ClientLoadSummary {
        SummaryValues.validate("client", requestCount, failureCount, tps, p95Millis, p99Millis, measuredAt);
        if (droppedIterations < 0) {
            throw new IllegalArgumentException("droppedIterations 는 0 이상이어야 한다: " + droppedIterations);
        }
    }

    /**
     * 이 회차를 비교표에 넣어도 되는지. 도착률이 목표 아래로 떨어진 회차는 서버 수치가 아니라
     * 생성기 수치라, 통과하지 못한 회차의 p99 를 v1↔v2↔v3 비교에 쓰면 안 된다.
     *
     * @return 도착률이 목표를 지켰으면 true
     */
    public boolean arrivalRateHeld() {
        return droppedIterations == 0;
    }
}
