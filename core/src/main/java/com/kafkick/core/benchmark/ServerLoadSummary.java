package com.kafkick.core.benchmark;

import java.time.Instant;

/**
 * 서버가 스스로 잰 요약. {@link ClientLoadSummary} 와 <b>같은 수치가 아니다</b> — 둘이 벌어지는
 * 폭 자체가 관측 대상이다(네트워크 왕복 · 큐 대기 · 소켓 백로그는 서버 타이머 밖에 있다).
 *
 * <p>공식 비교값은 client 쪽이다. 이 값은 대조용이라, 둘이 크게 벌어지면 "서버는 빨랐는데
 * 종단이 느렸다" 는 별개의 사실을 가리킨다. 하나로 합치면 그 사실이 사라진다.
 *
 * @param requestCount 서버가 받은 요청 수
 * @param failureCount 서버가 실패로 분류한 수
 * @param tps 서버 관측 처리량
 * @param p95Millis 서버 관측 p95
 * @param p99Millis 서버 관측 p99. 인스턴스가 여럿이면 최댓값이다 — 평균·합산은 금지다(DEC-02)
 * @param measuredAt 이 요약이 확정된 시각
 */
public record ServerLoadSummary(
        long requestCount,
        long failureCount,
        double tps,
        double p95Millis,
        double p99Millis,
        Instant measuredAt
) {

    public ServerLoadSummary {
        SummaryValues.validate("server", requestCount, failureCount, tps, p95Millis, p99Millis, measuredAt);
    }
}
