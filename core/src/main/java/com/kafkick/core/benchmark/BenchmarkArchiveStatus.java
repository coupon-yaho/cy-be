package com.kafkick.core.benchmark;

/**
 * 완료 회차 시계열 archive(OBS-22)의 결과. {@link BenchmarkRunStatus} 와 <b>독립이다.</b>
 *
 * <p>archive 가 실패해도 회차 자체는 완료다. 하나의 상태로 합치면 두 가지를 동시에 잃는다 —
 * 실패한 archive 때문에 회차가 영원히 미완으로 남고, "회차는 끝났으니 archive 만 다시 돌리면
 * 된다" 는 재실행 조건을 표현할 자리가 없어진다.
 */
public enum BenchmarkArchiveStatus {

    /** 아직 돌지 않았다. 회차가 확정되기 전에는 정상 상태다. */
    NONE,

    /** retry 소유권을 얻어 실행 중이다. lease가 만료되면 다른 프로세스가 회수할 수 있다. */
    IN_PROGRESS,

    DONE,

    /** 실패 이유를 반드시 함께 남긴다 — 재실행 판단의 유일한 근거다. */
    FAILED
}
