package com.kafkick.core.benchmark;

/**
 * 측정 회차의 수명주기. archive 성패는 여기 들어오지 않는다 — {@link BenchmarkArchiveStatus} 가 따로 받는다.
 *
 * <p>단계가 넷인 이유는 시각이 셋이기 때문이다. 부하 종료({@code LOAD_STOPPED})와 관측
 * 종료({@code OBSERVED})를 하나로 합치면 v3 가 부하 뒤에 Kafka 백로그를 0 으로 흘려보내는 구간이
 * 통째로 잘린다. 그 수렴 곡선이 "비동기여도 결국 맞는다" 의 증명이라, 합치는 순간 v3 의 핵심
 * 증거가 사라진다.
 *
 * <p>전이는 한 방향뿐이고 건너뛸 수 없다. 되돌리는 전이도 없다 — 측정은 되돌릴 수 없어서,
 * 잘못 닫은 회차는 상태를 되돌리는 게 아니라 새 회차를 여는 것이 맞다.
 */
public enum BenchmarkRunStatus {

    /** 부하가 흐르고 있다. 이 상태인 회차는 동시에 하나뿐이다. */
    RUNNING,

    /** 부하가 끝났고 회복을 관측하는 중이다. v3 의 수렴 구간이 여기다. */
    LOAD_STOPPED,

    /** 관측까지 끝났다. Kafka 백로그 0 을 확인한 회차만 여기 온다. */
    OBSERVED,

    /** 공식 요약까지 붙어 확정됐다. */
    FINALIZED
}
