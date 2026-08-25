package com.kafkick.core.observation.attempt;

/**
 * live 화면이 읽는 버퍼에 한 건을 넣는다. 구현은 {@code infra:redis} 의 Redis Stream 이다.
 *
 * <p>포트를 core 에 두는 이유는 {@link com.kafkick.core.observation.EventRecorder} 와 같다 —
 * 발행하는 쪽({@code infra:mq} 컨슈머)이 저장 기술을 컴파일 타임에 몰라야 두 인프라 모듈이
 * 서로를 안 물고, "v1 발급 경로에 Redis 미사용" 이 의존 그래프에서 계속 읽힌다.
 *
 * <p><b>구현은 던지지 않는 것을 약속하지 않는다.</b> Redis 가 죽으면 던진다. 부르는 쪽이
 * 그것을 삼키고 offset 을 넘겨야 한다 — 화면 버퍼의 장애가 Kafka 소비를 멈추면, 같은 토픽을
 * 읽는 archive 는 멀쩡한데 화면 하나 때문에 컨슈머 그룹이 리밸런싱을 반복한다.
 */
public interface AttemptLiveSink {

    /**
     * 정제본 한 건을 버퍼 끝에 넣는다.
     *
     * @param entry 화면용으로 정제된 이벤트
     */
    void append(AttemptLiveEntry entry);
}
