package com.kafkick.batch.coupon.v2;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 회차 하나의 <b>게이트 쓰기</b>를 한 번에 하나만 통과시킨다. 워밍업과 재구성이 <b>같은
 * 가드를 공유한다</b> — 둘 다 카운터 세 키를 통째로 다시 쓰므로, 서로 겹치는 것도 자기끼리
 * 겹치는 것과 똑같이 07 의 그림이다(늦은 쪽이 먼저 열린 게이트 뒤에서 {@code issued} 를 지운다).
 *
 * <p><b>{@code readMeta} 선검사로는 못 막는다.</b> 검사와 첫 쓰기 사이가 열려 있고, 무엇보다
 * 먼저 시작한 쪽이 이미 게이트를 연 뒤에도 늦은 쪽은 검사를 통과한 상태로 들어온다.
 *
 * <p><b>여기까지가 이 클래스가 지는 몫이다 — 프로세스 안이 전부다.</b> {@link ConcurrentHashMap}
 * 은 JVM 힙이라, batch 가 두 대가 되는 순간(compose 의 {@code replicas: 1} 을 {@code --scale} 로
 * 이기거나 롤링 배포로 잠깐 공존하면) 이 가드는 아무것도 안 한다. 그때는 07 의 (b) — 회차 단위
 * Redis 락 — 이 선행 조건이지 나중 과제가 아니다.
 */
public class RoundGateWriteGuard {

    private final ConcurrentHashMap<Long, Boolean> inFlight = new ConcurrentHashMap<>();

    /** @return 이 스레드가 회차를 잡았으면 {@code true}. 이미 누가 잡고 있으면 {@code false} */
    public boolean tryAcquire(long couponRoundId) {
        return inFlight.putIfAbsent(couponRoundId, Boolean.TRUE) == null;
    }

    public void release(long couponRoundId) {
        inFlight.remove(couponRoundId);
    }
}
