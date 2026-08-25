package com.kafkick.core.observation.attempt;

/**
 * 도착한 한 건을 {@code issue_attempts} 에 적재한다. 구현은 {@code storage} 다.
 *
 * <p><b>멱등이어야 한다.</b> 리밸런싱 후 재소비는 예외가 아니라 정상 경로다 — 컨슈머 그룹이
 * 파티션을 다시 나눠 가지면 마지막 커밋 이후 구간이 그대로 다시 온다. 그때 같은 이벤트가
 * 두 번 들어오는 것을 이 계층이 흡수하지 못하면, 적재가 실패하고 offset 을 못 넘겨
 * 같은 자리에서 무한 재시도한다.
 *
 * <p>중복의 판정 기준은 두 유니크 키다 — {@code event_id} 와 {@code (topic, partition, offset)}.
 * 둘 중 무엇에 걸렸든 "이미 처리됨" 이고, 구현은 정상 반환해야 한다.
 */
public interface AttemptArchive {

    /**
     * 한 건을 적재한다. 이미 있으면 아무것도 하지 않고 정상 반환한다.
     *
     * @param record 도착 좌표가 붙은 한 건
     * @return 새로 적재했으면 true, 이미 있어 건너뛰었으면 false
     */
    boolean append(AttemptRecord record);
}
