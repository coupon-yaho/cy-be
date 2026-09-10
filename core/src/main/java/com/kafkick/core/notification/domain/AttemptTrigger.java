package com.kafkick.core.notification.domain;

import java.util.List;

public enum AttemptTrigger {
    INITIAL,
    AUTO,
    MANUAL;

    /**
     * <b>발행 명령(outbox 행)에 실제로 올 수 있는 값.</b>
     *
     * <p>{@link #AUTO} 는 Consumer 안에서 도는 재시도라 <b>행을 만들지 않는다</b> —
     * {@code notification_outbox} 의 CHECK 제약이 그것을 못 박는다.
     *
     * <p><b>왜 목록이 필요한가.</b> 선점이 종류마다 몫을 떼어 도는데, 그 순회가 이
     * 목록에서 나온다. 여기에 빠진 값이 컬럼에 들어가면 <b>어느 회차에도 안 집혀
     * 영원히 남는다</b> — 조용한 유실이다. 그래서 이 목록이 제약과 <b>정확히 같은지</b>를
     * {@code OutboxKindCoverageTest} 가 실제 스키마에서 확인한다.
     */
    public static List<AttemptTrigger> outboxKinds() {
        return List.of(INITIAL, MANUAL);
    }
}
