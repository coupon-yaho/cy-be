package com.kafkick.core.observation;

public enum SourceStatus {

    VALID,
    PENDING,
    WARMING_UP,
    STALE,
    NO_TRAFFIC,
    UNAVAILABLE,
    N_A;

    /**
     * 이 상태가 실제 값과 관측 시각을 함께 실어야 하는지 여부입니다.
     *
     * <p>같은 분할을 {@code SourceObservation} 과 {@code ObservedValue} 가 각자 적고 있어
     * 세 곳이 갈라질 수 있습니다. 새로 읽는 쪽은 이 메서드를 씁니다.
     *
     * @return 값·시각이 있어야 하면 true, 둘 다 null 이어야 하면 false
     */
    public boolean carriesValue() {
        return switch (this) {
            case VALID, WARMING_UP, STALE, NO_TRAFFIC -> true;
            case PENDING, UNAVAILABLE, N_A -> false;
        };
    }
}
