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
     * <p>Core 원천 관측과 관리자 HTTP 관측값은 이 메서드를 공통 기준으로 사용합니다.
     * 모든 enum 상수를 나열한 switch 식이므로 상태가 추가되면 분류 누락을 컴파일 단계에서 발견합니다.</p>
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
