package com.kafkick.core.observation;

public enum SourceStatus {

    VALID,
    PENDING,
    WARMING_UP,
    STALE,
    NO_TRAFFIC,
    UNAVAILABLE,

    /**
     * 그 값이 <b>정의되지 않는</b> 상태입니다. 늦게 오는 값이 아니라 없는 값입니다.
     *
     * <p>두 가지가 여기로 옵니다.</p>
     * <ul>
     *   <li><b>구성상 없다.</b> 그 회차·그 버전에 그 기능이 없다 — V1 에는 Redis 가 없으므로
     *       수집이 실패해도 장애가 아니라 해당 없음이다({@code DomainGaugeRegistrar}).</li>
     *   <li><b>[OBS-44] 지금 표본이 없어 계산이 성립하지 않는다.</b> 요청이 0 건이면 지연
     *       백분위도 실패 비율도 정의되지 않는다. 이쪽은 트래픽이 돌아오면 다시 값이 생기므로
     *       <b>영구적이지 않습니다</b> — 화면이 "이 회차에 그 기능이 없다" 로 읽으면 안 됩니다.</li>
     * </ul>
     *
     * <p>{@link #PENDING} 과 갈리는 자리입니다. PENDING 은 "값이 있어야 하는데 아직 못 읽었다"
     * 이고 N_A 는 "값이 있을 수 없다" 입니다. 원천을 못 읽어 어느 쪽인지 모르면 N_A 가 아니라
     * PENDING 입니다 — 없다고 단정하는 쪽이 더 비싼 거짓말입니다.</p>
     *
     * <p>{@link #NO_TRAFFIC} 과도 갈립니다. 저쪽은 <b>값이 0 인 것</b>이라
     * {@link #carriesValue()} 가 true 이고(초당 0 건은 정확한 처리량입니다), N_A 는 실을 값이
     * 애초에 없습니다.</p>
     */
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
