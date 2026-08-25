package com.kafkick.core.observation;

public enum ReasonCode {

    NOT_OPENED,
    CAMPAIGN_CLOSED,
    GRADE_NOT_ELIGIBLE,
    QUEUE_REQUIRED,
    NO_ENTRY_TOKEN,
    ENTRY_TOKEN_EXPIRED,
    ALREADY_ISSUED,
    STOCK_EXHAUSTED,

    /**
     * 그 요청을 받을 수 없는 상태 전이입니다.
     *
     * <p><b>서버 실패가 아닙니다.</b> {@code NOT_OPENED}·{@code CAMPAIGN_CLOSED}·
     * {@code GRADE_NOT_ELIGIBLE} 과 같은 축입니다 — "이 회차는 지금 그 요청을 받을 상태가
     * 아니다". 그래서 {@code OverviewPrometheusContract.isFailure()} 는 false 이고 O3 는
     * {@code INELIGIBLE} 로 접습니다. 짝이 어긋나면 실패 원인 표에는 안 보이는데 O3 에서는
     * 시스템 실패로 잡힙니다.</p>
     *
     * <p>⚠️ <b>아직 아무도 내지 않습니다.</b> 값이 흐르려면 {@code ErrorCode.reasonCode()} 에
     * 매핑이 있어야 합니다. 관측 계약을 먼저 세운 것은 이 상수가 뒤늦게 들어올 때
     * {@code isFailure()} 의 default 없는 switch 가 빌드를 깨뜨리기 때문입니다 — 그때 원인을
     * 찾는 비용이 지금 미리 자리를 여는 비용보다 큽니다.</p>
     */
    INVALID_TRANSITION,

    TEMPORARILY_UNAVAILABLE,
    INTERNAL_ERROR,
    UNMAPPED
}
