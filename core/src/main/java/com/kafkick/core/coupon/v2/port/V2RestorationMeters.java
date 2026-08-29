package com.kafkick.core.coupon.v2.port;

/**
 * 재고 복원 결과의 계측.
 *
 * <p><b>음수 반환은 장애가 아니라 답이므로 카운터로 남긴다</b>(06). 로그로만 두면
 * Prometheus 경보를 걸 자리가 없고, {@code GATE_NOT_READY} 건수가 재구성 이후 수렴 확인의
 * 입력이 되지도 못한다.
 *
 * <p>core 에 micrometer 의존이 없어 포트로 둔다. 구현이 없는 조립에서는 {@link #NONE} 이다.
 */
public interface V2RestorationMeters {

    V2RestorationMeters NONE = new V2RestorationMeters() {
        @Override
        public void recordOutcome(RestoreOutcome outcome) {
        }

        @Override
        public void recordCallFailure() {
        }

        @Override
        public void recordHaltWriteFailure() {
        }
    };

    /** 스크립트가 답을 냈다 — 성공과 네 가지 거절을 각각 센다. */
    void recordOutcome(RestoreOutcome outcome);

    /** 호출 자체가 실패했다(통신·타임아웃). 거절과 합치지 않는다 — 원인이 다르다. */
    void recordCallFailure();

    /**
     * {@code -2} 는 받았는데 중단 표식을 <b>남기지 못했다.</b> 취소 경로(api)에는 표식을 읽는
     * 쪽이 없어 프로세스 로컬 폴백이 아무 방어도 못 하므로, 그 사실이 드러나는 곳은 이 카운터뿐이다.
     */
    void recordHaltWriteFailure();
}
