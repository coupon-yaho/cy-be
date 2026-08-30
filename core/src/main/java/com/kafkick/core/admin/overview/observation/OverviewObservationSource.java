package com.kafkick.core.admin.overview.observation;

/**
 * 관리자 운영현황의 기술 중립 관측 입력을 조회합니다.
 *
 * <p>구현체는 요청한 동일 쿠폰 회차 모집단의 O1과 전체 O3·발급률·지연 관측을 반환합니다. HTTP, 특정
 * 시계열 저장소, API 표현 모델은 이 계약 밖에서 변환합니다.</p>
 */
@FunctionalInterface
public interface OverviewObservationSource {

    /**
     * 요청한 시점과 쿠폰 회차 모집단의 관측 입력을 조회합니다.
     *
     * @param request 하나의 운영현황 조립에 사용할 기준 시각과 쿠폰 회차 대상
     * @return O1·O3·전체 발급률·지연 관측을 포함한 기술 중립 입력
     */
    OverviewObservationData observe(OverviewObservationRequest request);
}
