package com.kafkick.core.consistency;

import com.kafkick.core.observation.EngineVersion;

/**
 * 수집된 Redis·DB 원천값을 엔진 버전과 평가 단계에 맞는 정합성 결과로 변환합니다.
 *
 * <p>구현체는 원천 시스템을 직접 조회하지 않습니다. 호출자가 수집을 끝낸
 * {@link ConsistencyRawSnapshot}을 전달하며, 계산기는 gap 산식·적용 범위·판정만 담당합니다.
 */
public interface ConsistencyCalculator {

    /**
     * 정합성 gap 4종과 초과 발급 수를 계산하고 현재 단계의 판정을 반환합니다.
     *
     * <p>{@link ConsistencyPhase#FINAL} 진입 조건 확인은 호출자의 책임입니다. 계산기는 FINAL에
     * 필요한 원천값이 유효하지 않으면 최종 판정을 만들지 않고 예외를 발생시킬 수 있습니다.
     *
     * @param snapshot Redis·DB에서 수집한 원천값과 원천별 상태·관측 시각
     * @param phase 진행 중 추세를 위한 LIVE 또는 최종 합격 판정을 위한 FINAL
     * @param engineVersion gap 적용 범위와 LIVE 심각도 정책을 결정하는 발급 엔진 버전
     * @return gap 4종, 초과 발급 수, 단계별 verdict와 severity를 포함한 평가 결과
     */
    ConsistencyEvaluation evaluate(
            ConsistencyRawSnapshot snapshot,
            ConsistencyPhase phase,
            EngineVersion engineVersion
    );
}
