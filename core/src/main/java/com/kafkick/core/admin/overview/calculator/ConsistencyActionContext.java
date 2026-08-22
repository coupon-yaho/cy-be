package com.kafkick.core.admin.overview.calculator;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.observation.EngineVersion;

/**
 * FINAL 정합성 판정을 관리자 조치 후보로 바꾸는 데 필요한 캠페인 문맥입니다.
 *
 * @param couponId 조치 대상 쿠폰 캠페인 회차 식별자
 * @param campaignName 화면에 표시할 캠페인명; 확인할 수 없으면 null
 * @param opensAt 캠페인 오픈 시각; 확인할 수 없으면 null
 * @param evaluatedAt FINAL 정합성 판정이 확정된 시각
 * @param engineVersion FINAL에서 적용할 gap을 결정하는 발급 엔진 버전
 * @param evaluation FINAL 또는 LIVE 정합성 계산 결과
 */
public record ConsistencyActionContext(
        Long couponId,
        String campaignName,
        Instant opensAt,
        Instant evaluatedAt,
        EngineVersion engineVersion,
        ConsistencyEvaluation evaluation
) {

    /** 필수 식별자·FINAL 판정 확정 시각·엔진 버전·평가 결과를 검증합니다. */
    public ConsistencyActionContext {
        Objects.requireNonNull(couponId, "couponId");
        Objects.requireNonNull(evaluatedAt, "evaluatedAt");
        Objects.requireNonNull(engineVersion, "engineVersion");
        Objects.requireNonNull(evaluation, "evaluation");
    }
}
