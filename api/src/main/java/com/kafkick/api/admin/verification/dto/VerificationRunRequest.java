package com.kafkick.api.admin.verification.dto;

import java.time.Instant;

import jakarta.validation.constraints.NotNull;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;

/**
 * 재현 가능한 전수 검증 실행을 시작하는 요청입니다.
 *
 * @param asOf 모든 원천 비교에 공통으로 적용할 결정론적 기준 시각
 * @param scope 전체 또는 증분 검증 범위
 * @param dataset 검증할 정상 또는 오염 데이터셋 유형
 */
public record VerificationRunRequest(
        @NotNull Instant asOf,
        @NotNull ScopeType scope,
        @NotNull DatasetType dataset) {
}
