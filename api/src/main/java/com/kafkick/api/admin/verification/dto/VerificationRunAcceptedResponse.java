package com.kafkick.api.admin.verification.dto;

import java.time.Instant;

import com.kafkick.core.admin.VerificationRunState;

/**
 * 전수 검증 실행 요청이 접수된 결과입니다.
 *
 * @param runId 새로 생성된 검증 실행 식별자
 * @param status 접수 직후 실행 상태
 * @param requestedAt 서버가 검증 요청을 접수한 시각
 */
public record VerificationRunAcceptedResponse(Long runId, VerificationRunState status, Instant requestedAt) {
}
