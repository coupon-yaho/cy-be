package com.kafkick.core.benchmark;

import java.time.Instant;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/** 관리자 Benchmark 목록의 선택 필터와 과거 방향 Keyset 조건입니다. */
public record BenchmarkRunQuery(
        Instant fromInclusive,
        Instant toExclusive,
        EngineVersion engineVersion,
        String scenarioCode,
        BenchmarkRunPosition before,
        int limit
) {

    /** 기본 목록 페이지 크기입니다. */
    public static final int DEFAULT_LIMIT = 50;

    /** 한 번에 읽을 수 있는 최대 회차 수입니다. */
    public static final int MAX_LIMIT = 200;

    /** 선택 기간·시나리오·페이지 크기의 유효 범위를 검증합니다. */
    public BenchmarkRunQuery {
        if (fromInclusive != null && toExclusive != null && !fromInclusive.isBefore(toExclusive)) {
            throw invalidInput("fromInclusive는 toExclusive보다 빨라야 합니다.");
        }
        if (scenarioCode != null && scenarioCode.isBlank()) {
            throw invalidInput("scenarioCode는 비어 있을 수 없습니다.");
        }
        if (limit < 1 || limit > MAX_LIMIT) {
            throw invalidInput("limit은 1에서 " + MAX_LIMIT + " 사이여야 합니다.");
        }
    }

    /** 외부 목록 조건의 오류를 공통 HTTP 400 계약으로 변환합니다. */
    private static BusinessException invalidInput(String detail) {
        return new BusinessException(CommonErrorCode.INVALID_INPUT, detail);
    }
}
