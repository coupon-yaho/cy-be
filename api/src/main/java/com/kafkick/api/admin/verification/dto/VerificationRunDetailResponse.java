package com.kafkick.api.admin.verification.dto;

import java.util.List;

import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.VerdictType;

/** 검증 실행의 판정과 finding 근거를 반환합니다. */
public record VerificationRunDetailResponse(Long runId, VerdictType verdict, long missingCount,
                                            long falsePositiveCount, List<VerificationFindingItem> findings) {
    /**
     * 검증 규칙 하나가 발견한 불일치 근거입니다.
     *
     * @param findingType 적용된 검증 규칙
     * @param targetKey 원천 간 동일 대상을 식별하는 안정적인 키
     * @param expected 권위 원천에서 기대한 값
     * @param actual 비교 원천에서 실제 관측한 값
     */
    public record VerificationFindingItem(FindingType findingType, String targetKey, String expected, String actual) { }
}
