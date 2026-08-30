package com.kafkick.core.admin.overview;

import java.util.Objects;

/**
 * 관리자 운영현황 Service가 HTTP 표현 계층에 전달하는 계산·조립 결과입니다.
 *
 * <p>Service는 운영 원천을 계산한 {@link AdminOverviewSnapshot}과 전체 데이터 완전성을 함께
 * 반환합니다. Controller는 이 결과를 받아 {@code AdminOverviewResponse.from(...)}으로 HTTP DTO를
 * 생성합니다. 따라서 Service가 HTTP 응답 DTO에 의존하거나 DTO가 완전성 정책을 다시 계산하지
 * 않습니다.</p>
 *
 * @param snapshot 쿠폰 회차·관측 원천별 값과 상태를 조립한 기술 중립 Snapshot
 * @param overallStatus 적용 원천 그룹의 상태를 종합한 전체 데이터 완전성
 */
public record AdminOverviewResult(
        AdminOverviewSnapshot snapshot,
        OverallStatus overallStatus
) {

    /** Service 결과가 계산값이나 완전성 없이 Controller로 전달되지 않도록 보장합니다. */
    public AdminOverviewResult {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(overallStatus, "overallStatus");
    }

    /** 관리자 운영현황 응답에 포함된 적용 원천 데이터의 전체 완전성입니다. */
    public enum OverallStatus {
        COMPLETE,
        PARTIAL,
        UNAVAILABLE
    }
}
