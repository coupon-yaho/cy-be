package com.kafkick.core.admin.inquiry;

import java.time.Instant;

/** 요청 조건과 관측 시각을 기준으로 발급 문의 후보 원천 행을 읽는 포트입니다. */
@FunctionalInterface
public interface AdminIssuanceInquirySourceReader {

    /**
     * 회원 존재를 먼저 확인하고 선택 쿠폰을 확인한 뒤 같은 관측 시각의 후보를 반환합니다.
     *
     * <p>Storage 구현은 AVAILABLE 원천에 query의 회원·선택 쿠폰 조건, snapshotAt 경계,
     * requestId별 대표 attempt 선택 후 HTTP 상태·사유 필터, 원천별 Cursor 경계와 limit + 1을
     * 적용해야 합니다. Calculator는 합친 후보의 전역 정렬·Cursor·연결 규칙을 방어적으로 다시
     * 검증합니다.
     *
     * @param query 회원·쿠폰·결과 필터와 Cursor 조건
     * @param snapshotAt 한 요청에서 한 번 확정한 관측 기준 시각
     * @return 존재 상태와 세 원천 조회 결과
     */
    AdminIssuanceInquiryReadResult read(AdminIssuanceInquiryQuery query, Instant snapshotAt);
}
