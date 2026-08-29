package com.kafkick.core.admin.issuancehistory;

import java.time.Instant;

/** 기술 구현에 독립적으로 관리자 발급 상태 변경 이력을 읽습니다. */
public interface AdminIssuanceHistoryReader {

    /**
     * 업무 필터에 맞는 제한 후보와 전체 모집단 요약을 읽습니다.
     *
     * @param query 업무 필터와 Keyset 위치
     * @param snapshotAt 요청에서 확정한 조회 상한 시각
     * @return DB 단계에서 제한한 후보와 Cursor 이전 전체 요약
     */
    AdminIssuanceHistoryReadResult read(AdminIssuanceHistoryQuery query, Instant snapshotAt);
}
