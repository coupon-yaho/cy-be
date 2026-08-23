package com.kafkick.core.admin.inquiry;

import java.time.Instant;

/** 한 요청의 관측 시각을 기준으로 발급 문의 원천 행을 읽는 포트입니다. */
@FunctionalInterface
public interface AdminIssuanceInquirySourceReader {

    /** 같은 관측 시각에 속하는 세 DB 조회 결과를 반환합니다. */
    AdminIssuanceInquirySource create(Instant snapshotAt);
}
