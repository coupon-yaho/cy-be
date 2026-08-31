package com.kafkick.core.admin.couponroundsource;

import java.time.Instant;

/** 관리자 운영현황과 쿠폰 회차 상세가 소비할 DB 쿠폰 회차 조회 경계입니다. */
public interface AdminCouponRoundDataReader {

    /**
     * 하나의 운영현황 스냅샷에 사용할 쿠폰 회차 카탈로그를 조회합니다.
     *
     * <p>이 계약은 FINAL 정합성 상태나 조치 후보를 표현하지 않습니다. A-F6 전까지 조회 구현체는
     * FINAL 후보를 합성하지 않습니다.</p>
     *
     * @param snapshotAt 조회 결과가 속할 운영현황 기준 시각
     * @return 쿠폰 회차 목록과 DB 관측 상태
     */
    AdminCouponRoundCatalog loadCatalog(Instant snapshotAt);

    /**
     * 한 쿠폰 회차의 상세 지표 원천을 지정한 전이 집계 구간으로 조회합니다.
     *
     * @param couponId 조회할 쿠폰 회차 식별자
     * @param fromInclusive 전이 집계 구간의 포함 시작 시각
     * @param toExclusive 전이 집계 구간의 제외 종료 시각
     * @param snapshotAt 조회 결과가 속할 상세 화면 기준 시각
     * @return 찾음·없음·DB 조회 불가를 구분한 상세 결과
     */
    AdminCouponRoundDetailData findDetail(
            long couponId,
            Instant fromInclusive,
            Instant toExclusive,
            Instant snapshotAt
    );
}
