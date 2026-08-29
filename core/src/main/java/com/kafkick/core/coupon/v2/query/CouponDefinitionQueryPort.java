package com.kafkick.core.coupon.v2.query;

import java.util.List;

/**
 * V2 조회가 L1/L2에 보관할 수 있는 불변 회차 정의의 읽기 포트다.
 *
 * <p><b>인자를 받지 않는 것이 계약이다.</b> 결과가 요청 시각에 종속되면, 회원 축 없는 단일
 * 캐시 키에 담기는 순간 누가 먼저 채웠는지가 답을 바꾼다. 시각 판정은 캐시 밖에서 한다.
 */
public interface CouponDefinitionQueryPort {

    List<CouponDefinition> findCandidates();
}
