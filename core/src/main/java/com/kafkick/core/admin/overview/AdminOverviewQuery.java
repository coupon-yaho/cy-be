package com.kafkick.core.admin.overview;

import java.time.Instant;
import java.util.Set;

/**
 * 운영 현황 Provider에 전달할 기술 중립 조회 조건입니다.
 *
 * <p>최신 HTML에서 확정한 {@code from}, {@code to}, {@code couponIds}만 포함합니다. Controller 요청
 * 바인딩용 객체가 아니므로 Spring MVC나 Validation annotation에 결합하지 않습니다. 기간 역전,
 * null 기간, null 또는 빈 쿠폰 집합의 검색 의미는 문서에서 확정되지 않았으므로 이 record가 임의로
 * 거부하거나 기본값으로 정규화하지 않습니다. 실제 조회 정책은 별도 검증 경계에서
 * 처리해야 합니다.</p>
 *
 * @param from 조회 시작 시각; 미지정 조회의 의미가 확정되기 전까지 null 허용
 * @param to 조회 종료 시각; 미지정 조회의 의미가 확정되기 전까지 null 허용
 * @param couponIds 조회할 쿠폰 식별자 집합; 식별자는 기존 계약대로 couponId를 사용하며 null·빈 집합의
 *                  의미는 후속 계약에서 확정
 */
public record AdminOverviewQuery(Instant from, Instant to, Set<Long> couponIds) {

    /**
     * 외부의 변경 가능한 쿠폰 식별자 집합과 조회 조건의 생명주기를 분리합니다.
     *
     * <p>{@code null}은 필터 미지정 상태로 유지하고, 값이 있으면 불변 집합으로 복사합니다.</p>
     */
    public AdminOverviewQuery {
        couponIds = couponIds == null ? null : Set.copyOf(couponIds);
    }
}
