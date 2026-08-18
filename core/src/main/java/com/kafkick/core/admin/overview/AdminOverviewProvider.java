package com.kafkick.core.admin.overview;

/**
 * 관리자 운영 현황에 필요한 내부 Snapshot을 조회하는 Adapter 경계입니다.
 *
 * <p>이 경계는 DB, Redis, Kafka, Servlet 또는 HTTP 헤더 타입에 의존하지 않습니다.</p>
 *
 * <p>현재는 인터페이스만 선구축하며 운영 Bean, Mock/Fake 구현체 및 Controller 주입을 만들지 않습니다.
 * 여러 원천을 조립하는 실제 구현체를 연결하면 {@code GET /api/v1/admin/overview}의
 * 성공·부분 응답을 활성화합니다. 따라서 이 인터페이스의 존재만으로 실제 데이터 조회나 API 기능이
 * 완료된 것은 아닙니다.</p>
 */
@FunctionalInterface
public interface AdminOverviewProvider {

    /**
     * 조회 기간과 선택 쿠폰 조건에 해당하는 운영 현황 Snapshot을 반환합니다.
     *
     * <p>반환 Snapshot은 미관측 수치를 0으로 위조하지 않고 값, {@code SourceStatus}, 실제 관측 시각을
     * 분리합니다. 부분 원천 실패 처리와 실제 데이터 조회 방식은 구현체의 책임입니다.</p>
     *
     * @param query HTML에서 확정한 기간 및 쿠폰 필터; null·빈 집합의 검색 의미는 후속 계약에서 확정
     * @return 기술 인프라 타입과 분리된 운영 현황 Snapshot
     */
    AdminOverviewSnapshot getOverview(AdminOverviewQuery query);
}
