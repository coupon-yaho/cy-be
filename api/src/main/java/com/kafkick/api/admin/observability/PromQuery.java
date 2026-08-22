package com.kafkick.api.admin.observability;

import java.util.List;

/**
 * Prometheus 에 instant query 하나를 던지고 표본을 받아 오는 경계입니다.
 *
 * <p>조립하는 쪽은 HTTP 도, 응답 스키마도 알 필요가 없습니다. 이 경계가 있어야 시험용 대역이
 * {@link PromQueryClient} 를 상속해 {@code super(null)} 로 생성자를 통과시키지 않아도 됩니다 —
 * 그렇게 두면 클라이언트에 인자 검증을 하나 넣는 순간 무관한 테스트가 한꺼번에 깨집니다.</p>
 */
@FunctionalInterface
public interface PromQuery {

    /**
     * instant query 하나를 실행합니다.
     *
     * @param promQl 실행할 PromQL
     * @return 벡터 결과의 표본 목록; 일치하는 시계열이 없으면 빈 목록
     * @throws PromQueryException 호출이 실패했거나 결과를 해석할 수 없는 경우
     */
    List<PromSample> query(String promQl);
}
