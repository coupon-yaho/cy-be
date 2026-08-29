package com.kafkick.core.batch;

import java.util.List;

/**
 * 배치 실행 이력의 조회 계약.
 *
 * <p>batch 모듈은 storage 를 runtimeOnly 로만 보므로 배치 메타 SQL 을 직접 쥘 수 없다.
 * 계약은 여기 두고 어댑터는 storage 에 둔다.
 */
public interface BatchRunRepository {

    /**
     * 최근 실행부터 페이지 하나. {@code jobName} 이 null 이면 전체를 준다.
     *
     * <p>정렬은 실행 id 내림차순이다. 시작 시각은 실행기가 거절하면 비어 있어(NULL)
     * 정렬 키로 못 쓴다.
     */
    List<BatchRun> findRecent(String jobName, int limit, int offset);

    /** 같은 조건의 전체 건수. 화면이 마지막 페이지를 알아야 한다. */
    int countRecent(String jobName);
}
