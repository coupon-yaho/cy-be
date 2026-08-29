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
     *
     * <p><b>{@code anchor} 가 페이지 경계를 얼린다.</b> {@code null} 이면 지금 시점 전체를 본다.
     * 값이 오면 {@code JOB_EXECUTION_ID <= anchor} 로 좁혀, 요청 사이에 새 실행이 생겨도
     * 뒤 페이지가 밀리지 않는다 — {@code OFFSET} 만으로는 <b>같은 행이 다시 나오고 뒤쪽
     * 행이 빠진다.</b>
     */
    List<BatchRun> findRecent(String jobName, int limit, int offset, Long anchor);

    /**
     * 같은 조건의 전체 건수. 화면이 마지막 페이지를 알아야 한다.
     *
     * <p>{@link #findRecent} 와 <b>같은 anchor</b> 로 세야 total 이 그 페이지들의 것이 된다.
     */
    int countRecent(String jobName, Long anchor);

    /**
     * 같은 조건에서 <b>가장 큰 실행 id</b>. {@code anchor} 를 안 받은 첫 요청이 경계를 잡는 데 쓴다.
     *
     * <p><b>페이지의 첫 행으로 대신하면 안 된다.</b> 첫 요청이 {@code offset > 0} 이면 그 행은
     * 전체의 최댓값이 아니라 <b>그 페이지의 첫 행</b>이라, 경계가 낮게 잡혀 {@code total} 이
     * 줄고 다음 요청이 행을 건너뛴다(봇 리뷰가 짚었다). 대상이 없으면 {@code null} 이다.
     */
    Long latestExecutionId(String jobName);
}
