package com.kafkick.api.observation.resource;

/**
 * OBS-9가 위험 임계 대비 현재 사용률로 정렬하는 자원 행입니다.
 *
 * <p>DB 커넥션 대기 수는 별도 행이 아닙니다. {@link ResourceSnapshot.DbPool}에 DB 풀 행의
 * 동반 값으로 실립니다. 네트워크는 이 환경에서 측정하지 않지만, 측정하지 않기로 한 결정을
 * 보존하기 위해 자리를 유지합니다.
 */
public enum ResourceMetric {

    DB_POOL,
    PROCESS_CPU,
    HEAP_MEMORY,
    TOMCAT_THREADS,
    DISK,
    NETWORK
}
