// 걷어낸 만료 실행의 결과 상태입니다.
package com.kafkick.batch.api;

/**
 * <b>{@code FAILED} 로 닫혔다는 것이 이 응답의 전부다.</b>
 *
 * <p>{@code ABANDONED} 가 아니다 — 그 상태는 {@code COMPLETED} 와 같은 취급이라
 * ({@code TaskExecutorJobLauncher} 가 둘을 같이 막는다) <b>그 {@code JobInstance} 를 같은
 * 파라미터로 영원히 못 돌린다.</b> 만료는 {@code asOf} 가 식별 파라미터라 그 크론 슬롯이
 * 통째로 사라진다. {@code FAILED} 는 그 문을 안 닫는다.
 */
public record Recovered(long executionId, String status, boolean alreadyRecovered) {
}
