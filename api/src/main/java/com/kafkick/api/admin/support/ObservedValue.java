package com.kafkick.api.admin.support;

import java.time.Instant;

import com.kafkick.core.admin.SourceStatus;

/**
 * 독립 수집 원천의 값, 수집 상태, 마지막 관측 시각을 함께 전달하는 공통 관리자 응답 값입니다.
 *
 * <p>{@code value}는 미수집·장애·해당 없음 상태에서 null일 수 있으며, 이를 0이나 정상값으로 대체하면 안 됩니다.
 * {@code observedAt} 역시 실제 관측이 없으면 null입니다. 화면은 값만 보지 않고 {@code state}를 함께 사용해
 * VALID, PENDING, STALE, UNAVAILABLE 등의 상태를 구분해야 합니다.</p>
 *
 * @param <T> 관측한 값의 타입
 * @param value 실제 관측값; 미수집·장애·해당 없음이면 null
 * @param state 값을 제공한 원천의 확정 수집 상태
 * @param observedAt 원천에서 마지막으로 관측한 시각; 관측 이력이 없으면 null
 */
public record ObservedValue<T>(T value, SourceStatus state, Instant observedAt) { }
