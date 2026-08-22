package com.kafkick.api.admin.support;

import java.util.List;

import com.kafkick.api.admin.observability.dto.AdminEventItem;

/**
 * 최근 운영 이벤트를 마지막 소비 위치 이후 방향으로 polling하는 응답 초안입니다.
 *
 * <p>{@code nextAfterCursor}는 반환된 마지막 이벤트 다음 조회에 사용할 cursor이며, {@code hasMore}는 같은
 * 조회 시점에 아직 반환하지 못한 이벤트가 있음을 뜻합니다. 만료 cursor를 최신 유효 위치로 복구한 경우
 * {@code cursorReset=true}로 표시하고, 그 과정에서 일부 이벤트를 놓칠 수 있으면
 * {@code eventsMayBeMissing=true}를 함께 반환합니다.</p>
 *
 * @param items 저장소 수집 순서로 정렬된 이벤트 목록
 * @param nextAfterCursor 다음 polling에서 마지막 소비 위치로 전달할 cursor
 * @param hasMore 같은 조회 시점에 아직 반환하지 못한 이벤트 존재 여부
 * @param cursorReset 요청 cursor의 만료로 자동 복구했는지 여부
 * @param eventsMayBeMissing cursor 복구 과정에서 이벤트 유실 가능성이 있는지 여부
 */
public record LiveEventPollResponse(List<AdminEventItem> items, String nextAfterCursor, boolean hasMore,
                                    boolean cursorReset, boolean eventsMayBeMissing) { }
