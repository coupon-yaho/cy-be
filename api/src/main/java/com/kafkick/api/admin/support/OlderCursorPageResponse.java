package com.kafkick.api.admin.support;

import java.util.List;

/**
 * 최신 항목에서 과거 항목 방향으로 탐색하는 관리자 목록의 공통 cursor 응답 초안입니다.
 *
 * <p>{@code items}는 최신에서 과거 순서이고, {@code nextBeforeCursor}는 현재 페이지의 마지막 항목보다
 * 오래된 데이터를 요청할 때 사용합니다. {@code hasOlder=false}이면 cursor도 일반적으로 null이며 더 과거의
 * 페이지가 없습니다. 최근 이벤트 polling의 after-cursor 계약과는 방향과 복구 의미가 다릅니다.</p>
 *
 * @param <T> 목록 항목 타입
 * @param items 최신 항목부터 과거 항목 순서로 정렬된 목록
 * @param nextBeforeCursor 다음 과거 페이지 조회에 사용할 cursor
 * @param hasOlder 더 과거의 항목 존재 여부
 */
public record OlderCursorPageResponse<T>(List<T> items, String nextBeforeCursor, boolean hasOlder) { }
