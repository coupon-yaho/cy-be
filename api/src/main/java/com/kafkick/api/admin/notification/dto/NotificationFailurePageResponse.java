package com.kafkick.api.admin.notification.dto;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.core.notification.NotificationFailurePage;
import com.kafkick.api.admin.notification.NotificationFailureCursorCodec;

/** 실패 알림의 과거 방향 cursor 목록입니다. */
public record NotificationFailurePageResponse(List<NotificationFailureItem> items,
                                              String nextBeforeCursor, boolean hasOlder) {
    public static NotificationFailurePageResponse from(NotificationFailurePage page,
            NotificationFailureCursorCodec cursorCodec) {
        List<NotificationFailureItem> items = page.items().stream()
                .map(item -> new NotificationFailureItem(item.notificationId(), item.couponId(),
                        item.memberId(), item.reason(), item.attemptCount(), item.failedAt()))
                .toList();
        String next = page.nextBeforeId() == null
                ? null : cursorCodec.encode(page.nextBeforeId());
        return new NotificationFailurePageResponse(items, next, page.hasOlder());
    }

    /** 개인정보 원문과 메시지 본문을 제외한 재발송 대상 식별 정보입니다. */
    public record NotificationFailureItem(Long notificationId, Long couponId, Long memberId,
                                          NotifyFailureReason reasonCode, int attemptCount, Instant failedAt) { }
}
