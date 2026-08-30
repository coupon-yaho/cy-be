package com.kafkick.core.notification;

import java.util.List;
import java.util.Objects;

import com.kafkick.core.notification.domain.NotificationFailure;

public record NotificationFailurePage(List<NotificationFailure> items,
        Long nextBeforeId, boolean hasOlder) {
    /**
     * @throws IllegalArgumentException {@code hasOlder}와 {@code nextBeforeId} 존재 여부가
     *         서로 다른 경우
     */
    public NotificationFailurePage {
        items = List.copyOf(Objects.requireNonNull(items, "items"));
        if (hasOlder != (nextBeforeId != null)) {
            throw new IllegalArgumentException("다음 페이지 여부와 cursor 위치가 일치해야 합니다.");
        }
    }
}
