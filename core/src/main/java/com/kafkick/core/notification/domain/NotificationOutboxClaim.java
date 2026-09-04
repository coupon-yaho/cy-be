package com.kafkick.core.notification.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * 선점한 발행 명령 한 건.
 *
 * <p><b>생성 거부는 두 예외로 갈린다.</b> 식별자·{@code failureCount} 범위·
 * {@code AUTO} 트리거·빈 {@code claimToken} 은 {@link IllegalArgumentException} 이고,
 * {@code trigger}·{@code requestedAt} 이 {@code null} 인 것은
 * {@link java.util.Objects#requireNonNull} 이 던지는 {@link NullPointerException} 이다.
 * 어느 쪽이든 선점한 행을 표현할 수 없다는 뜻이라 그 자리에서 멈춘다.
 *
 * <p><b>{@code failureCount} 는 이번 실패를 세기 전 값이다.</b> 백오프를 계산하는 쪽은
 * {@code failureCount + 1} 을 몇 번째 재시도로 본다 — 그대로 쓰면 첫 실패의 지연 상한이
 * 한 칸 작아진다. 이 값을 claim 에 실은 이유는 지연 정책이 <b>저장소가 아니라 릴레이</b>에
 * 있어야 하기 때문이다. 어댑터가 정하면 DB 없이는 그 정책을 검증할 수 없다.
 */
public record NotificationOutboxClaim(Long outboxId, Long notificationId, int attemptSeq,
        AttemptTrigger trigger, String claimToken, Instant requestedAt, int failureCount) {
    public NotificationOutboxClaim {
        if (outboxId == null || outboxId <= 0 || notificationId == null || notificationId <= 0
                || attemptSeq < 1) {
            throw new IllegalArgumentException("outbox claim 식별자는 양수여야 합니다.");
        }
        // 스키마의 ck_notification_outbox_failure_count 와 같은 범위다. 둘이 갈리면
        // DB 가 받아들인 행을 도메인이 거부해 선점이 통째로 막힌다.
        if (failureCount < 0 || failureCount > 10) {
            throw new IllegalArgumentException(
                    "failureCount 는 0..10 이어야 합니다. 받은 값=" + failureCount);
        }
        Objects.requireNonNull(trigger, "trigger");
        if (trigger == AttemptTrigger.AUTO) {
            throw new IllegalArgumentException("outbox claim trigger는 INITIAL 또는 MANUAL이어야 합니다.");
        }
        if (claimToken == null || claimToken.isBlank()) {
            throw new IllegalArgumentException("claimToken은 비어 있을 수 없습니다.");
        }
        Objects.requireNonNull(requestedAt, "requestedAt");
    }
}
