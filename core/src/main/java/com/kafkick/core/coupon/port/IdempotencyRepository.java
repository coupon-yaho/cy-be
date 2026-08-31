package com.kafkick.core.coupon.port;

import java.time.Instant;
import java.util.Optional;

import com.kafkick.core.coupon.domain.IdempotencyRecord;

public interface IdempotencyRepository {

    boolean tryStart(
            String key,
            String requestHash,
            Instant createdAt
    );

    Optional<IdempotencyRecord> findByKey(String key);

    boolean tryReclaim(
            String key,
            String requestHash,
            Instant previousClaimedAt,
            Instant reclaimedAt
    );

    /**
     * 완성된 DONE 레코드를 한 번의 INSERT로 기록합니다.
     *
     * <p>발급 경로 전용입니다. 발급은 {@code uk_coupon_member}가 멱등 선점과 같은 배제를 이미
     * 제공하므로 IN_PROGRESS 선점을 먼저 커밋할 이유가 없습니다. 사용·취소는 자연 유일 제약이
     * 없어 {@link #tryStart}부터 시작하는 2단계 쓰기를 그대로 씁니다.
     *
     * @return 같은 멱등키가 이미 있으면 {@code false}
     */
    boolean insertCompleted(
            String key,
            Long memberId,
            Long issuanceId,
            String requestHash,
            String responseBody,
            Instant createdAt
    );

    void complete(
            String key,
            Long memberId,
            Long issuanceId,
            String responseBody,
            Instant claimedAt
    );

    void release(
            String key,
            String requestHash,
            Instant claimedAt
    );
}
