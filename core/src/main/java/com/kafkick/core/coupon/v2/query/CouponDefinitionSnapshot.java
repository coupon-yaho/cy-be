package com.kafkick.core.coupon.v2.query;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * L2 가 인스턴스 사이에서 주고받는 정의 목록 한 벌이다.
 *
 * <p>{@code nextBoundary} 를 함께 실어야 하는 이유는, 받는 쪽이 그 값으로 자기 L1 의 수명을
 * 정하기 때문이다. 목록만 건네면 회차 경계를 모르는 채로 TTL 상한까지 들고 있게 된다.
 */
public record CouponDefinitionSnapshot(List<CouponDefinition> definitions, Instant nextBoundary) {

    public CouponDefinitionSnapshot {
        definitions = List.copyOf(Objects.requireNonNull(definitions, "definitions"));
        Objects.requireNonNull(nextBoundary, "nextBoundary");
    }
}
