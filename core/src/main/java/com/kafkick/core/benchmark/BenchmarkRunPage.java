package com.kafkick.core.benchmark;

import java.util.List;
import java.util.Objects;

/** DB가 제한 조회한 후보에서 조립된 Benchmark 회차의 과거 방향 페이지입니다. */
public record BenchmarkRunPage(
        List<BenchmarkRun> items,
        BenchmarkRunPosition nextBefore,
        boolean hasOlder
) {

    /** 반환 항목의 불변성과 다음 Keyset 위치의 존재 조건을 검증합니다. */
    public BenchmarkRunPage {
        Objects.requireNonNull(items, "items");
        items = List.copyOf(items);
        if (hasOlder != (nextBefore != null)) {
            throw new IllegalArgumentException("hasOlder와 nextBefore는 함께 존재해야 합니다.");
        }
    }
}
