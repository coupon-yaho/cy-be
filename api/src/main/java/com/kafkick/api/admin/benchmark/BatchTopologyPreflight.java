package com.kafkick.api.admin.benchmark;

import java.util.List;

/** batch 프로세스가 자기 소유 측정 조건을 검사한 결과를 얻는 포트. */
public interface BatchTopologyPreflight {

    Result validate(long couponId);

    record Result(boolean valid, List<Violation> violations) {
        public Result {
            if (violations == null) {
                valid = false;
                violations = List.of(new Violation(
                    "batch.preflight.response", "violations array", "null",
                    "batch preflight 응답 형식이 올바르지 않다"));
            } else {
                violations = List.copyOf(violations);
            }
        }
    }

    record Violation(String key, String expected, String actual, String reason) {
    }
}
