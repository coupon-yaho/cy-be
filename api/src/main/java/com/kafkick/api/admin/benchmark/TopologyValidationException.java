package com.kafkick.api.admin.benchmark;

import java.util.List;

import com.kafkick.api.admin.benchmark.BatchTopologyPreflight.Violation;
import com.kafkick.core.benchmark.BenchmarkErrorCode;
import com.kafkick.core.support.exception.BusinessException;

/** HTTP 응답까지 수정 가능한 토폴로지 위반값을 보존한다. */
public final class TopologyValidationException extends BusinessException {

    private final List<Violation> violations;

    public TopologyValidationException(List<Violation> violations) {
        super(BenchmarkErrorCode.INVALID_RUN_CONDITION,
            "L2 measurement topology validation failed: keys="
                + violations.stream().map(Violation::key).toList());
        this.violations = List.copyOf(violations);
    }

    public List<Violation> violations() {
        return violations;
    }
}
