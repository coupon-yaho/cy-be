package com.kafkick.api.admin.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

class TopologyValidationExceptionTest {

    @Test
    void exceptionMessageContainsKeysButNotInfrastructureValues() {
        TopologyValidationException exception = new TopologyValidationException(List.of(
            new BatchTopologyPreflight.Violation(
                "mysql.max-connections", "50", "151", "mismatch")));

        assertThat(exception.getMessage())
            .contains("mysql.max-connections")
            .doesNotContain("151")
            .doesNotContain("mismatch");
        assertThat(exception.violations()).singleElement().satisfies(violation ->
            assertThat(violation.actual()).isEqualTo("151"));
    }
}
