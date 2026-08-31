package com.kafkick.core.observation;

import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.member.Grade;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ObservationContractTest {

    @Test
    void enumNamesRemainStable() {
        assertThat(names(Severity.values())).containsExactly("NONE", "WARN", "CRITICAL");
        assertThat(names(Dependency.values())).containsExactly("NONE", "REDIS", "MYSQL", "KAFKA");
        assertThat(names(Grade.values())).containsExactly("WELCOME", "SILVER", "GOLD", "VIP");
        assertThat(names(EventType.values()))
                .containsExactly("ENTRY_RESULT", "QUEUE_ADMITTED", "ISSUE_ATTEMPT", "ISSUE_RESULT");
        assertThat(names(SourceStatus.values())).containsExactly(
                "VALID", "PENDING", "WARMING_UP", "STALE", "NO_TRAFFIC", "UNAVAILABLE", "N_A"
        );
        assertThat(names(EngineVersion.values())).containsExactly("V1", "V2", "V3");
        assertThat(names(ReleaseStage.values())).containsExactly("V1", "V2_1", "V2_2", "V3");
        assertThat(names(QueueMode.values())).containsExactly("OFF", "ALWAYS", "ADAPTIVE");
        assertThat(names(ReasonCode.values())).containsExactly(
                "NOT_OPENED", "COUPON_ROUND_CLOSED", "GRADE_NOT_ELIGIBLE", "QUEUE_REQUIRED",
                "NO_ENTRY_TOKEN", "ENTRY_TOKEN_EXPIRED", "ALREADY_ISSUED", "STOCK_EXHAUSTED",
                "INVALID_TRANSITION", "TEMPORARILY_UNAVAILABLE", "INTERNAL_ERROR",
                "REPLAY_IN_PROGRESS", "VALUE_CORRUPT", "GATE_NOT_READY",
                "BAD_ARGUMENT", "COUNTER_UNREADABLE", "UNMAPPED"
        );
        assertThat(names(ConsistencyGapType.values())).containsExactly(
                "ACTIVE_DB_GAP", "LUA_GAP", "PERSIST_GAP", "DB_COUNTER_GAP"
        );
        assertThat(names(Verdict.values())).containsExactly("PASS", "FAIL");
        assertThat(names(ConsistencyPhase.values())).containsExactly("LIVE", "FINAL");
    }

    @Test
    void gradeBitValuesRemainStable() {
        assertThat(Arrays.stream(Grade.values()).mapToInt(Grade::getBitValue))
                .containsExactly(1, 2, 4, 8);
    }

    private static List<String> names(Enum<?>[] values) {
        return Arrays.stream(values).map(Enum::name).toList();
    }
}
