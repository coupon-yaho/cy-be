package com.kafkick.core.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

/** 관리자 HTTP 계약에서 사용하는 확정 enum의 상수 집합과 직렬화 이름을 고정합니다. */
class AdminContractEnumTest {

    /** SourceStatus가 합의된 일곱 원천 상태를 순서와 이름까지 정확히 유지하는지 검증합니다. */
    @Test
    void sourceStatusKeepsAllSevenOperationalStates() {
        assertThat(names(SourceStatus.values())).containsExactly(
                "VALID", "PENDING", "WARMING_UP", "STALE", "NO_TRAFFIC", "UNAVAILABLE", "N_A");
    }

    /** 엔진·릴리스·대기열·심각도 등 확정 enum의 외부 계약 이름을 일괄 검증합니다. */
    @Test
    void fixedAdminEnumsKeepTheirWireNames() {
        assertThat(names(EngineVersion.values())).containsExactly("V1", "V2", "V3");
        assertThat(names(ReleaseStage.values())).containsExactly("V1", "V2_1", "V2_2", "V3");
        assertThat(names(QueueMode.values())).containsExactly("OFF", "ALWAYS", "ADAPTIVE");
        assertThat(names(QueueState.values())).containsExactly("IDLE", "QUEUEING", "DRAINING");
        assertThat(names(Severity.values())).containsExactly("NONE", "WARN", "CRITICAL");
        assertThat(names(EventType.values())).containsExactly("ENTRY_RESULT", "QUEUE_ADMITTED", "ISSUE_RESULT");
        assertThat(Arrays.stream(MetricsWindow.values()).map(MetricsWindow::wireValue).toList())
                .containsExactly("1m", "5m", "15m");
        assertThat(names(ConsistencyPhase.values())).containsExactly("LIVE", "FINAL");
        assertThat(names(BenchmarkRunState.values())).containsExactly(
                "CREATED", "RUNNING", "STOPPING", "STOPPED", "FINALIZING", "FINALIZED", "FAILED");
        assertThat(names(BrandCategory.values())).containsExactly("CAFE", "SHOP", "DELIVERY", "CULTURE");
        assertThat(names(CouponPolicyType.values())).containsExactly(
                "PERCENT_CAPPED", "FIXED_AMOUNT", "DATA_GRANT");
    }

    /** 운영 사유 코드가 제한된 어휘 집합이며 알 수 없는 값을 위한 UNMAPPED를 포함하는지 검증합니다. */
    @Test
    void reasonCodeHasBoundedOperationalVocabulary() {
        assertThat(ReasonCode.values()).hasSize(11);
        assertThat(ReasonCode.valueOf("UNMAPPED")).isNotNull();
    }

    private List<String> names(Enum<?>[] values) {
        return Arrays.stream(values).map(Enum::name).toList();
    }
}
