package com.kafkick.core.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

/** 관리자 API 전용 enum의 외부 계약 이름을 고정합니다. */
class AdminContractEnumTest {

    /** 수명주기·카탈로그·조회 구간 enum의 외부 계약 이름을 일괄 검증합니다. */
    @Test
    void adminOwnedEnumsKeepTheirWireNames() {
        assertThat(names(QueueState.values())).containsExactly("IDLE", "QUEUEING", "DRAINING");
        assertThat(Arrays.stream(MetricsWindow.values()).map(MetricsWindow::wireValue).toList())
                .containsExactly("1m", "5m", "15m");
        assertThat(names(BenchmarkRunState.values())).containsExactly(
                "CREATED", "RUNNING", "STOPPING", "STOPPED", "FINALIZING", "FINALIZED", "FAILED");
        assertThat(names(BrandCategory.values())).containsExactly("CAFE", "SHOP", "DELIVERY", "CULTURE");
        assertThat(names(CouponPolicyType.values())).containsExactly(
                "PERCENT_CAPPED", "FIXED_AMOUNT", "DATA_GRANT");
        assertThat(names(MeasurementState.values())).containsExactly("RUNNING", "STOPPED");
        assertThat(names(VerificationRunState.values())).containsExactly(
                "REQUESTED", "RUNNING", "COMPLETED", "FAILED");
    }

    private List<String> names(Enum<?>[] values) {
        return Arrays.stream(values).map(Enum::name).toList();
    }
}
