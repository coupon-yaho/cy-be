package com.kafkick.core.consistency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.EngineVersion;

/** 발급 엔진 버전별 FINAL 정합성 gap 적용 범위를 검증합니다. */
class ConsistencyGapTypeTest {

    /** V1은 Redis 기반 gap을 제외하고 DB 내부 카운터 대조만 FINAL 판정에 사용합니다. */
    @Test
    @DisplayName("V1 FINAL은 DB_COUNTER_GAP만 적용한다")
    void appliesOnlyDatabaseCounterGapForV1() {
        assertThat(applicableGaps(EngineVersion.V1))
                .containsExactly(ConsistencyGapType.DB_COUNTER_GAP);
    }

    /** Redis 발급 경로를 쓰는 V2와 V3은 확정된 네 gap 모두를 FINAL 판정에 사용합니다. */
    @Test
    @DisplayName("V2와 V3 FINAL은 네 gap 모두 적용한다")
    void appliesAllGapsForV2AndV3() {
        assertThat(applicableGaps(EngineVersion.V2)).containsExactly(ConsistencyGapType.values());
        assertThat(applicableGaps(EngineVersion.V3)).containsExactly(ConsistencyGapType.values());
    }

    /** null 엔진 버전을 임의의 적용 정책으로 해석하면 안 됩니다. */
    @Test
    @DisplayName("null 엔진 버전의 gap 적용 여부는 거부한다")
    void rejectsNullEngineVersion() {
        assertThatNullPointerException().isThrownBy(() ->
                ConsistencyGapType.ACTIVE_DB_GAP.isApplicable(null));
    }

    /** enum 선언 순서대로 실제 FINAL 적용 gap 목록을 만듭니다. */
    private static List<ConsistencyGapType> applicableGaps(EngineVersion engineVersion) {
        return Arrays.stream(ConsistencyGapType.values())
                .filter(gapType -> gapType.isApplicable(engineVersion))
                .toList();
    }
}
