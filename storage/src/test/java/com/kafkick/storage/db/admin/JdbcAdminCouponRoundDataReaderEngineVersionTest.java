package com.kafkick.storage.db.admin;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.kafkick.core.observation.EngineVersion;

/** 관리자 재고 조회가 지원하는 회차 엔진의 허용 범위를 검증합니다. */
class JdbcAdminCouponRoundDataReaderEngineVersionTest {

    /** V1·V2 이외의 엔진이 DB 재고를 사용하는 경로로 들어오지 못하게 막는지 검증합니다. */
    @ParameterizedTest
    @EnumSource(value = EngineVersion.class, mode = EnumSource.Mode.EXCLUDE, names = {"V1", "V2"})
    @DisplayName("관리자 재고가 지원하지 않는 엔진은 거부한다")
    void rejectsUnsupportedEngineVersion(EngineVersion unsupportedVersion) {
        assertThatThrownBy(() -> JdbcAdminCouponRoundDataReader.parseEngineVersion(unsupportedVersion.name()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("지원하지 않는 발급 엔진");
    }
}
