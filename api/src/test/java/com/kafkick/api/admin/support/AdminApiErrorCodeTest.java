package com.kafkick.api.admin.support;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 미연결 관리자 API가 사용하는 오류 상태·코드·메시지 계약을 검증합니다. */
class AdminApiErrorCodeTest {

    /** 선구축 오류가 HTTP 501과 ADMIN-001로 안정적으로 노출되는지 확인합니다. */
    @Test
    @DisplayName("관리자 API 선구축 오류는 501 ADMIN-001과 고정 메시지를 제공한다")
    void notImplementedProvidesApiContract() {
        assertThat(AdminApiErrorCode.NOT_IMPLEMENTED.getStatus()).isEqualTo(501);
        assertThat(AdminApiErrorCode.NOT_IMPLEMENTED.getCode()).isEqualTo("ADMIN-001");
        assertThat(AdminApiErrorCode.NOT_IMPLEMENTED.getMessage())
                .isEqualTo("관리자 API 구현이 아직 연결되지 않았습니다.");
    }
}
