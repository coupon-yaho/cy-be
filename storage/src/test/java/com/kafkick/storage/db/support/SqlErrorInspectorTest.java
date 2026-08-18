// 중첩 SQLException의 오류 코드와 메시지 키워드 검사 규칙을 검증합니다.
package com.kafkick.storage.db.support;

import java.sql.SQLException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SqlErrorInspectorTest {

    @Test
    @DisplayName("중첩 원인에서 오류 코드와 제약 이름을 찾는다")
    void findErrorCodeAndKeywordFromNestedCause() {
        SQLException sqlException = new SQLException(
                "Duplicate entry for key 'uk_coupon_member'",
                "23000",
                1062
        );
        RuntimeException wrapper = new RuntimeException(
                "저장 실패",
                sqlException
        );

        assertThat(SqlErrorInspector.hasErrorCode(wrapper, 1062))
                .isTrue();
        assertThat(SqlErrorInspector.hasErrorCode(
                wrapper,
                1062,
                "uk_coupon_member"
        )).isTrue();
    }

    @Test
    @DisplayName("오류 코드나 메시지 키워드가 다르면 일치하지 않는다")
    void rejectDifferentErrorCodeOrKeyword() {
        SQLException sqlException = new SQLException(
                "Duplicate entry for key 'uk_template_open'",
                "23000",
                1062
        );

        assertThat(SqlErrorInspector.hasErrorCode(sqlException, 1452))
                .isFalse();
        assertThat(SqlErrorInspector.hasErrorCode(
                sqlException,
                1062,
                "uk_coupon_member"
        )).isFalse();
    }
}
