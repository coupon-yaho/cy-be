package com.kafkick.storage.db.admin.inquiry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryErrorCode;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryReadResult;
import com.kafkick.core.support.exception.BusinessException;

/** 개인정보 테이블 존재 확인이 관측 계정으로 나가지 않는지 검증합니다. */
class JdbcAdminIssuanceInquiryPoolRoutingTest {

    @Test
    void readsMemberExistenceOnlyFromMainPool() {
        NamedParameterJdbcTemplate main = mock(NamedParameterJdbcTemplate.class);
        NamedParameterJdbcTemplate observation = mock(NamedParameterJdbcTemplate.class);
        when(main.queryForObject(
                eq(AdminIssuanceInquirySql.MEMBER_EXISTS),
                any(MapSqlParameterSource.class),
                eq(Boolean.class)))
                .thenReturn(false);
        JdbcAdminIssuanceInquirySourceReader reader =
                new JdbcAdminIssuanceInquirySourceReader(main, observation);

        AdminIssuanceInquiryReadResult result = reader.read(
                new AdminIssuanceInquiryQuery(999L, null, null, null, null, 50),
                Instant.parse("2026-08-25T00:00:00Z"));

        assertThat(result.availability())
                .isEqualTo(AdminIssuanceInquiryReadResult.Availability.MEMBER_NOT_FOUND);
        verify(main).queryForObject(
                eq(AdminIssuanceInquirySql.MEMBER_EXISTS),
                any(MapSqlParameterSource.class),
                eq(Boolean.class));
        verifyNoInteractions(observation);
    }

    /** 원본 DB 예외를 보관하지 않으면서 로그에서 실패 종류를 구분할 식별자는 남깁니다. */
    @Test
    void identifiesDataAccessFailureWithoutKeepingTheOriginalCause() {
        NamedParameterJdbcTemplate main = mock(NamedParameterJdbcTemplate.class);
        NamedParameterJdbcTemplate observation = mock(NamedParameterJdbcTemplate.class);
        when(main.queryForObject(
                eq(AdminIssuanceInquirySql.MEMBER_EXISTS),
                any(MapSqlParameterSource.class),
                eq(Boolean.class)))
                .thenThrow(new DataRetrievalFailureException("sensitive SQL detail"));
        JdbcAdminIssuanceInquirySourceReader reader =
                new JdbcAdminIssuanceInquirySourceReader(main, observation);

        assertThatThrownBy(() -> reader.read(
                new AdminIssuanceInquiryQuery(999L, null, null, null, null, 50),
                Instant.parse("2026-08-25T00:00:00Z")))
                .isInstanceOfSatisfying(BusinessException.class, exception -> {
                    assertThat(exception.getErrorCode())
                            .isEqualTo(AdminIssuanceInquiryErrorCode.SOURCE_UNAVAILABLE);
                    assertThat(exception.getMessage())
                            .isEqualTo("DataRetrievalFailureException");
                    assertThat(exception.getCause()).isNull();
                });
    }
}
