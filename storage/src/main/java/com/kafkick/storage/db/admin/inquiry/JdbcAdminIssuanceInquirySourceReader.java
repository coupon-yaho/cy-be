package com.kafkick.storage.db.admin.inquiry;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryErrorCode;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryReadResult;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawAttempt;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawHistoryLink;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawIssuance;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySourceReader;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.support.exception.BusinessException;

/** 관측 전용 JDBC 풀에서 회원 발급 문의 후보를 제한 조회합니다. */
@Component
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class JdbcAdminIssuanceInquirySourceReader implements AdminIssuanceInquirySourceReader {

    private final NamedParameterJdbcTemplate mainJdbc;
    private final NamedParameterJdbcTemplate observationJdbc;

    /** 회원 존재만 운영 풀에서 확인하고 나머지 관측 행은 {@code obs} 풀에서 읽습니다. */
    public JdbcAdminIssuanceInquirySourceReader(
            @Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcTemplate mainJdbc,
            @Qualifier("obs") NamedParameterJdbcTemplate observationJdbc
    ) {
        this.mainJdbc = Objects.requireNonNull(mainJdbc, "mainJdbc");
        this.observationJdbc = Objects.requireNonNull(observationJdbc, "observationJdbc");
    }

    /**
     * 회원 존재는 운영 풀에서 별도로 확인하고, 관측 후보 SELECT만 하나의 관측 read-only
     * 트랜잭션에서 실행합니다.
     *
     * <p>각 원천에 필터, snapshot, cursor와 {@code limit + 1}을 적용하며 SQL·연결·DB enum
     * 매핑 실패는 세부 쿼리나 식별자를 노출하지 않는 MySQL 원천 오류로 변환합니다.
     */
    @Override
    @Transactional(transactionManager = "observationTransactionManager", readOnly = true)
    public AdminIssuanceInquiryReadResult read(
            AdminIssuanceInquiryQuery query,
            Instant snapshotAt
    ) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        try {
            MapSqlParameterSource parameters = parameters(query, snapshotAt);
            Boolean memberExists = mainJdbc.queryForObject(
                    AdminIssuanceInquirySql.MEMBER_EXISTS, parameters, Boolean.class);
            if (!Boolean.TRUE.equals(memberExists)) {
                return AdminIssuanceInquiryReadResult.memberNotFound();
            }
            Boolean couponExists = query.couponId() == null
                    ? Boolean.TRUE
                    : observationJdbc.queryForObject(
                            AdminIssuanceInquirySql.COUPON_EXISTS, parameters, Boolean.class);
            if (!Boolean.TRUE.equals(couponExists)) {
                return AdminIssuanceInquiryReadResult.couponNotFound();
            }

            List<RawAttempt> attempts = observationJdbc.query(
                    AdminIssuanceInquirySql.attempts(query.before()), parameters,
                    JdbcAdminIssuanceInquirySourceReader::mapAttempt);
            Map<Long, RawIssuance> issuances = new LinkedHashMap<>();
            if (query.httpStatus() == null && query.reasonCode() == null) {
                // 연결 로그가 없어도 권위 DB에 존재하는 발급은 독립 후보로 보존한다.
                putIssuances(issuances, observationJdbc.query(
                        AdminIssuanceInquirySql.issuances(query.before()), parameters,
                        JdbcAdminIssuanceInquirySourceReader::mapIssuance));
            }

            Set<Long> issuanceIds = new LinkedHashSet<>();
            Set<String> requestIds = new LinkedHashSet<>();
            for (RawAttempt attempt : attempts) {
                if (attempt.issuanceId() != null) issuanceIds.add(attempt.issuanceId());
                requestIds.add(attempt.requestId());
            }
            if (!issuanceIds.isEmpty()) {
                parameters.addValue("issuanceIds", issuanceIds);
                putIssuances(issuances, observationJdbc.query(
                        AdminIssuanceInquirySql.DIRECT_ISSUANCES, parameters,
                        JdbcAdminIssuanceInquirySourceReader::mapIssuance));
            }

            if (!requestIds.isEmpty()) parameters.addValue("requestIds", requestIds);
            List<HistoryRow> historyRows = requestIds.isEmpty()
                    ? List.of()
                    : observationJdbc.query(
                            AdminIssuanceInquirySql.ISSUE_HISTORIES,
                            parameters,
                            JdbcAdminIssuanceInquirySourceReader::mapHistory);
            // requestId는 보조 연결 키이며 Core가 회원·쿠폰 범위를 다시 검증할 수 있게 발급행도 전달한다.
            for (HistoryRow row : historyRows) issuances.putIfAbsent(
                    row.issuance().issuanceId(), row.issuance());
            List<RawHistoryLink> histories = historyRows.stream().map(HistoryRow::history).toList();

            return AdminIssuanceInquiryReadResult.available(new AdminIssuanceInquirySource(
                    attempts, List.copyOf(issuances.values()), histories));
        } catch (DataAccessException | IllegalArgumentException exception) {
            // 원본 예외는 SQL·식별자를 포함할 수 있어 버리고, 로그에서 분류할 타입 이름만 남긴다.
            throw new BusinessException(
                    AdminIssuanceInquiryErrorCode.SOURCE_UNAVAILABLE,
                    exception.getClass().getSimpleName());
        }
    }

    private static MapSqlParameterSource parameters(
            AdminIssuanceInquiryQuery query,
            Instant snapshotAt
    ) {
        MapSqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("memberId", query.memberId())
                .addValue("couponId", query.couponId())
                .addValue("httpStatus", query.httpStatus())
                .addValue("reasonCode", query.reasonCode() == null ? null : query.reasonCode().name())
                .addValue("snapshotAt", Timestamp.from(snapshotAt))
                .addValue("fetchLimit", query.limit() + 1);
        if (query.before() != null) {
            parameters.addValue("beforeOccurredAt", Timestamp.from(query.before().occurredAt()))
                    .addValue("beforeSourceId", query.before().sourceId());
        }
        return parameters;
    }

    private static RawAttempt mapAttempt(ResultSet rs, int rowNum) throws SQLException {
        return new RawAttempt(
                rs.getLong("id"),
                EventType.valueOf(rs.getString("event_type")),
                rs.getString("request_id"),
                rs.getLong("member_id"),
                rs.getLong("coupon_id"),
                nullableLong(rs, "issuance_id"),
                nullableInteger(rs, "http_status"),
                nullableReason(rs.getString("reason_code")),
                rs.getTimestamp("occurred_at").toInstant());
    }

    private static RawIssuance mapIssuance(ResultSet rs, int rowNum) throws SQLException {
        return new RawIssuance(
                rs.getLong("id"),
                rs.getLong("member_id"),
                rs.getLong("coupon_id"),
                IssuanceStatus.valueOf(rs.getString("status")),
                rs.getTimestamp("issued_at").toInstant());
    }

    private static HistoryRow mapHistory(ResultSet rs, int rowNum) throws SQLException {
        RawIssuance issuance = new RawIssuance(
                rs.getLong("issuance_id"),
                rs.getLong("member_id"),
                rs.getLong("coupon_id"),
                IssuanceStatus.valueOf(rs.getString("status")),
                rs.getTimestamp("issued_at").toInstant());
        RawHistoryLink history = new RawHistoryLink(
                rs.getLong("id"),
                rs.getLong("issuance_id"),
                IssuanceEventType.valueOf(rs.getString("event_type")),
                rs.getString("request_id"),
                rs.getTimestamp("created_at").toInstant());
        return new HistoryRow(issuance, history);
    }

    private static Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private static Integer nullableInteger(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static ReasonCode nullableReason(String value) {
        return value == null ? null : ReasonCode.valueOf(value);
    }

    private static void putIssuances(Map<Long, RawIssuance> target, List<RawIssuance> rows) {
        for (RawIssuance row : rows) target.putIfAbsent(row.issuanceId(), row);
    }

    private record HistoryRow(RawIssuance issuance, RawHistoryLink history) {
    }
}
