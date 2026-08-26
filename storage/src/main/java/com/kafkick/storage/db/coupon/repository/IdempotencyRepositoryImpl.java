package com.kafkick.storage.db.coupon.repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.exception.IdempotencyPersistenceException;
import com.kafkick.core.coupon.port.IdempotencyRepository;

@Repository
public class IdempotencyRepositoryImpl implements IdempotencyRepository {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyRepositoryImpl.class);

    private final JdbcTemplate jdbcTemplate;

    public IdempotencyRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean tryStart(
            String key,
            String requestHash,
            Instant createdAt
    ) {
        try {
            jdbcTemplate.update(
                    """
                    INSERT INTO idempotency_records (
                        idem_key, member_id, issuance_id, request_hash,
                        status, response_body, created_at
                    ) VALUES (?, NULL, NULL, ?, 'IN_PROGRESS', NULL, ?)
                    """,
                    key,
                    requestHash,
                    Timestamp.from(createdAt)
            );
            return true;
        } catch (DuplicateKeyException exception) {
            return false;
        } catch (DataAccessException exception) {
            throw new IdempotencyPersistenceException(
                    "멱등키 선점에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public boolean insertCompleted(
            String key,
            Long memberId,
            Long issuanceId,
            String requestHash,
            String responseBody,
            Instant createdAt
    ) {
        if (memberId == null || memberId <= 0
                || issuanceId == null || issuanceId <= 0
                || responseBody == null || responseBody.isBlank()
                || createdAt == null) {
            throw new IdempotencyPersistenceException(
                    "완료할 멱등 레코드 대상 값이 올바르지 않습니다."
            );
        }
        try {
            jdbcTemplate.update(
                    """
                    INSERT INTO idempotency_records (
                        idem_key, member_id, issuance_id, request_hash,
                        status, response_body, created_at
                    ) VALUES (?, ?, ?, ?, 'DONE', ?, ?)
                    """,
                    key,
                    memberId,
                    issuanceId,
                    requestHash,
                    responseBody,
                    Timestamp.from(createdAt)
            );
            return true;
        } catch (DuplicateKeyException exception) {
            return false;
        } catch (DataAccessException exception) {
            throw new IdempotencyPersistenceException(
                    "멱등 처리 결과 저장에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public Optional<IdempotencyRecord> findByKey(String key) {
        try {
            return jdbcTemplate.query(
                    """
                    SELECT idem_key, member_id, issuance_id, request_hash,
                           status, response_body, created_at
                    FROM idempotency_records
                    WHERE idem_key = ?
                    """,
                    (resultSet, rowNumber) -> new IdempotencyRecord(
                            resultSet.getString("idem_key"),
                            resultSet.getObject("member_id", Long.class),
                            resultSet.getObject("issuance_id", Long.class),
                            resultSet.getString("request_hash"),
                            IdempotencyStatus.valueOf(
                                    resultSet.getString("status")
                            ),
                            resultSet.getString("response_body"),
                            resultSet.getTimestamp("created_at").toInstant()
                    ),
                    key
            ).stream().findFirst();
        } catch (DataAccessException exception) {
            throw new IdempotencyPersistenceException(
                    "멱등 처리 상태 조회에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public boolean tryReclaim(
            String key,
            String requestHash,
            Instant previousClaimedAt,
            Instant reclaimedAt
    ) {
        try {
            return jdbcTemplate.update(
                    """
                    UPDATE idempotency_records
                    SET created_at = ?
                    WHERE idem_key = ?
                      AND request_hash = ?
                      AND status = 'IN_PROGRESS'
                      AND created_at = ?
                    """,
                    Timestamp.from(reclaimedAt),
                    key,
                    requestHash,
                    Timestamp.from(previousClaimedAt)
            ) == 1;
        } catch (DataAccessException exception) {
            throw new IdempotencyPersistenceException(
                    "오래된 멱등 요청 회수에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public void complete(
            String key,
            Long memberId,
            Long issuanceId,
            String responseBody,
            Instant claimedAt
    ) {
        if (memberId == null || memberId <= 0
                || issuanceId == null || issuanceId <= 0
                || responseBody == null || responseBody.isBlank()
                || claimedAt == null) {
            throw new IdempotencyPersistenceException(
                    "완료할 멱등 레코드 대상 값이 올바르지 않습니다."
            );
        }
        try {
            int changed = jdbcTemplate.update(
                    """
                    UPDATE idempotency_records
                    SET member_id = ?, issuance_id = ?,
                        status = 'DONE', response_body = ?
                    WHERE idem_key = ?
                      AND status = 'IN_PROGRESS'
                      AND created_at = ?
                    """,
                    memberId,
                    issuanceId,
                    responseBody,
                    key,
                    Timestamp.from(claimedAt)
            );
            if (changed != 1) {
                throw new IdempotencyPersistenceException(
                        "완료할 멱등 레코드를 찾을 수 없습니다."
                );
            }
        } catch (IdempotencyPersistenceException exception) {
            throw exception;
        } catch (DataAccessException exception) {
            throw new IdempotencyPersistenceException(
                    "멱등 처리 결과 저장에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public void release(
            String key,
            String requestHash,
            Instant claimedAt
    ) {
        try {
            int deleted = jdbcTemplate.update(
                    """
                    DELETE FROM idempotency_records
                    WHERE idem_key = ?
                      AND request_hash = ?
                      AND status = 'IN_PROGRESS'
                      AND created_at = ?
                    """,
                    key,
                    requestHash,
                    Timestamp.from(claimedAt)
            );
            if (deleted == 0) {
                log.debug("멱등 선점 해제 대상이 없습니다. idempotencyKey={}", key);
            }
        } catch (DataAccessException exception) {
            throw new IdempotencyPersistenceException(
                    "실패한 멱등 요청 정리에 실패했습니다.",
                    exception
            );
        }
    }
}
