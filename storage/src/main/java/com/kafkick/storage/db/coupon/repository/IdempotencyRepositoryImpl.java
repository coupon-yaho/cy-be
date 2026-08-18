// 멱등키 선점과 최초 응답 저장을 현재 상태 변경 트랜잭션에서 처리합니다.
package com.kafkick.storage.db.coupon.repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.exception.IdempotencyPersistenceException;
import com.kafkick.core.coupon.port.IdempotencyRepository;

@Repository
public class IdempotencyRepositoryImpl implements IdempotencyRepository {

    private final JdbcTemplate jdbcTemplate;

    public IdempotencyRepositoryImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
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
    @Transactional(
            propagation = Propagation.MANDATORY,
            readOnly = true
    )
    public Optional<IdempotencyRecord> findByKey(String key) {
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
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void complete(
            String key,
            Long memberId,
            Long issuanceId,
            String responseBody
    ) {
        try {
            int changed = jdbcTemplate.update(
                    """
                    UPDATE idempotency_records
                    SET member_id = ?, issuance_id = ?,
                        status = 'DONE', response_body = ?
                    WHERE idem_key = ? AND status = 'IN_PROGRESS'
                    """,
                    memberId,
                    issuanceId,
                    responseBody,
                    key
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
}
