// 리플레이 입력 읽기 어댑터입니다. issuance_histories 를 아는 유일한 곳입니다.
package com.kafkick.storage.verification;

import static com.kafkick.storage.verification.ColumnValues.toEnum;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.verification.replay.IssuanceHistoryRecord;
import com.kafkick.core.verification.replay.IssuanceIdRange;
import com.kafkick.core.verification.replay.ReplayHistoryRepository;

/**
 * 구간을 받아 그 안의 이력만 읽는다. 구간을 나누는 판단은 배치가 하고
 * 여기는 SQL 만 안다.
 */
@Repository
public class ReplayHistoryJdbcAdapter implements ReplayHistoryRepository {

    /**
     * {@code created_at} 에는 인덱스가 없어 이 문장은 전체를 훑는다.
     * 실행마다 한 번뿐이라 감수한다.
     */
    private static final String SELECT_RANGE_BOUNDS = """
            SELECT MIN(issuance_id) AS min_id, MAX(issuance_id) AS max_id
              FROM issuance_histories
             WHERE created_at <= :asOf
            """;

    /**
     * {@code ORDER BY} 에 {@code id} 를 넣는 것이 타이브레이커다.
     * 같은 {@code created_at} 이 여럿이면 이것 없이는 실행마다 순서가 달라져
     * 접은 결과가 흔들린다.
     */
    private static final String SELECT_RANGE = """
            SELECT id, issuance_id, event_type, from_status, to_status, created_at
              FROM issuance_histories
             WHERE issuance_id BETWEEN :fromIssuanceId AND :toIssuanceId
               AND created_at <= :asOf
             ORDER BY issuance_id, created_at, id
            """;

    private static final RowMapper<IssuanceHistoryRecord> ROW_MAPPER =
            (rs, rowNum) -> new IssuanceHistoryRecord(
                    rs.getLong("id"),
                    rs.getLong("issuance_id"),
                    IssuanceEventType.valueOf(rs.getString("event_type")),
                    toEnum(rs.getString("from_status"), IssuanceStatus::valueOf),
                    IssuanceStatus.valueOf(rs.getString("to_status")),
                    rs.getObject("created_at", LocalDateTime.class)
            );

    private final JdbcClient jdbcClient;

    public ReplayHistoryJdbcAdapter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public Optional<IssuanceIdRange> issuanceIdRange(LocalDateTime asOf) {
        // MIN/MAX 는 대상이 없어도 NULL 한 행을 준다. 그래서 single() 이고,
        // 빈 결과 판정은 wasNull() 로 한다 — getLong 은 NULL 을 0 으로 돌려준다.
        return jdbcClient.sql(SELECT_RANGE_BOUNDS)
                .param("asOf", asOf)
                .query((rs, rowNum) -> {
                    long min = rs.getLong("min_id");
                    return rs.wasNull()
                            ? Optional.<IssuanceIdRange>empty()
                            : Optional.of(new IssuanceIdRange(min, rs.getLong("max_id")));
                })
                .single();
    }

    @Override
    public List<IssuanceHistoryRecord> findRange(
            long fromIssuanceId,
            long toIssuanceId,
            LocalDateTime asOf
    ) {
        return jdbcClient.sql(SELECT_RANGE)
                .param("fromIssuanceId", fromIssuanceId)
                .param("toIssuanceId", toIssuanceId)
                .param("asOf", asOf)
                .query(ROW_MAPPER)
                .list();
    }
}
