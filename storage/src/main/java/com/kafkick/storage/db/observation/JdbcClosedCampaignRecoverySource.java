package com.kafkick.storage.db.observation;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.kafkick.core.observation.ClosedCampaign;
import com.kafkick.core.observation.ClosedCampaignRecoverySource;

@Repository
@ConditionalOnProperty(
        name = "observation.datasource.enabled",
        havingValue = "true"
)
public class JdbcClosedCampaignRecoverySource
        implements ClosedCampaignRecoverySource {

    private static final String SELECT_RECENT_CLOSED = """
            SELECT id, close_at
              FROM coupons
             WHERE status = 'CLOSED'
               AND close_at >= ?
               AND close_at <= ?
             ORDER BY close_at DESC, id DESC
             LIMIT ?
            """;

    private final JdbcTemplate observationJdbcTemplate;

    public JdbcClosedCampaignRecoverySource(
            @Qualifier("obs") JdbcTemplate observationJdbcTemplate
    ) {
        this.observationJdbcTemplate = Objects.requireNonNull(
                observationJdbcTemplate
        );
    }

    @Override
    public List<ClosedCampaign> findRecentlyClosed(
            Instant fromInclusive,
            Instant toInclusive,
            int limit
    ) {
        Objects.requireNonNull(fromInclusive, "fromInclusive");
        Objects.requireNonNull(toInclusive, "toInclusive");
        if (limit <= 0) {
            throw new IllegalArgumentException(
                    "기동 보정 조회 상한은 양수여야 합니다."
            );
        }
        return observationJdbcTemplate.query(
                SELECT_RECENT_CLOSED,
                (resultSet, rowNumber) -> new ClosedCampaign(
                        resultSet.getLong("id"),
                        resultSet.getTimestamp("close_at").toInstant()
                ),
                Timestamp.from(fromInclusive),
                Timestamp.from(toInclusive),
                limit
        );
    }
}
