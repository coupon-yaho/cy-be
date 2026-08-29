// 리플레이 입력 읽기 어댑터입니다. issuance_histories 를 아는 유일한 곳입니다.
package com.kafkick.storage.db.verification;

import static com.kafkick.storage.db.verification.ColumnValues.toEnum;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.verification.replay.IssuanceHistoryRecord;
import com.kafkick.core.verification.replay.ReplayScanRange;
import com.kafkick.core.verification.replay.ReplayHistoryRepository;

/**
 * 구간을 받아 그 안의 이력만 읽는다. 구간을 나누는 판단은 배치가 하고
 * 여기는 SQL 만 안다.
 */
@Repository
public class ReplayHistoryJdbcAdapter implements ReplayHistoryRepository {

    /**
     * <b>이 문장은 전체를 훑는다.</b> 실행마다 한 번뿐이라 감수한다.
     *
     * <p>⚠️ <b>인덱스가 없어서가 아니다.</b> {@code created_at} 인덱스는 있다 —
     * {@code idx_issuance_histories_created_id}({@code V2026082502}, 관리자 이력 조회용).
     * 못 타는 이유는 <b>세 집계가 전부 {@code CASE} 로 감싸져 있어서</b>다. 한때 여기
     * "인덱스가 없어" 라고 적혀 있었는데(CY-744 3차 리뷰가 잡았다), 그 말을 믿고
     * {@code (created_at)} 인덱스를 새로 파면 <b>이미 있는 것을 또 파면서 이 문장은 그대로</b>다.
     *
     * <p>전체 최대 시각까지 <b>같은 스캔에서</b> 잰다. 따로 질의하면 풀스캔이 두 번이고,
     * 두 스캔 사이에 행이 들어오면 검증하려던 값끼리 어긋난다.
     */
    private static final String SELECT_SCAN_RANGE = """
            SELECT MIN(CASE WHEN created_at <= :asOf THEN issuance_id END) AS min_issuance_id,
                   MAX(CASE WHEN created_at <= :asOf THEN issuance_id END) AS max_issuance_id,
                   MAX(CASE WHEN created_at <= :asOf THEN id END)          AS max_history_id,
                   MAX(created_at)                                          AS latest_created_at
              FROM issuance_histories
            """;

    /**
     * {@code ORDER BY} 에 {@code id} 를 넣는 것이 타이브레이커다.
     * 같은 {@code created_at} 이 여럿이면 이것 없이는 실행마다 순서가 달라져
     * 접은 결과가 흔들린다.
     *
     * <p>⚠️ <b>이 정렬은 현재 스키마에서 filesort 를 강제한다 — 창 하나가 통째로 정렬
     * 대상이다.</b> 세 컬럼이 전부 ASC 인데 그 조합의 인덱스는
     * {@code idx_issuance_histories_issuance_created_id (issuance_id, created_at DESC, id DESC)}
     * 하나뿐이라 방향이 안 맞는다(관리자 조회용으로 들어온 것이다). 기본
     * {@code batch.verify.replay-window-size} 가 50,000 이고 발급건당 이력이 약 1.8행이라
     * <b>창 하나가 약 9만 행</b>이고, 그것이 청크 트랜잭션 안에서 정렬된다.
     * 실측(MySQL 8.4): 같은 질의를 {@code created_at DESC, id DESC} 로 바꾸면 정렬이 사라진다.
     *
     * <p><b>처방은 이미 있다</b> — {@code 90_perf_indexes_optional.sql} 의
     * {@code idx_history_issuance (issuance_id, created_at)}. InnoDB 가 PK 를 뒤에 붙여
     * 실질 {@code (issuance_id, created_at, id)} ASC 라 이 정렬을 그대로 덮는다.
     * <b>넣는 시점은 인덱스 실측 티켓의 몫</b>이고, 그때 개선폭의 상당 부분이 "커버링" 이
     * 아니라 <b>"filesort 제거"</b> 라는 것을 {@code EXPLAIN ANALYZE} 앞뒤로 남겨야
     * 원인을 잘못 귀속하지 않는다.
     *
     * <p><b>{@code INNER JOIN} 이라 고아 이력이 빠지는데, 그런 행은 물리적으로 없다.</b>
     * {@code issuance_histories.issuance_id → issuances.id} FK 가
     * <b>세 스키마 전부</b>에 살아 있다 — cy-be {@code V1__init_schema.sql:643} ·
     * 시드 {@code 10_constraints_common.sql:50} · CORRUPT
     * ({@code V9999999999__drop_clean_only_constraints.sql} 이 이 FK 를 안 뗀다).
     * 고아 이력은 애초에 <b>검증 대상이 아니다</b>(설계 §검증 금지) — 오염셋도 안 심는다.
     * FK 를 떼는 날 이 조인은 {@code LEFT JOIN} 이어야 하고, 그러면
     * {@code expires_at} 이 {@code null} 로 와 {@code HistoryReplay.settledOutcome} 이
     * 판정을 포기한다(그쪽이 {@code null} 을 이미 그렇게 다룬다).
     */
    private static final String SELECT_RANGE = """
            SELECT h.id, h.issuance_id, h.event_type, h.from_status, h.to_status,
                   h.created_at, i.expires_at
              FROM issuance_histories h
              JOIN issuances i ON i.id = h.issuance_id
             WHERE h.issuance_id BETWEEN :fromIssuanceId AND :toIssuanceId
               AND h.id <= :maxHistoryId
               AND h.created_at <= :asOf
             ORDER BY h.issuance_id, h.created_at, h.id
            """;

    private static final RowMapper<IssuanceHistoryRecord> ROW_MAPPER =
            (rs, rowNum) -> new IssuanceHistoryRecord(
                    rs.getLong("id"),
                    rs.getLong("issuance_id"),
                    IssuanceEventType.valueOf(rs.getString("event_type")),
                    toEnum(rs.getString("from_status"), IssuanceStatus::valueOf),
                    IssuanceStatus.valueOf(rs.getString("to_status")),
                    rs.getObject("created_at", LocalDateTime.class),
                    rs.getObject("expires_at", LocalDateTime.class)
            );

    private final JdbcClient jdbcClient;

    public ReplayHistoryJdbcAdapter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public Optional<ReplayScanRange> scanRange(LocalDateTime asOf) {
        // MIN/MAX 는 대상이 없어도 NULL 한 행을 준다. 그래서 single() 이고,
        // 빈 결과 판정은 wasNull() 로 한다 — getLong 은 NULL 을 0 으로 돌려준다.
        return jdbcClient.sql(SELECT_SCAN_RANGE)
                .param("asOf", asOf)
                .query((rs, rowNum) -> {
                    LocalDateTime latest = rs.getObject("latest_created_at", LocalDateTime.class);
                    if (latest == null) {
                        // 이력이 한 행도 없다. 검증할 것도 거부할 것도 없다.
                        return Optional.<ReplayScanRange>empty();
                    }

                    // 창은 비어도 마지막 시각은 돌려준다 — asOf 가 모든 이력보다 앞서는 경우가
                    // 바로 거부해야 하는 경우라, 여기서 함께 비우면 그 검사를 건너뛴다.
                    long minIssuanceId = rs.getLong("min_issuance_id");
                    if (rs.wasNull()) {
                        return Optional.of(new ReplayScanRange(latest, null, null, null));
                    }

                    return Optional.of(new ReplayScanRange(
                            latest,
                            minIssuanceId,
                            rs.getLong("max_issuance_id"),
                            rs.getLong("max_history_id")));
                })
                .single();
    }

    @Override
    public List<IssuanceHistoryRecord> findRange(
            long fromIssuanceId,
            long toIssuanceId,
            LocalDateTime asOf,
            long maxHistoryId
    ) {
        return jdbcClient.sql(SELECT_RANGE)
                .param("fromIssuanceId", fromIssuanceId)
                .param("toIssuanceId", toIssuanceId)
                .param("asOf", asOf)
                .param("maxHistoryId", maxHistoryId)
                .query(ROW_MAPPER)
                .list();
    }
}
