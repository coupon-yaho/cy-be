// 검출 결과 쓰기 어댑터입니다. 오염셋에서 800행이 한 실행에 쌓입니다.
package com.kafkick.storage.verification;

import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationFindingRepository;

/**
 * {@code campaign_id} 에 <b>회차</b>({@code coupons.id})가, {@code coupon_id} 에
 * <b>발급건</b>({@code issuances.id})이 들어갑니다. 어휘가 뒤집힌 레거시 컬럼명이라
 * 이름만 보고 매핑하면 조회 편의 컬럼이 통째로 어긋납니다.
 */
@Repository
public class VerificationFindingJdbcAdapter implements VerificationFindingRepository {

    /**
     * 재시작 안전. 청크가 죽은 지점부터 다시 도는데 {@code uk_run_finding} 이 걸려 있어
     * 그냥 INSERT 면 이미 쓴 행에서 중복키로 죽는다.
     *
     * <p>갱신 대상은 증적뿐이다. 같은 {@code (run_id, finding_type, target_key)} 면
     * 같은 검출이고, FK 컬럼은 키에서 파생되므로 다시 쓸 것이 없다.
     */
    private static final String UPSERT = """
            INSERT INTO verification_findings
                (run_id, finding_type, target_key,
                 campaign_id, member_id, coupon_id, history_id, expected, actual)
            VALUES (:runId, :findingType, :targetKey,
                    :couponId, :memberId, :issuanceId, :historyId, :expected, :actual) AS new
            ON DUPLICATE KEY UPDATE
                expected = new.expected,
                actual   = new.actual
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public VerificationFindingJdbcAdapter(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void appendAll(long runId, List<VerificationFinding> findings) {
        if (findings.isEmpty()) {
            return;
        }

        SqlParameterSource[] batch = findings.stream()
                .map(finding -> toParams(runId, finding))
                .toArray(SqlParameterSource[]::new);

        jdbcTemplate.batchUpdate(UPSERT, batch);
    }

    private static SqlParameterSource toParams(long runId, VerificationFinding finding) {
        return new MapSqlParameterSource()
                .addValue("runId", runId)
                .addValue("findingType", finding.type().name())
                .addValue("targetKey", finding.targetKey())
                // campaign_id ← 회차, coupon_id ← 발급건. 뒤집힌 레거시 이름이다.
                .addValue("couponId", finding.couponId())
                .addValue("memberId", finding.memberId())
                .addValue("issuanceId", finding.issuanceId())
                .addValue("historyId", finding.historyId())
                .addValue("expected", finding.expected())
                .addValue("actual", finding.actual());
    }
}
