// 검출 결과 쓰기 어댑터입니다. 오염셋에서 800행이 한 실행에 쌓입니다.
package com.kafkick.storage.db.verification;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationFindingRepository;

/**
 * {@code campaign_id} 에 <b>회차</b>({@code coupons.id})가, {@code coupon_id} 에
 * <b>발급건</b>({@code issuances.id})이 들어갑니다. 어휘가 뒤집힌 레거시 컬럼명이라
 * 이름만 보고 매핑하면 조회 편의 컬럼이 통째로 어긋납니다.
 */
@Repository
public class VerificationFindingJdbcAdapter implements VerificationFindingRepository {

    /** 계약이 정한 필드 구분자 U+001F. 값에 들어갈 수 없어 경계가 모호해지지 않는다. */
    private static final byte FIELD_SEPARATOR = 0x1f;

    /** 계약이 정한 레코드 구분자 U+001E. */
    private static final byte RECORD_SEPARATOR = 0x1e;

    /**
     * <b>정렬이 계약의 일부다.</b> 같은 집합이라도 순서가 다르면 다른 checksum 이 나와
     * 재실행 판정이 갈린다. {@code (finding_type, target_key)} 는 {@code uk_run_finding} 이
     * 유일성을 보장하므로 이 정렬이 전순서다 — 타이브레이커가 필요 없다.
     *
     * <p><b>{@code CAST(... AS BINARY)} 여야 한다.</b> 컬럼 콜레이션이
     * {@code utf8mb4_0900_ai_ci} 라 UCA 순서를 쓰는데, 참조 구현({@code cy-seed/seedgen/stats.py})은
     * 파이썬 {@code sorted()} 즉 <b>코드포인트 순서</b>다. V2 키만 구분자 {@code |} 를 갖고
     * UCA 는 {@code |}(U+007C)를 숫자보다 앞에 두므로 둘이 갈린다(실측).
     *
     * <pre>
     * 콜레이션  COUPON:1|MEMBER:2   COUPON:11|MEMBER:2
     * 코드포인트 COUPON:11|MEMBER:2  COUPON:1|MEMBER:2
     * </pre>
     *
     * 오염셋의 {@code DUP_PER_MEMBER} 200행이 전부 그 모양이고 회차 id 가 1~291 이라
     * 자릿수가 섞인다. 검출이 정답과 <b>완벽히 일치해도 checksum 만 달라져</b>,
     * 판정표의 "지문 같음 + checksum 다름 = 검증기 버그" 칸에 거짓 양성이 찍힌다.
     *
     * <p>덤으로 {@code uk_run_finding} 커버링 인덱스를 못 타게 되어,
     * <b>{@code ORDER BY} 를 지우면 테스트가 잡는다</b> — 전에는 인덱스가 정렬을 대신해 줘서
     * 블랙박스로 확인할 수 없는 자리였다.
     */
    private static final String SELECT_CHECKSUM_INPUT = """
            SELECT finding_type, target_key
              FROM verification_findings
             WHERE run_id = :runId
             ORDER BY CAST(finding_type AS BINARY), CAST(target_key AS BINARY)
            """;

    private static final String SELECT_COUNT = """
            SELECT COUNT(*) FROM verification_findings WHERE run_id = :runId
            """;

    /**
     * <b>{@code ORDER BY} 는 이 포트의 계약이지 리포트 순서의 근거가 아니다.</b>
     * {@code GROUP BY} 의 출력 순서를 MySQL 이 보장하지 않으므로 여기서 고정한다.
     *
     * <p><b>다만 지금 소비자는 그 순서를 안 쓴다.</b> {@code VerifyReportView.of} 가 결과를
     * 버리고 {@code FindingType.values()} 로 처음부터 다시 채운다 — 검출이 0인 규칙까지
     * 보여야 하기 때문이다. 제출물의 결정론을 지는 것은 그 순회이지 이 {@code ORDER BY} 가
     * 아니다. 한때 여기 그 반대로 적혀 있었다.
     */
    private static final String SELECT_COUNT_BY_TYPE = """
            SELECT finding_type, COUNT(*) AS c
              FROM verification_findings
             WHERE run_id = :runId
             GROUP BY finding_type
             ORDER BY finding_type
            """;

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

    /**
     * 한 번에 보낼 행 수. 전량을 한 배열로 만들면 검출 객체와 파라미터 배열이 동시에 살아
     * 상한 직전에서 메모리가 두 배가 된다 — 상한이 막으려던 그 실패가 상한 안에서 일어난다.
     */
    private static final int BATCH_SIZE = 1_000;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public VerificationFindingJdbcAdapter(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void appendAll(long runId, List<VerificationFinding> findings) {
        if (findings.isEmpty()) {
            return;
        }

        for (int from = 0; from < findings.size(); from += BATCH_SIZE) {
            int to = Math.min(from + BATCH_SIZE, findings.size());

            SqlParameterSource[] batch = findings.subList(from, to).stream()
                    .map(finding -> toParams(runId, finding))
                    .toArray(SqlParameterSource[]::new);

            jdbcTemplate.batchUpdate(UPSERT, batch);
        }
    }

    @Override
    public int countOf(long runId) {
        Integer count = jdbcTemplate.queryForObject(
                SELECT_COUNT, new MapSqlParameterSource("runId", runId), Integer.class);

        return count == null ? 0 : count;
    }

    /**
     * <b>중간 리스트를 만들지 않는다.</b> 행을 받는 즉시 다이제스트에 넣어,
     * 검출 객체 리스트와 그 복사본이 동시에 살지 않는다.
     *
     * <p><b>커서 스트리밍은 아니다.</b> {@code fetchSize} 를 주지 않아 드라이버가 결과를
     * 전량 버퍼링한다. 행 수 방어는 여기가 아니라 규칙 Step 의 상한이 한다 —
     * 기본값 10000 × 6규칙이라 천장이 6만 행이고, 그 이상은 애초에 저장되지 않는다.
     * <b>그 상한을 올리려면 여기부터 다시 봐야 한다.</b>
     *
     * <p>바이트로 직접 넣는다. 문자열로 이어 붙이면 구분자가 문자로 인코딩되는 방식에
     * 결과가 묶여, 나중에 인코딩이 바뀌면 <b>같은 데이터가 다른 checksum</b> 을 낸다.
     */
    @Override
    public String checksumOf(long runId) {
        MessageDigest digest = DigestValues.sha256();

        jdbcTemplate.query(SELECT_CHECKSUM_INPUT, new MapSqlParameterSource("runId", runId),
                (RowCallbackHandler) rs -> {
                    digest.update(rs.getString("finding_type").getBytes(StandardCharsets.UTF_8));
                    digest.update(FIELD_SEPARATOR);
                    digest.update(rs.getString("target_key").getBytes(StandardCharsets.UTF_8));
                    digest.update(RECORD_SEPARATOR);
                });

        return DigestValues.hex(digest.digest());
    }
    @Override
    public Map<FindingType, Integer> countByType(long runId) {
        Map<FindingType, Integer> byType = new LinkedHashMap<>();
        jdbcTemplate.query(SELECT_COUNT_BY_TYPE, new MapSqlParameterSource("runId", runId),
                (RowCallbackHandler) rs ->
                        byType.put(FindingType.valueOf(rs.getString("finding_type")),
                                rs.getInt("c")));
        return byType;
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
