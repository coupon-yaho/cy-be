// 정답 매니페스트와 검출을 양방향으로 대조합니다.
package com.kafkick.storage.db.verification;

import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.kafkick.core.verification.ExpectedFindingRepository;
import com.kafkick.core.verification.FindingKey;

/**
 * <b>MySQL 에는 {@code MINUS} 가 없습니다.</b> {@code LEFT JOIN … IS NULL} 로 같은 뜻을 만듭니다.
 *
 * <p><b>조인 키는 {@code (finding_type, target_key)} 두 컬럼뿐입니다.</b>
 * 식별자 컬럼으로 조인하면 규칙마다 안 쓰는 쪽이 양쪽 다 NULL 이고 {@code NULL = NULL} 이
 * UNKNOWN 이라, <b>정확히 검출한 행까지 전부 누락으로 뒤집힙니다.</b>
 *
 * <p><b>조인이 집합 차와 같은 결과를 내는 것은 두 유니크 덕분입니다.</b> 참조 구현
 * ({@code cy-seed/seedgen/verify.py#compare_with_expected})은 파이썬 {@code set} 차인데,
 * 조인은 한쪽에 같은 키가 둘 있으면 행을 불립니다. {@code uk_run_finding(run_id, finding_type,
 * target_key)} 과 {@code uk_expected(seed_run_id, finding_type, target_key)} 가 양쪽의 중복을
 * 막아 두 연산이 같아집니다 — CORRUPT 스키마에서도 이 둘은 살아 있습니다
 * ({@code ddl/12_constraints_corrupt.sql}). <b>둘 중 하나를 떼면 이 등식이 깨집니다.</b>
 *
 * <p><b>비교도 이진입니다.</b> 컬럼에 {@code COLLATE} 가 없어 서버 기본
 * {@code utf8mb4_0900_ai_ci} 를 타는데, 그 콜레이션의 등호는 대소문자·악센트를 무시합니다 —
 * 키가 대소문자만 달라도 <b>매칭된 것으로 세어 거짓 PASS 가 납니다.</b> 검증기가 내면 안 되는
 * 방향의 오류입니다. 참조 구현은 파이썬 {@code set} 차라 <b>바이트 정확</b>이고, 두 구현이
 * 갈리면 이 프로젝트가 기대는 교차 검증이 무의미해집니다.
 *
 * <p><b>대가는 인덱스입니다.</b> 캐스트를 씌우면 {@code uk_expected}·{@code uk_run_finding} 을
 * 조인에 못 씁니다. 정답 800행 · 검출 상한 6만 행인 지금 규모에서는 문제가 아니지만, 커지면
 * 컬럼 콜레이션을 {@code utf8mb4_bin} 으로 바꾸는 것이 답입니다 — 시드 저장소의 DDL 이라
 * 함께 가야 합니다.
 */
@Repository
public class ExpectedFindingJdbcAdapter implements ExpectedFindingRepository {

    /**
     * 정답에 있는데 검출에 없는 것.
     *
     * <p>정렬은 {@code CAST(... AS BINARY)} 다. {@code findings_checksum} 과 같은 이유로
     * 콜레이션 순서를 쓰면 실패 메시지의 표본이 실행마다 달라진다 — UCA 는 V2 키의
     * 구분자 {@code |} 를 숫자보다 앞에 둔다.
     */
    private static final String SELECT_MISSING = """
            SELECT e.finding_type, e.target_key
              FROM expected_findings e
              LEFT JOIN verification_findings f
                     ON f.run_id       = :runId
                    AND CAST(f.finding_type AS BINARY) = CAST(e.finding_type AS BINARY)
                    AND CAST(f.target_key   AS BINARY) = CAST(e.target_key   AS BINARY)
             WHERE e.seed_run_id = :seedRunId
               AND f.id IS NULL
             ORDER BY CAST(e.finding_type AS BINARY), CAST(e.target_key AS BINARY)
            """;

    /** 검출에 있는데 정답에 없는 것. 위를 그대로 뒤집는다. */
    private static final String SELECT_UNEXPECTED = """
            SELECT f.finding_type, f.target_key
              FROM verification_findings f
              LEFT JOIN expected_findings e
                     ON e.seed_run_id  = :seedRunId
                    AND CAST(e.finding_type AS BINARY) = CAST(f.finding_type AS BINARY)
                    AND CAST(e.target_key   AS BINARY) = CAST(f.target_key   AS BINARY)
             WHERE f.run_id = :runId
               AND e.id IS NULL
             ORDER BY CAST(f.finding_type AS BINARY), CAST(f.target_key AS BINARY)
            """;

    /**
     * <b>{@code BIT_XOR} 은 순서를 안 탄다.</b> 집합을 접는 데 정확히 맞는 성질이고,
     * {@code uk_expected(seed_run_id, finding_type, target_key)} 가 중복을 막아
     * "행 다중집합" 과 "키 집합" 이 같아진다 — 그 유니크가 없으면 이 값은 집합 지문이 아니다.
     *
     * <p>{@code GROUP_CONCAT} 을 안 쓰는 이유는 {@code policyDigest} 와 같다 —
     * {@code group_concat_max_len} 을 넘으면 경고만 내고 잘려, 뒤쪽 정답이 지문에서 빠진다.
     */
    private static final String SELECT_DIGEST = """
            SELECT CONCAT(COUNT(*), ':', LPAD(HEX(BIT_XOR(CAST(CONV(SUBSTR(
                     SHA2(CONCAT_WS(0x1f, finding_type, target_key), 256), 1, 16), 16, 10)
                     AS UNSIGNED))), 16, '0'))
              FROM expected_findings
             WHERE seed_run_id = :seedRunId
            """;

    private static final String EXISTS_SEED_RUN = """
            SELECT EXISTS(SELECT 1 FROM expected_findings WHERE seed_run_id = :seedRunId)
            """;

    private final JdbcClient jdbcClient;

    public ExpectedFindingJdbcAdapter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public List<FindingKey> missing(long runId, long seedRunId) {
        return query(SELECT_MISSING, runId, seedRunId);
    }

    @Override
    public List<FindingKey> unexpected(long runId, long seedRunId) {
        return query(SELECT_UNEXPECTED, runId, seedRunId);
    }

    @Override
    public boolean exists(long seedRunId) {
        return Boolean.TRUE.equals(jdbcClient.sql(EXISTS_SEED_RUN)
                .param("seedRunId", seedRunId)
                .query(Boolean.class)
                .single());
    }

    @Override
    public int countOf(long seedRunId) {
        Integer count = jdbcClient.sql(
                        "SELECT COUNT(*) FROM expected_findings WHERE seed_run_id = :seedRunId")
                .param("seedRunId", seedRunId)
                .query(Integer.class)
                .single();

        return count == null ? 0 : count;
    }

    @Override
    public String digestOf(long seedRunId) {
        return jdbcClient.sql(SELECT_DIGEST)
                .param("seedRunId", seedRunId)
                .query(String.class)
                .single();
    }

    private List<FindingKey> query(String sql, long runId, long seedRunId) {
        return jdbcClient.sql(sql)
                .param("runId", runId)
                .param("seedRunId", seedRunId)
                .query((rs, rowNum) -> new FindingKey(
                        rs.getString("finding_type"), rs.getString("target_key")))
                .list();
    }
}
