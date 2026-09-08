// 검출 결과 쓰기 어댑터입니다. 오염셋에서 800행이 한 실행에 쌓입니다.
package com.kafkick.storage.db.verification;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ResidualCount;
import com.kafkick.core.verification.exception.VerificationErrorCode;
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
     * <b>키를 자바로 안 올리고 DB 에서 접는다.</b> 규칙당 상한이 10,000 이고 규칙이 여섯이라
     * 한 실행이 최대 6만 키다 — 두 실행이면 12만이고, 이 조회는 관리자 API 의 5초 예산
     * 안에서 끝나야 한다. 돌려주는 것은 <b>규칙 수만큼의 행</b>이다.
     *
     * <p><b>안쪽 {@code GROUP BY} 가 두 실행을 키로 겹친다.</b> 한 키가 한 실행에 두 번
     * 못 나오므로({@code uk_run_finding}) {@code sides} 는 1 아니면 2 다 —
     * <b>2 면 두 실행에 다 있다</b>. 1 이면 {@code has_after} 가 어느 쪽인지 가른다.
     * 그 유니크가 사라지면 같은 실행 안의 중복이 {@code sides = 2} 를 만들어
     * <b>지속을 과대 보고</b>한다.
     *
     * <p><b>인덱스 모양은 사실이고 실행계획은 안 쟀다.</b> {@code run_id} 는
     * {@code uk_run_finding} 의 선두이고 이 SELECT 가 읽는 세 컬럼이 정확히 그 인덱스의
     * 컬럼이다({@code V1__init_schema.sql} 에서 확인). 반면 {@code GROUP BY} 가 그
     * 인덱스의 <b>접미</b>라 정렬을 그대로 못 쓸 것으로 <b>보이는데</b>, 옵티마이저가
     * 실제로 무엇을 고르는지와 어디가 제일 비싼지는 <b>EXPLAIN 도 타이밍도 안 떴다.</b>
     * 예산을 실제로 넘기는 것을 보는 날 그때 재고 고른다 — 후보는
     * {@code (finding_type, target_key)} 인덱스인데, 그것은 마이그레이션이라 시드 DDL 과
     * 함께 가야 한다(CY-945 가 겪은 비용).
     */
    private static final String SELECT_RESIDUAL_BY_TYPE = """
            SELECT k.finding_type AS finding_type,
                   SUM(k.sides = 2)                        AS persisted,
                   SUM(k.sides = 1 AND k.has_after = 1)    AS introduced,
                   SUM(k.sides = 1 AND k.has_after = 0)    AS resolved
              FROM (SELECT finding_type,
                           target_key,
                           COUNT(*)                  AS sides,
                           MAX(run_id = :afterRunId) AS has_after
                      FROM verification_findings
                     WHERE run_id IN (:beforeRunId, :afterRunId)
                     GROUP BY finding_type, target_key) k
             GROUP BY k.finding_type
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
     * <b>같은 실행을 두 번 주면 전부 "지속" 이 된다 — 그래서 여기서 막는다.</b>
     * 부르는 쪽({@code VerifyReportController})도 같은 검사를 하지만, 이 SQL 은
     * {@code IN (:a, :b)} 라 두 값이 같으면 <b>조용히 한 실행만 훑고</b> 모든 키가
     * {@code sides = 1}·{@code has_after = 1} 이 되어 <b>전부 "새로 생겼다"</b> 로 나온다.
     * 답이 틀린 채로 나가는 갈래라 포트 안에서도 끊는다.
     */
    @Override
    public Map<FindingType, ResidualCount> residualByType(long beforeRunId, long afterRunId) {
        if (beforeRunId == afterRunId) {
            // **raw 예외를 안 던진다.** BatchApiExceptionHandler 의 마지막 그물이
            // Exception 을 500 으로 뭉개므로, 컨트롤러 가드가 어느 날 빠지면 이 조용한
            // 오답이 **500 + 스프링 기본 본문**으로 나간다. 컨트롤러의 requireComparable 이
            // 던지는 것과 **같은 코드**를 쓴다 — 두 자리가 다른 답을 내면 안 된다.
            throw new BusinessException(VerificationErrorCode.RUNS_NOT_COMPARABLE,
                    "같은 실행끼리는 맞댈 수 없습니다. runId=" + beforeRunId);
        }
        Map<FindingType, ResidualCount> residual = new EnumMap<>(FindingType.class);
        jdbcTemplate.query(SELECT_RESIDUAL_BY_TYPE,
                new MapSqlParameterSource()
                        .addValue("beforeRunId", beforeRunId)
                        .addValue("afterRunId", afterRunId),
                rs -> {
                    // **valueOf 를 직접 부르지 않는다.** 그 컬럼에 CHECK 가 없어서 enum 밖
                    // 값이 들어갈 수 있고, 그러면 형제 조회는 UNKNOWN_FINDING_TYPE 봉투를
                    // 내주는데 여기만 500 + 스프링 기본 본문으로 끝난다. 같은 사고에
                    // 답이 갈리면 안 된다 — 근거는 toType 에 있다.
                    residual.put(toType(rs.getString("finding_type"), afterRunId),
                            new ResidualCount(rs.getInt("persisted"),
                                    rs.getInt("introduced"), rs.getInt("resolved")));
                });

        return residual;
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
                (RowCallbackHandler) rs -> {
                    String raw = rs.getString("finding_type");
                    byType.put(toType(raw, runId), rs.getInt("c"));
                });
        return byType;
    }

    /**
     * <b>{@code valueOf} 를 그냥 부르면 안 된다.</b> 그 컬럼에 CHECK 제약이 없어서
     * {@link FindingType} 에 없는 값이 들어갈 수 있고, 그때
     * {@code IllegalArgumentException} 이 그대로 올라가 <b>제출물 조회가 500 + 스프링
     * 기본 본문</b>으로 끝난다 — 원인이 어디에도 안 남는다.
     *
     * <p>형제 어댑터({@code VerificationRunJdbcAdapter})가 이미 같은 자리에서
     * {@code BusinessException} 을 던진다. 그 방식을 따른다.
     *
     * <p><b>건너뛰지 않는다.</b> 모르는 행을 조용히 빼면 규칙별 검출 수가 실제보다 적어지고,
     * 그 리포트가 <b>합격 증거로 쓰인다.</b> 못 읽으면 못 읽는다고 말해야 한다.
     */
    private static FindingType toType(String raw, long runId) {
        try {
            return FindingType.valueOf(raw);
        } catch (IllegalArgumentException e) {
            throw new BusinessException(
                    VerificationErrorCode.UNKNOWN_FINDING_TYPE,
                    "run=" + runId + " finding_type=" + raw);
        }
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
