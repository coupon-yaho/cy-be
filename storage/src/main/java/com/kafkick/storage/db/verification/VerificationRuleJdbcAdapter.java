// V1·V2·V3·V5·V6 판정 SQL 입니다. 규칙마다 드라이빙 테이블이 다릅니다.
package com.kafkick.storage.db.verification;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Stream;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationRuleRepository;

/**
 * <b>{@code asof_state.coupon_id} 는 발급건({@code issuances.id})입니다.</b> 레거시 컬럼명이라
 * 회차로 읽으면 조인이 통째로 어긋납니다.
 *
 * <p>{@code target_key} 를 SQL 에서 만들지 않습니다. 형식이 코드와 SQL 두 곳에 생기면
 * 갈라지고, 그러면 개수는 맞는데 키만 달라져 누락과 오탐이 동시에 뜹니다.
 * 여기서는 식별자만 꺼내고 키는 {@link VerificationFinding} 의 팩토리가 만듭니다.
 */
@Repository
public class VerificationRuleJdbcAdapter implements VerificationRuleRepository {

    /**
     * <b>배치가 없으면 아무것도 못 하는 테이블</b>만 넣습니다. 목록을 넓히면 스키마가
     * 조금씩 자랄 때마다 기동이 막혀, 가드가 "배포 순서" 가 아니라 "스키마 최신성" 을
     * 보게 됩니다 — 그건 Flyway 의 몫입니다.
     *
     * <p><b>Spring Batch 메타 테이블을 함께 봅니다.</b> {@code docs/11} 이 배포 순서 위반의
     * 증상으로 지목한 문자열이 바로 {@code Table 'BATCH_JOB_INSTANCE' doesn't exist} 이고,
     * 그것 없이는 <b>어떤 잡도 못 돕니다</b>. 그리고 이 축은 데이터 테이블과 <b>따로 빕니다</b> —
     * 검증용 셋({@code coupon_clean}·{@code coupon_corrupt})은 cy-seed 의 {@code ddl/} 로
     * 만들어지는데 거기에 {@code BATCH_*} 가 하나도 없습니다. 데이터 넷은 다 있고 메타만
     * 없는 상태가 정상 절차에서 실제로 생깁니다.
     *
     * <p>아홉 개 전부가 아니라 <b>넷만</b> 봅니다. {@code V11__batch_metadata.sql} 은 원본
     * 그대로라 부분 적용되는 경로가 없어, 이 넷이 있으면 나머지도 있습니다. 넷째
     * ({@code BATCH_JOB_EXECUTION_PARAMS})는 CY-368 이 더했습니다 — {@code nextAttempt} 가
     * 그 테이블을 읽으므로 접수 경로의 필수 의존이 됐습니다.
     */
    private static final List<String> DATA_TABLES =
            List.of("issuances", "issuance_histories", "verification_runs", "coupon_stocks");

    /** {@code V11__batch_metadata.sql} 이 만듭니다. 없으면 잡 실행 시점에 SQL 에러로 죽습니다. */
    private static final List<String> BATCH_META_TABLES = List.of(
            "BATCH_JOB_INSTANCE", "BATCH_JOB_EXECUTION", "BATCH_STEP_EXECUTION",
            // CY-368 이 이 테이블을 **접수 경로의 필수 의존**으로 승격시켰다 —
            // nextAttempt 가 배치 메타의 attempt 를 여기서 읽는다. 목록이 안 따라가면
            // 기동은 초록인데 첫 트리거가 "Table ... doesn't exist" 로 죽는다.
            // 이 가드가 없애려는 늦은 실패 그 자체다.
            "BATCH_JOB_EXECUTION_PARAMS");

    /**
     * <b>테이블이 있다고 컬럼도 있는 것은 아니다.</b> 여기 넣는 기준은 <i>"이것이 없으면
     * 그 경로가 매번 SQL 에러로 죽는가"</i> 다 — 스키마 최신성 전반이 아니라, 배치가
     * 질의문에 <b>이름으로 박아 둔</b> 컬럼만 본다.
     *
     * <p>{@code origin} 은 cy-seed {@code 1f217b5} 부터 생겼고, 그 이전 검증용 셋에는
     * 테이블은 다 있고 이것만 없다. Flyway 가 그 DB 에 안 닿아 재생성 말고는 답이 없다.
     */
    private static final List<String> CRITICAL_COLUMNS =
            List.of("verification_runs.origin");

    /**
     * <b>없어도 기동과 동작이 통과하는 인덱스 둘.</b> {@code V2026082513}·{@code V2026082514}
     * 가 판다. 빠지면 되읽기가 {@code STATUS}·{@code END_TIME} 을 전체 스캔하고 정리 잡이
     * {@code CREATE_TIME} 을 매 청크 전체 스캔한다 — 조용히 느려질 뿐이라 늦게 드러난다.
     */
    private static final List<String> CRITICAL_INDEXES =
            List.of("BATCH_JOB_EXECUTION.IX_JOB_EXEC_STATUS_END(STATUS,END_TIME)",
                    "BATCH_JOB_EXECUTION.IX_JOB_EXEC_CREATE_TIME(CREATE_TIME)");

    private static final List<String> CORE_TABLES =
            Stream.concat(DATA_TABLES.stream(), BATCH_META_TABLES.stream()).toList();

    /**
     * 접은 상태와 저장된 상태가 다른 발급건.
     *
     * <p>{@code asof_state} 를 드라이빙 테이블로 잡는다. {@code issuances} 를 드라이빙으로 잡으면
     * 접힌 상태가 없는 발급건이 {@code state IS NULL} 로 올라와, 기대 매트릭스에 없는 검출을 낸다.
     *
     * <p>{@code i.updated_at <= :asOf} 로 자른다. 접힌 상태는 asOf 로 얼어 있는데 저장 상태는
     * 질의 순간의 현재값이라, 이 조건이 없으면 배치가 도는 동안 런타임이 건드린 발급건이
     * 전부 어긋난 것으로 잡히고 재실행 결과도 달라진다.
     *
     * <p><b>이 조건이 실제로 무언가를 거르면 그건 이미 비정상이다.</b> 실행 시작에
     * {@link #hasIssuancesUpdatedAfter} 로 거부하므로, 정상 경로에서는 여기서 걸러지는 행이 없다.
     * 남는 것은 가드 통과 후 이 질의까지의 짧은 틈뿐이다.
     */
    // stored 는 MySQL 예약어다(GENERATED ... STORED). 별칭에 그대로 쓰면 문법 오류가 난다.
    //
    // 문장 상한은 여기 걸지 않는다. MAX_EXECUTION_TIME 은 read-only SELECT 에만 먹어서
    // 이 잡에서 가장 무거운 문장(300만 행 UPDATE)을 못 덮는다. 상한은 Step 의 트랜잭션 속성으로
    // 건다 — Spring 이 그 트랜잭션의 모든 Statement 에 setQueryTimeout 을 적용한다.
    private static final String SELECT_REPLAY_MISMATCH = """
            SELECT a.coupon_id AS issuance_id,
                   a.state     AS replayed_state,
                   i.status    AS stored_status
              FROM asof_state a
              JOIN issuances i ON i.id = a.coupon_id
             WHERE a.run_id = :runId
               AND i.updated_at <= :asOf
               AND a.state <> i.status
             ORDER BY a.coupon_id
             LIMIT :limit
            """;

    /**
     * 상태와 활성 사용 건수가 어긋나는 발급건.
     *
     * <p>불변식은 {@code USED} 면 1건, 아니면 0건이다. 한쪽 방향만 보면
     * 이중 사용({@code USED} 인데 2건)을 놓친다.
     *
     * <p><b>{@code asof_state} 를 드라이빙으로 쓰므로 이력이 하나도 없는 발급건은 시야 밖이다.</b>
     * 그런 발급건은 {@code asof_state} 행 자체가 안 생겨, 활성 사용 행이 남아 있어도 여기서
     * 안 보인다(V3 도 같은 드라이빙이라 같이 못 본다). V1 이 회차 재고 집계로 간접 검출할 뿐
     * 발급건 단위로는 아무도 지목하지 못한다. 계약의 오염 유형 7 은 ISSUE 이력이 있는 발급건에
     * 심으므로 정답 매니페스트 대조는 영향을 받지 않는다 — 런타임 사고(발급건은 만들어졌는데
     * 이력 INSERT 만 실패)에서만 벌어지는 사각이라 별도 관측 지표로 다룬다.
     */
    private static final String SELECT_USAGE_MISMATCH = """
            SELECT a.coupon_id                                       AS issuance_id,
                   a.active_usage_count                              AS actual_count,
                   CASE WHEN a.state = 'USED' THEN 1 ELSE 0 END      AS expected_count
              FROM asof_state a
             WHERE a.run_id = :runId
               AND a.active_usage_count <> CASE WHEN a.state = 'USED' THEN 1 ELSE 0 END
             ORDER BY a.coupon_id
             LIMIT :limit
            """;

    /**
     * {@code updated_at} 에 인덱스가 없어 <b>이 문장도 0건일 때 300만 행을 끝까지 읽는다</b> —
     * 정상 경로가 곧 최악 경로다. {@code EXISTS} 가 {@code COUNT(*)} 보다 나은 것은
     * "갱신이 실제로 있는" 비정상 경로뿐이다.
     *
     * <p>처방은 {@code cy-seed/ddl/90_perf_indexes_optional.sql} 이고, 붙이기 전후
     * {@code EXPLAIN ANALYZE} 가 과제의 일부다. 지금 이 주석은 <b>개선을 주장하지 않는다.</b>
     */
    private static final String EXISTS_UPDATED_AFTER = """
            SELECT EXISTS(SELECT 1 FROM issuances WHERE updated_at > :asOf LIMIT 1)
            """;

    /**
     * V6 판정이 읽는 <b>살아 있는 두 테이블</b>을 한 값으로 접는다.
     *
     * <p><b>축이 둘인 이유.</b> V6 은 {@code (coupons.eligible_grades_mask & grades.bit_value) = 0}
     * 으로 판정한다 — AND 의 양쪽이 각각 다른 테이블이다. {@code coupons} 만 얼리면
     * {@code SILVER} 의 {@code bit_value} 를 2→16 으로 옮기는 것만으로 검출 집합이 달라지는데
     * 지문은 그대로다. 둘 다 {@code dataset_fingerprint} 재료에 없고 {@code updated_at} 도 없어
     * 시각 비교를 못 하므로, 값 자체를 접는다. 회차 147~291행 · 등급 4행이라 비용이 없다.
     *
     * <p><b>{@code GROUP_CONCAT} 을 쓰지 않는다.</b> {@code group_concat_max_len} 기본값이
     * 1024 바이트인데 <b>CLEAN 147행이 실측 920 바이트</b>(한계의 90%)이고
     * <b>CORRUPT 291행이면 약 1820 바이트로 이미 한계를 넘는다.</b>
     * 지금 잘리지 않는 것은 {@code GROUP_CONCAT} 을 안 쓰기 때문이지 여유가 있어서가 아니다.
     * MySQL 은 넘치면
     * <b>경고만 내고 조용히 자른다.</b> 뒤쪽 회차의 마스크가 바뀌어도 지문이 그대로가 되어
     * <b>가드가 열린 채로 실패한다.</b> 세션 변수를 올릴 수도 있지만 커넥션 상태에 기대게 된다.
     *
     * <p>행별 해시를 {@code BIT_XOR} 로 접으면 길이 제한이 없다. PK 가 해시 재료에 있어
     * 같은 해시가 둘 나올 수 없으니 상쇄도 없다. 행 수를 앞에 붙여 <b>INSERT·DELETE</b> 도 잡는다 —
     * XOR 만으로는 두 행이 함께 사라지는 경우를 놓칠 수 있다. 종류 접두사({@code 'C'}/{@code 'G'})는
     * 두 축의 해시 공간이 겹치지 않게 한다.
     */
    private static final String SELECT_POLICY_DIGEST = """
            SELECT CONCAT(
                     (SELECT CONCAT(COUNT(*), ':', LPAD(HEX(BIT_XOR(
                          CAST(CONV(SUBSTR(
                              SHA2(CONCAT_WS(0x1f, 'C', id, eligible_grades_mask), 256), 1, 16),
                              16, 10) AS UNSIGNED))), 16, '0'))
                        FROM coupons),
                     0x1e,
                     (SELECT CONCAT(COUNT(*), ':', LPAD(HEX(BIT_XOR(
                          CAST(CONV(SUBSTR(
                              SHA2(CONCAT_WS(0x1f, 'G', code, bit_value), 256), 1, 16),
                              16, 10) AS UNSIGNED))), 16, '0'))
                        FROM grades))
            """;

    /** 재고는 회차가 147~291행이라 훑는 비용이 없다. 그래도 형태는 발급건 쪽과 같이 둔다. */
    private static final String EXISTS_STOCK_UPDATED_AFTER = """
            SELECT EXISTS(SELECT 1 FROM coupon_stocks WHERE updated_at > :asOf LIMIT 1)
            """;

    /**
     * 접은 활성 건수와 저장된 재고가 다른 회차.
     *
     * <p><b>{@code coupons} 가 드라이빙이다.</b> {@code asof_state} 를 잡으면 활성이 0인 회차가,
     * {@code coupon_stocks} 를 잡으면 재고 행이 없는 회차가 각각 결과에서 빠진다 —
     * 둘 다 잡아야 할 상태다. 재고 없이 발급이 쌓이는 것은 초과 발급의 가장 위험한 형태이고,
     * 재고가 남았는데 발급이 0인 것은 오염 유형 1 이다. 회차가 147~291개라 전수 비용이 없다.
     *
     * <p>활성은 {@code ISSUED}·{@code USED} 다 — 컬럼 주석이 못 박은 <b>현재 보유량</b>이고
     * 누적 발급 수가 아니다. {@code CANCELLED}·{@code EXPIRED} 는 재고로 돌아간 것이라 빠진다.
     *
     * <p>{@code s.updated_at <= :asOf} 로 자르는 이유는 V3 와 같다. 정상 경로에서는
     * {@link #hasStocksUpdatedAfter} 가 시작에서 막으므로 여기서 걸러지는 행이 없다.
     */
    private static final String SELECT_STOCK_MISMATCH = """
            SELECT c.id                            AS coupon_id,
                   COALESCE(r.active_count, 0)     AS replayed_active,
                   COALESCE(s.active_count, 0)     AS stored_active
              FROM coupons c
              LEFT JOIN coupon_stocks s ON s.coupon_id = c.id
              LEFT JOIN (
                    SELECT i.coupon_id      AS coupon_id,
                           COUNT(*)         AS active_count
                      FROM asof_state a
                      JOIN issuances i ON i.id = a.coupon_id
                     WHERE a.run_id = :runId
                       AND a.state IN ('ISSUED', 'USED')
                     GROUP BY i.coupon_id
                   ) r ON r.coupon_id = c.id
             WHERE (s.updated_at IS NULL OR s.updated_at <= :asOf)
               AND COALESCE(r.active_count, 0) <> COALESCE(s.active_count, 0)
             ORDER BY c.id
             LIMIT :limit
            """;

    /**
     * 발급 시점 등급이 회차의 허용 등급에 없는 발급건.
     *
     * <p><b>{@code members} 를 조인하지 않는다.</b> 현재 등급으로 판정하면 회원이 강등되는 순간
     * 정상 발급이 위반으로 잡힌다. 스냅샷 둘만 본다.
     *
     * <p><b>그래도 결정론은 아니다.</b> {@code coupons.eligible_grades_mask} 는 살아 있는 행이고
     * 지문 재료에도 없다. 그래서 {@code i.updated_at <= :asOf} 로 "어떤 발급건을 볼지" 만이라도
     * 고정한다 — <b>이 조건을 지우면 재실행이 갈린다.</b> 정책 축은 {@code policyDigest} 가 지킨다.
     *
     * <p><b>참조 구현과 갈리는 자리다.</b> {@code cy-seed/seedgen/verify.py} 는 INNER JOIN 이라
     * 미등록 등급을 안 잡는다. 지금은 {@code issued_grade} 의 FK 가 그런 행의 발생 자체를 막아
     * 두 집합이 같지만, <b>FK 가 빠지면 여기만 검출해 오탐이 된다.</b>
     *
     * <p><b>{@code grades} 를 LEFT JOIN 으로 잡는다.</b> INNER 로 잡으면 {@code grades} 에 없는
     * 등급 문자열이 조용히 빠지는데, 그것이야말로 잡아야 할 위반이다.
     * 오타든 새 등급이든 마스크에 자리가 없으면 발급되면 안 된다.
     */
    private static final String SELECT_GRADE_VIOLATION = """
            SELECT i.id                    AS issuance_id,
                   i.issued_grade          AS issued_grade,
                   c.eligible_grades_mask  AS eligible_mask,
                   g.bit_value             AS grade_bit
              FROM issuances i
              JOIN coupons c ON c.id = i.coupon_id
              LEFT JOIN grades g ON g.code = i.issued_grade
             WHERE i.updated_at <= :asOf
               AND (g.bit_value IS NULL OR (c.eligible_grades_mask & g.bit_value) = 0)
             ORDER BY i.id
             LIMIT :limit
            """;

    /**
     * V2 1인 1매 위반. 케이스 둘을 {@code UNION} 으로 합친다 —
     * 한 회원이 두 케이스에 다 걸려도 {@code target_key} 가 같아 한 행이어야 한다.
     * {@code UNION ALL} 을 쓰면 {@code uk_run_finding} 중복키로 잡 전체가 죽는다.
     *
     * <p><b>케이스 2 에서 {@code MIN(id)} 를 뺀다.</b> 같은 code 가 둘이면 먼저 발급된 쪽은
     * 정상이고 복제본만 위반이다. 안 빼면 원본 회원까지 검출돼 <b>오탐이 오염 수만큼 늘어난다.</b>
     *
     * <p>두 케이스 모두 {@code updated_at <= :asOf} 로 자른다 — 어떤 발급건을 볼지가
     * 고정돼야 재실행 결과가 같다. 집계 안팎에 모두 걸어야 한다:
     * 안쪽을 안 자르면 {@code asOf} 이후 행이 {@code COUNT(*)} 를 올려 <b>없는 중복이 보인다.</b>
     */
    private static final String SELECT_DUPLICATE_ISSUANCE = """
            SELECT coupon_id, member_id
              FROM (
                    SELECT coupon_id, member_id
                      FROM issuances
                     WHERE updated_at <= :asOf
                     GROUP BY coupon_id, member_id
                    HAVING COUNT(*) > 1

                    UNION

                    SELECT i.coupon_id, i.member_id
                      FROM issuances i
                      JOIN (
                            SELECT coupon_id, code, MIN(id) AS original_id
                              FROM issuances
                             WHERE updated_at <= :asOf
                             GROUP BY coupon_id, code
                            HAVING COUNT(*) > 1
                           ) d ON d.coupon_id = i.coupon_id AND d.code = i.code
                     WHERE i.updated_at <= :asOf
                       AND i.id <> d.original_id
                   ) dup
             ORDER BY coupon_id, member_id
             LIMIT :limit
            """;

    /** 계약이 정한 재료 구분자. */
    private static final String FINGERPRINT_SEPARATOR = "|";

    /** 시드 참조 구현({@code stats.py})의 폴백. 갈리면 같은 데이터에 다른 지문이 나온다. */
    private static final LocalDateTime EMPTY_DATASET_TIME = LocalDateTime.of(1970, 1, 1, 0, 0);

    private static final DateTimeFormatter FINGERPRINT_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    /**
     * 계약의 {@code fingerprint.formula} 를 재료 다섯으로 뽑는다. 이어 붙이기와 해싱은
     * 자바가 한다 — 시각 포맷이 계약에 <b>문자열로</b> 정해져 있어 DB 의 기본 표현에 맡길 수 없다.
     *
     * <p>이력은 {@code created_at <= :asOf} 로 자른다. 나머지 넷은 <b>안 자른다</b> —
     * 계약이 그 필터를 이력에만 걸었고, {@code coupon_stocks} 에는 그럴 컬럼도 없다.
     * 대신 {@code assertFrozenStep} 이 실행 중 갱신을 막는다.
     *
     * <p>빈 데이터셋에서 {@code MAX}·{@code SUM} 은 NULL 이다. 그 자리는
     * <b>시드 저장소의 참조 구현이 쓰는 값</b>을 그대로 쓴다 —
     * {@code cy-seed/seedgen/stats.py#dataset_fingerprint} 가 숫자는 {@code 0},
     * 시각은 {@code 1970-01-01 00:00:00.000000} 으로 접는다.
     *
     * <p><b>여기서 다른 값을 쓰면 안 된다.</b> 시드의 매니페스트와 배치의 지문은
     * <b>대조하라고 있는 값</b>이라({@code seedgen/manifest.py}) 폴백이 갈리면
     * 같은 데이터에 다른 지문이 나온다. 처음에 {@code "NULL"} 로 짰다가 참조 구현과 맞췄다.
     */
    private static final String SELECT_FINGERPRINT_INPUT = """
            SELECT (SELECT MAX(id) FROM issuance_histories WHERE created_at <= :asOf) AS max_history_id,
                   (SELECT COUNT(*) FROM issuance_histories WHERE created_at <= :asOf) AS history_count,
                   (SELECT COUNT(*) FROM issuances)                                    AS issuance_count,
                   (SELECT CAST(COALESCE(SUM(active_count), 0) AS SIGNED)
                      FROM coupon_stocks)                                              AS active_total,
                   (SELECT MAX(updated_at) FROM issuances)                             AS max_updated_at
            """;

    private static final RowMapper<VerificationFinding> REPLAY_MISMATCH_MAPPER =
            (rs, rowNum) -> VerificationFinding.forIssuance(
                    FindingType.REPLAY_MISMATCH,
                    rs.getLong("issuance_id"),
                    "replay=" + rs.getString("replayed_state"),
                    "issuances.status=" + rs.getString("stored_status"));

    private static final RowMapper<VerificationFinding> STOCK_MISMATCH_MAPPER =
            (rs, rowNum) -> VerificationFinding.forCoupon(
                    FindingType.STOCK_MISMATCH,
                    rs.getLong("coupon_id"),
                    "replay=" + rs.getInt("replayed_active"),
                    "coupon_stocks.active_count=" + rs.getInt("stored_active"));

    private static final RowMapper<VerificationFinding> DUPLICATE_ISSUANCE_MAPPER =
            (rs, rowNum) -> VerificationFinding.forCouponMember(
                    FindingType.DUP_PER_MEMBER,
                    rs.getLong("coupon_id"),
                    rs.getLong("member_id"),
                    "발급 1건",
                    "발급 2건 이상");

    /** 등급 문자열이 {@code grades} 에 없으면 {@code grade_bit} 가 null 이다 — 그 사실을 증거에 남긴다. */
    private static final RowMapper<VerificationFinding> GRADE_VIOLATION_MAPPER = (rs, rowNum) -> {
        int mask = rs.getInt("eligible_mask");
        int bit = rs.getInt("grade_bit");
        String actualBit = rs.wasNull() ? "grades 에 없는 등급" : "bit=" + bit;

        return VerificationFinding.forIssuance(
                FindingType.GRADE_VIOLATION,
                rs.getLong("issuance_id"),
                "eligible_grades_mask=" + mask,
                "issued_grade=" + rs.getString("issued_grade") + " " + actualBit);
    };

    private static final RowMapper<VerificationFinding> USAGE_MISMATCH_MAPPER =
            (rs, rowNum) -> VerificationFinding.forIssuance(
                    FindingType.USAGE_MISMATCH,
                    rs.getLong("issuance_id"),
                    "active_usage=" + rs.getInt("expected_count"),
                    "active_usage=" + rs.getInt("actual_count"));

    private final JdbcClient jdbcClient;

    public VerificationRuleJdbcAdapter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public List<VerificationFinding> findReplayMismatches(long runId, LocalDateTime asOf, int limit) {
        requireLimit(limit);

        return jdbcClient.sql(SELECT_REPLAY_MISMATCH)
                .param("runId", runId)
                .param("asOf", asOf)
                .param("limit", limit)
                .query(REPLAY_MISMATCH_MAPPER)
                .list();
    }

    @Override
    public List<VerificationFinding> findUsageMismatches(long runId, int limit) {
        requireLimit(limit);

        return jdbcClient.sql(SELECT_USAGE_MISMATCH)
                .param("runId", runId)
                .param("limit", limit)
                .query(USAGE_MISMATCH_MAPPER)
                .list();
    }

    @Override
    public List<VerificationFinding> findStockMismatches(long runId, LocalDateTime asOf, int limit) {
        requireLimit(limit);

        return jdbcClient.sql(SELECT_STOCK_MISMATCH)
                .param("runId", runId)
                .param("asOf", asOf)
                .param("limit", limit)
                .query(STOCK_MISMATCH_MAPPER)
                .list();
    }

    @Override
    public List<VerificationFinding> findGradeViolations(LocalDateTime asOf, int limit) {
        requireLimit(limit);

        return jdbcClient.sql(SELECT_GRADE_VIOLATION)
                .param("asOf", asOf)
                .param("limit", limit)
                .query(GRADE_VIOLATION_MAPPER)
                .list();
    }

    @Override
    public List<VerificationFinding> findDuplicateIssuances(LocalDateTime asOf, int limit) {
        requireLimit(limit);

        return jdbcClient.sql(SELECT_DUPLICATE_ISSUANCE)
                .param("asOf", asOf)
                .param("limit", limit)
                .query(DUPLICATE_ISSUANCE_MAPPER)
                .list();
    }

    @Override
    public String policyDigest() {
        return jdbcClient.sql(SELECT_POLICY_DIGEST)
                .query(String.class)
                .single();
    }

    @Override
    public boolean hasHistoriesAddedAbove(long frozenMaxHistoryId, LocalDateTime asOf) {
        return Boolean.TRUE.equals(jdbcClient.sql("""
                        SELECT EXISTS(
                                 SELECT 1 FROM issuance_histories
                                  WHERE id > :maxHistoryId AND created_at <= :asOf)
                        """)
                .param("maxHistoryId", frozenMaxHistoryId)
                .param("asOf", asOf)
                .query(Boolean.class)
                .single());
    }

    /**
     * <b>V5 와 똑같은 술어를 쓴다</b> — {@code AsOfStateJdbcAdapter#APPLY_USAGE_COUNTS} 의
     * {@code used_at <= asOf AND (canceled_at IS NULL OR canceled_at > asOf)} 그대로다.
     * 이 가드가 답할 질문이 <i>"V5 의 답이 달라지는가"</i> 라서, 술어가 갈리면 둘 중 하나가 된다.
     *
     * <p><b>{@code canceled_at} 을 아예 안 보면 오탐이다.</b> {@code asOf} <b>이전에 이미
     * 취소된</b> 행이 끼어들면 V5 는 그 행을 애초에 안 세므로 답이 그대로인데, 가드만
     * 실행을 죽인다 — 정상 데이터에서 죽는 형상이다.
     *
     * <p><b>반대로 {@code canceled_at IS NULL} 만 보면 놓친다.</b> {@code asOf} <b>이후에</b>
     * 취소되는 행은 V5 가 <i>활성</i>으로 세는데({@code canceled_at > asOf}) 그 술어로는 안 잡힌다.
     * 두 방향을 다 맞추는 것이 V5 와 같은 술어다.
     */
    @Override
    public boolean hasUsagesAddedAbove(long frozenMaxUsageId, LocalDateTime asOf) {
        return Boolean.TRUE.equals(jdbcClient.sql("""
                        SELECT EXISTS(
                                 SELECT 1 FROM issuance_usages
                                  WHERE id > :maxUsageId
                                    AND used_at <= :asOf
                                    AND (canceled_at IS NULL OR canceled_at > :asOf))
                        """)
                .param("maxUsageId", frozenMaxUsageId)
                .param("asOf", asOf)
                .query(Boolean.class)
                .single());
    }

    /**
     * <b>{@code used_at} 술어를 일부러 안 건다.</b> id 는 오토인크리먼트라 <i>얼린 뒤에</i>
     * 들어오는 행은 반드시 절대 최대 id 보다 크다 — 술어를 빼도 가드의 뜻이 그대로다.
     * 빠지는 것은 얼림 시점에 <b>이미 있던</b> {@code used_at > asOf} 행뿐이고,
     * 그 행은 V5 의 입력이 아니라 애초에 안 세어진다.
     *
     * <p>술어를 걸면 값이 비싸진다 — {@code issuance_usages} 에 {@code used_at} 인덱스가
     * 없어 <b>132만 행 전수 스캔</b>이 된다(실측 {@code type=ALL · rows=1,313,897 · 0.32초},
     * 97.6 MiB 를 128 MiB 버퍼 풀에 밀어 넣는다). 술어를 빼면
     * {@code Select tables optimized away} 로 <b>0.0025ms · 스캔 0 페이지</b>다.
     */
    @Override
    public long latestUsageId() {
        // 빈 테이블이면 MAX(id) 가 없다. 그때의 값이 **0 이라고 이미 정해져 있으므로**
        // null 을 한 번 거쳐 다시 비교할 이유가 없다.
        return jdbcClient.sql("SELECT MAX(id) FROM issuance_usages")
                .query(Long.class)
                .optional()
                .orElse(0L);
    }

    @Override
    public boolean hasCleanOnlyConstraints() {
        return Boolean.TRUE.equals(jdbcClient.sql("""
                        SELECT EXISTS(
                                 SELECT 1
                                   FROM information_schema.statistics
                                  WHERE table_schema = DATABASE()
                                    AND table_name = 'issuances'
                                    AND index_name = 'uk_coupon_member')
                        """)
                .query(Boolean.class)
                .single());
    }

    /**
     * {@code information_schema} 한 번으로 끝냅니다. 테이블마다 물으면 왕복이 늘고, 무엇보다
     * <b>없는 테이블에 {@code SELECT} 를 날리면 예외</b>라 "없다" 를 값으로 못 받습니다.
     */
    @Override
    public List<String> missingCoreTables() {
        List<String> present = jdbcClient.sql("""
                        SELECT table_name
                          FROM information_schema.tables
                         WHERE table_schema = DATABASE()
                           AND table_name IN (:names)
                        """)
                .param("names", CORE_TABLES)
                .query(String.class)
                .list();
        return CORE_TABLES.stream()
                .filter(name -> present.stream().noneMatch(name::equalsIgnoreCase))
                .toList();
    }

    @Override
    public List<String> missingCriticalColumns() {
        List<String> present = jdbcClient.sql("""
                        SELECT CONCAT(table_name, '.', column_name)
                          FROM information_schema.columns
                         WHERE table_schema = DATABASE()
                           AND CONCAT(table_name, '.', column_name) IN (:names)
                        """)
                .param("names", CRITICAL_COLUMNS)
                .query(String.class)
                .list();
        return CRITICAL_COLUMNS.stream()
                .filter(name -> present.stream().noneMatch(name::equalsIgnoreCase))
                .toList();
    }

    /**
     * <b>테이블 목록을 상수에서 뽑는다.</b> 질의에 이름을 또 적으면 사실이 두 곳에 산다 —
     * 다른 테이블의 인덱스를 {@link #CRITICAL_INDEXES} 에 더하는 날 질의가 그 행을 절대
     * 안 돌려줘 <b>영원히 "없음"</b> 이 되고, 기동이 계속 거절된다. 컴파일러도 테스트도
     * 안 잡는 모양이다.
     */
    private static List<String> guardedTables() {
        return CRITICAL_INDEXES.stream()
                .map(name -> name.substring(0, name.indexOf('.')))
                .distinct()
                .toList();
    }

    /**
     * <b>이름이 아니라 컬럼 구성까지 본다.</b> 지키려는 성질은 <b>선두 컬럼</b>이다 —
     * {@code V2026082513} 의 헤더가 EXPLAIN 으로 재서 박아 뒀다:
     * {@code (JOB_INSTANCE_ID, STATUS, END_TIME)} 은 {@code type=index rows=25,950} 인데
     * {@code (STATUS, END_TIME)} 은 {@code type=range rows=2,016} 이다. 이름만 대조하면
     * <b>같은 이름의 다른 모양</b>(손으로 친 DDL, 고치기 전 모양이 남은 스키마)이 가드를
     * 통과하고 되읽기는 여전히 전체를 훑는다 — "조용히 느린 것" 이 가드를 지나간다.
     *
     * <p><b>완전 일치는 아니다.</b> 뒤에 컬럼이 더 붙은 인덱스는 그 질의를 그대로 태우므로
     * 통과시킨다({@link #satisfies}). 반대로 하면 멀쩡한 인덱스를 없다고 판정해 기동을 막는다.
     *
     * <p><b>{@code is_visible} 도 본다.</b> {@code ALTER INDEX … INVISIBLE} 을 하면
     * {@code statistics} 에 행은 그대로 남고 <b>옵티마이저만 무시한다</b> — 그것을 안 보면
     * 가드는 "있다" 고 답하는데 되읽기는 여전히 전체를 훑는다. 이 저장소는 인덱스 개선폭을
     * 실측하는 것이 과제의 일부라 그 토글이 실제로 쓰인다.
     *
     * <p>{@code information_schema.statistics} 는 인덱스가 아니라 <b>인덱스 컬럼</b>마다
     * 한 행이라 {@code GROUP BY} 로 접는다 — 그래서 {@code DISTINCT} 가 필요 없다.
     * {@code table_name} 술어로 좁히는 것은 형제 {@code hasCleanOnlyConstraints} 와 같다.
     */
    @Override
    public List<String> missingCriticalIndexes() {
        List<String> present = jdbcClient.sql("""
                        SELECT CONCAT(table_name, '.', index_name, '(',
                                      GROUP_CONCAT(column_name ORDER BY seq_in_index), ')')
                          FROM information_schema.statistics
                         WHERE table_schema = DATABASE()
                           AND table_name IN (:tables)
                           AND is_visible = 'YES'
                         GROUP BY table_name, index_name
                        """)
                .param("tables", guardedTables())
                .query(String.class)
                .list();
        return CRITICAL_INDEXES.stream()
                .filter(required -> present.stream().noneMatch(actual -> satisfies(actual, required)))
                .toList();
    }

    /**
     * <b>완전 일치가 아니라 앞이 같은지를 본다.</b> 지키려는 성질은 <b>선두 컬럼</b>이므로
     * {@code (STATUS, END_TIME, JOB_INSTANCE_ID)} 처럼 뒤에 컬럼이 더 붙은 인덱스도 그 질의를
     * 그대로 태운다 — 완전 일치로 재면 <b>멀쩡한 인덱스를 없다고 판정해 기동을 막는다.</b>
     * 없는 인덱스를 막는 것보다 <i>있는 인덱스를 없다고 하는 것</i>이 더 비싸다.
     *
     * <p>이름은 완전 일치다. 컬럼만 접두사로 본다.
     */
    static boolean satisfies(String actual, String required) {
        if (!indexName(actual).equalsIgnoreCase(indexName(required))) {
            return false;
        }
        List<String> have = columnsOf(actual);
        List<String> want = columnsOf(required);
        if (have.size() < want.size()) {
            return false;
        }
        for (int i = 0; i < want.size(); i++) {
            if (!have.get(i).equalsIgnoreCase(want.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** {@code TABLE.INDEX(COLS)} 에서 {@code TABLE.INDEX}. */
    private static String indexName(String qualified) {
        int paren = qualified.indexOf('(');
        return paren < 0 ? qualified : qualified.substring(0, paren);
    }

    /** {@code TABLE.INDEX(A,B)} 에서 {@code [A, B]}. 괄호가 없으면 빈 목록이다. */
    private static List<String> columnsOf(String qualified) {
        int open = qualified.indexOf('(');
        int close = qualified.lastIndexOf(')');
        if (open < 0 || close <= open + 1) {
            return List.of();
        }
        return List.of(qualified.substring(open + 1, close).split(","));
    }

    @Override
    public String currentSchema() {
        return jdbcClient.sql("SELECT DATABASE()")
                .query(String.class)
                .optional()
                .orElse(null);
    }

    @Override
    public String datasetFingerprint(LocalDateTime asOf) {
        String material = jdbcClient.sql(SELECT_FINGERPRINT_INPUT)
                .param("asOf", asOf)
                .query((rs, rowNum) -> String.join(FINGERPRINT_SEPARATOR,
                        text(rs.getObject("max_history_id")),
                        text(rs.getObject("history_count")),
                        text(rs.getObject("issuance_count")),
                        text(rs.getObject("active_total")),
                        timestamp(rs.getObject("max_updated_at", LocalDateTime.class))))
                .single();

        return DigestValues.sha256Hex(material);
    }

    @Override
    public boolean hasStocksUpdatedAfter(LocalDateTime asOf) {
        return jdbcClient.sql(EXISTS_STOCK_UPDATED_AFTER)
                .param("asOf", asOf)
                .query(Boolean.class)
                .single();
    }

    @Override
    public boolean hasIssuancesUpdatedAfter(LocalDateTime asOf) {
        return jdbcClient.sql(EXISTS_UPDATED_AFTER)
                .param("asOf", asOf)
                .query(Boolean.class)
                .single();
    }

    /** 참조 구현의 폴백은 {@code 0} 이다. {@code totals} 가 0 에서 시작하기 때문이다. */
    private static String text(Object value) {
        return value == null ? "0" : String.valueOf(value);
    }

    /**
     * 계약이 정한 {@code %Y-%m-%d %H:%M:%S.%f} — 마이크로초 6자리다.
     *
     * <p><b>{@code getTimestamp} 를 쓰면 안 된다.</b> 그쪽은 {@code java.sql.Timestamp} 로 받아
     * <b>JVM 기본 시간대로 변환</b>한다. 서버가 UTC 라도 배치를 KST 머신에서 돌리면 9시간이 얹혀,
     * <b>같은 데이터가 머신마다 다른 지문</b>을 낸다 — 지문이 막으려던 바로 그 사고다.
     * {@code LocalDateTime} 으로 직접 받으면 변환이 없다.
     */
    private static String timestamp(LocalDateTime value) {
        return FINGERPRINT_TIME.format(value == null ? EMPTY_DATASET_TIME : value);
    }

    private static void requireLimit(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("검출 상한은 1 이상이어야 합니다. 값=" + limit);
        }
    }
}
