// 검증이 남긴 파생 행을 걷는 어댑터입니다. 지우는 것은 나눠서, 고르는 것은 한 번에 합니다.
package com.kafkick.storage.db.verification;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.verification.CleanupRepository;
import com.kafkick.core.verification.CleanupRepository.PurgedMetadata;

/**
 * <b>이미 걷은 실행은 대상이 아니다.</b> 두 선택 질의가 <i>"걷을 파생 행이 남았나"</i> 를
 * 함께 묻는다. 그 술어가 없으면 대상 집합이 <b>지난 이력 전체</b>가 되어, 부르는 쪽의
 * <i>"할 일이 없으면 그냥 성공"</i> 갈래가 <b>스키마가 갓 만들어진 며칠에만</b> 도달한다 —
 * 보존 창 밖에 실행이 하나라도 쌓이는 순간부터 영구히, 걷을 것이 하나도 없는 밤에도
 * 검증이 떠 있으면 {@code YIELDED} 로 닫혀 정상 상태에서 SLA 알림이 운다.
 * 매 밤 수십 번씩 돌던 빈 왕복도 함께 사라진다.
 *
 * <p><b>검출 행 조건이 {@code deleteFindings} 와 짝이어야 한다.</b> 그쪽이 {@code FAIL} 과
 * 오염셋 {@code PASS} 의 검출 행을 남기므로, 여기서 {@code EXISTS(findings)} 만 쓰면
 * 그 실행이 <b>영원히 대상으로 남아</b> 술어가 아무것도 줄이지 못한다.
 *
 * <p>{@code coupon_stats} 로 통계 축을 본다 — {@code stats_status} 가 아니라 실물 행이다.
 * 집계 뒤 상태 갱신 전에 죽은 실행이 있으면 상태만으로는 그 행을 영영 못 찾는다.
 * 세 통계 테이블은 같은 Step 의 한 트랜잭션에서 쓰이므로 하나만 봐도 갈리지 않는다.
 *
 * <p>세 술어 모두 {@code run_id} 가 PK·유니크의 선두라 인덱스로 끝난다.
 *
 * <p><b>{@code verification_runs} 행은 안 지운다.</b> 그것이 <i>"언제 무엇을 판정했나"</i> 의
 * 이력이고 관제 히스토리와 {@code cy_batch_last_success_seconds} 가 그 위에 선다.
 * 무거운 것은 실행당 최대 300만 행인 {@code asof_state} 라, 그쪽만 걷어도 목적은 달성된다.
 *
 * <p><b>{@code origin = 'BATCH'} 를 모든 선택 질의에 건다.</b> 시드가 심은 기준 행은 이
 * 배치가 만든 것이 아니라 <b>게이트가 대조하는 기준값</b>이다 — CORRUPT 는 {@code FAIL} 과
 * 정답 800행을 그 {@code run_id} 에 붙이고, 게이트가 쓰는 {@code as_of} 도 그 행에서 나온다.
 * {@code SELECT_LATEST_CLOSED} 가 같은 이유로 같은 조건을 이미 걸고 있고, 컬럼이 없는
 * 옛 검증용 셋은 기동 프리플라이트({@code CRITICAL_COLUMNS})가 먼저 거절한다.
 */
@Repository
public class CleanupJdbcAdapter implements CleanupRepository {

    private static final String DELETE_STEP_CONTEXT = """
            DELETE sec FROM BATCH_STEP_EXECUTION_CONTEXT sec
              JOIN BATCH_STEP_EXECUTION se ON se.STEP_EXECUTION_ID = sec.STEP_EXECUTION_ID
             WHERE se.JOB_EXECUTION_ID = ?
            """;

    private static final String DELETE_STEP_EXECUTION =
            "DELETE FROM BATCH_STEP_EXECUTION WHERE JOB_EXECUTION_ID = ?";

    private static final String DELETE_EXECUTION_CONTEXT =
            "DELETE FROM BATCH_JOB_EXECUTION_CONTEXT WHERE JOB_EXECUTION_ID = ?";

    private static final String DELETE_EXECUTION_PARAMS =
            "DELETE FROM BATCH_JOB_EXECUTION_PARAMS WHERE JOB_EXECUTION_ID = ?";

    private static final String DELETE_EXECUTION =
            "DELETE FROM BATCH_JOB_EXECUTION WHERE JOB_EXECUTION_ID = ?";

    private static final String COUNT_REMAINING_EXECUTIONS =
            "SELECT COUNT(*) FROM BATCH_JOB_EXECUTION WHERE JOB_EXECUTION_ID IN (:ids)";

    private static final String COUNT_REMAINING_INSTANCES =
            "SELECT COUNT(*) FROM BATCH_JOB_INSTANCE WHERE JOB_INSTANCE_ID IN (:ids)";

    /** 고아 판정을 {@code DELETE} 안에서 한다 — 조회로 고르면 그 사이에 실행이 붙는다. */
    private static final String DELETE_ORPHAN_INSTANCE = """
            DELETE i FROM BATCH_JOB_INSTANCE i
              LEFT JOIN BATCH_JOB_EXECUTION e ON e.JOB_INSTANCE_ID = i.JOB_INSTANCE_ID
             WHERE i.JOB_INSTANCE_ID = ?
               AND e.JOB_EXECUTION_ID IS NULL
            """;

    private final JdbcClient jdbcClient;

    private final JdbcTemplate jdbcTemplate;

    public CleanupJdbcAdapter(JdbcClient jdbcClient, JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.jdbcClient = jdbcClient;
    }

    /**
     * <b>{@code ORDER BY id DESC LIMIT :keep} 를 뒤집어 쓴다.</b> "최신 N 개를 뺀 나머지" 를
     * 한 문장으로 쓰려면 그 N 개를 먼저 골라 제외해야 하는데, MySQL 은 {@code IN} 서브쿼리에
     * {@code LIMIT} 을 못 쓴다 — 파생 테이블로 한 겹 감싸 그 제약을 피한다.
     *
     * <p><b>{@code v_latest_stats_run} 이 가리키는 실행은 보존 창과 무관하게 뺀다.</b>
     * 보존 창은 {@code id} 순으로 세는데 뷰는 {@code as_of DESC, attempt DESC} 로 고른다 —
     * {@code V9} 헤더가 그 두 축이 갈리는 경우를 이미 적어 뒀다({@code attempt} 3 → 4 → 2 로
     * 돌리면 {@code id} 는 4·5·6). 갈리면 뷰가 가리키는 실행이 창 밖으로 밀려 통계 셋이
     * 걷히고, 뷰는 <b>행이 0개인 실행</b>을 "완결된 최신 스냅샷" 으로 계속 가리킨다.
     * 그러면 대시보드가 "데이터 없음" 과 "0건" 을 구분할 수 없다 — {@code V8} 이 이 뷰를
     * 만든 이유가 바로 그 구분이다. 뷰는 항상 0~1행이라 이 절에 비용이 없다.
     */
    @Override
    public List<Long> purgeableRunIds(int keepRuns, LocalDateTime openRunGrace) {
        return jdbcClient.sql("""
                        SELECT r.id
                          FROM verification_runs r
                         WHERE r.origin = 'BATCH'
                           AND r.id NOT IN (
                               SELECT id FROM (
                                   SELECT id FROM verification_runs
                                    WHERE origin = 'BATCH'
                                    ORDER BY id DESC LIMIT :keep
                               ) newest
                         )
                           AND r.id NOT IN (SELECT id FROM v_latest_stats_run)
                           AND NOT (r.verdict IS NULL AND r.started_at >= :openRunGrace)
                           AND (EXISTS (SELECT 1 FROM asof_state a WHERE a.run_id = r.id)
                                OR EXISTS (SELECT 1 FROM coupon_stats c WHERE c.run_id = r.id)
                                OR EXISTS (SELECT 1 FROM verification_findings f
                                            WHERE f.run_id = r.id
                                              AND (r.verdict IS NULL
                                                   OR (r.dataset = 'CLEAN'
                                                       AND r.verdict = 'PASS'))))
                         ORDER BY r.id
                        """)
                .param("keep", keepRuns)
                .param("openRunGrace", openRunGrace)
                .query(Long.class)
                .list();
    }

    /**
     * <b>{@code verdict IS NULL} 이 곧 "판정을 못 냈다" 다.</b> 이 저장소가 그 등식을 여러
     * 곳에서 쓴다 — {@code finalizeRunStep} 이 판정을 쓰는 유일한 자리이고, 그 앞에서 죽으면
     * 열린 채 남는다.
     *
     * <p>{@code finished_at IS NULL} 을 함께 걸지 않는다. 닫혔는데 판정이 비어 있는 행도
     * 파생 행을 남기는 것은 같고, 그 조합은 {@code VerificationMetricsUnknown} 이 따로 본다.
     *
     * <p><b>이 시각 창은 두 번째 방어선이다.</b> 첫 번째는 호출부가 배치 메타로 묻는
     * "지금 검증이 도는가"({@code RunningJobProbe})다. 이 창의 값(하루)은 300만 전수의
     * 소요를 <b>아직 안 쟀기 때문에</b> 고른 것이라, 그것 하나에 파괴적 삭제를 맡길 수 없다.
     */
    @Override
    public List<Long> abandonedRunIds(LocalDateTime olderThan) {
        return jdbcClient.sql("""
                        SELECT r.id
                          FROM verification_runs r
                         WHERE r.origin = 'BATCH'
                           AND r.verdict IS NULL
                           AND r.started_at < :olderThan
                           AND r.id NOT IN (SELECT id FROM v_latest_stats_run)
                           -- 걷을 파생 행이 남았나. 위에서 verdict IS NULL 을 이미 걸었으므로
                           -- 검출 행은 **무조건 삭제 대상**이다 — purgeableRunIds 쪽의
                           -- deleteFindings 짝 조건이 여기서는 항상 참이라 안 적는다.
                           AND (EXISTS (SELECT 1 FROM asof_state a WHERE a.run_id = r.id)
                                OR EXISTS (SELECT 1 FROM coupon_stats c WHERE c.run_id = r.id)
                                OR EXISTS (SELECT 1 FROM verification_findings f
                                            WHERE f.run_id = r.id))
                         ORDER BY r.id
                        """)
                .param("olderThan", olderThan)
                .query(Long.class)
                .list();
    }

    /**
     * <b>PK 로 지운다.</b> {@code asof_state} 의 PK 가 {@code (run_id, coupon_id)} 라
     * {@code run_id} 가 선두이고, 그래서 {@code LIMIT} 을 붙여도 범위가 그 실행 안에 머문다.
     */
    @Override
    public int deleteAsOfStateChunk(long runId, int chunkSize) {
        return jdbcClient.sql("DELETE FROM asof_state WHERE run_id = :runId LIMIT :chunk")
                .param("runId", runId)
                .param("chunk", chunkSize)
                .update();
    }

    /**
     * <b>지우는 것은 "설명할 판정이 없는" 검출 행뿐이다.</b> 남기는 쪽이 기본이고 지우는
     * 쪽이 예외다 — 검출 행은 규칙당 {@code max-findings-per-rule} 로 잘려 있어 실행당 만
     * 단위인데, 무거운 것은 300만 행짜리 {@code asof_state} 다. 크기로 지울 이유가 없다.
     *
     * <p><b>지우는 갈래 둘.</b>
     * <ul>
     *   <li>{@code verdict IS NULL} — 버려진 실행. 설명할 판정이 아예 없다.
     *       {@code <> 'FAIL'} 류로 쓰면 NULL 비교가 UNKNOWN 이라 이 행이 영원히 남는다.</li>
     *   <li>{@code dataset='CLEAN' AND verdict='PASS'} — 정상셋 합격. 정의상 0행이라
     *       잃을 것이 없다.</li>
     * </ul>
     *
     * <p><b>{@code CORRUPT} 의 {@code PASS} 를 지우면 이 과제의 합격 증거가 사라진다.</b>
     * {@code judgeAgainstManifest} 는 검출 집합이 정답 매니페스트와 <b>정확히 일치할 때</b>
     * {@code PASS} 를 낸다 — 오염셋에서 목표하는 결과가 바로
     * {@code dataset=CORRUPT · verdict=PASS · finding_count=800} 이고, 그 800행의
     * {@code (finding_type, target_key)} 가 <i>"누락 0 · 오탐 0"</i> 을 보여 주는 산출물이다.
     * 한때 <i>"{@code FAIL} 만 남긴다"</i> 로 썼는데 정확히 그 산출물을 지우는 조건이었다.
     *
     * <p>{@code FAIL} 도 당연히 남는다. {@code VerificationVerdictFailed} 의 runbook 이
     * <i>"{@code verification_findings} 의 {@code (finding_type, target_key)} 를 봅니다"</i>
     * 라고 그 행을 직접 가리킨다.
     *
     * <p><b>{@code finding_count} 를 안 내리는 것이 여기서는 모순이 아니다.</b> 지우는 두
     * 갈래는 각각 그 값이 0 이거나({@code CLEAN} {@code PASS}) 판정 자체가 비어 있다
     * (버려진 실행) — 행과 포인터가 어긋나는 조합이 안 생긴다.
     */
    @Override
    public int deleteFindings(long runId) {
        return jdbcClient.sql("""
                        DELETE f
                          FROM verification_findings f
                          JOIN verification_runs r ON r.id = f.run_id
                         WHERE f.run_id = :runId
                           AND (r.verdict IS NULL
                                OR (r.dataset = 'CLEAN' AND r.verdict = 'PASS'))
                        """)
                .param("runId", runId)
                .update();
    }

    /**
     * <b>대상 실행을 먼저 고르고 그 목록으로 지운다.</b> 딸린 테이블마다 조건을 다시 쓰면
     * 그 사이 새 행이 끼어 <b>부모만 남고 자식이 지워지는</b> 상태가 생긴다 —
     * 같은 트랜잭션이라 안 생길 것 같지만, 조건이 여섯 벌이 되면 고치는 날 하나만 고쳐진다.
     *
     * <p><b>{@code CREATE_TIME} 조건을 함께 건다.</b> 술어는 {@code END_TIME} 인데 그 컬럼엔
     * 쓸 인덱스가 없다({@code V14} 는 선두가 {@code STATUS}). 실행은 <b>만들어진 뒤에
     * 끝나므로</b> {@code END_TIME < :olderThan} <b>이면</b> {@code CREATE_TIME < :olderThan}
     * 도 참이다 — 답을 안 바꾸면서 {@code V15} 의 {@code (CREATE_TIME)} range 스캔을 탄다.
     * 그 조건이 없으면 <b>지울 것이 0 인 날에도 테이블 전체를 훑는다</b>(실측: PK 스캔
     * 30,000행 4.5ms → range 스캔 0행 0.10ms).
     *
     * <p>⚠️ <b>함의는 한 방향뿐이다 — 역은 거짓이다.</b> {@code END_TIME} 술어 둘은
     * 인덱스용이 아니라 <b>시체 보존</b>이다. 끝나지 않은 실행은 {@code CREATE_TIME} 이
     * 아무리 오래돼도 대상이 아니어야 한다 — 지우면 {@code BatchStuckExecution} 이
     * 조용해지는데 그건 고친 게 아니라 <b>증거를 지운 것</b>이다. 중복처럼 보인다고
     * 줄이면 그 계약이 사라진다.
     *
     * <p><b>{@code ORDER BY} 도 {@code CREATE_TIME} 으로 간다 — 다만 그게 계획을 고정해
     * 주지는 않는다.</b> 실행 3,000 · 90일 균등 · id 오름차순 = 시각 오름차순으로 훑은
     * 결과({@code LIMIT 500}): 대상 0·6·1,995 에서는 {@code CREATE_TIME} 정렬이 인덱스
     * range 를 타지만, <b>중간 선택도(대상 990 ≈ 33%)에서는 table scan 3,000행 +
     * filesort 로 내려간다.</b> 그 구간에서는 PK 정렬이 오히려 500행만 읽는다.
     * 여기서 {@code CREATE_TIME} 정렬을 고른 이유는 <b>정상 야간(대상 0~수십)에서 Sort
     * 노드가 안 붙기 때문</b>이고, 중간 선택도는 보존 창을 처음 넘긴 밤에만 지나간다
     * (비용도 테이블 크기에 묶여 있다 — 3,000행 0.6ms). 고정하려면 {@code FORCE INDEX}
     * 가 필요한데 그러면 인덱스 이름에 코드가 묶인다.
     *
     * <p>⚠️ <b>한때 "PK 로 정렬하면 대상 1,201 에 1,201행을 읽는다" 고 적었는데 그건
     * id 순서와 시각 순서가 뒤집힌 시드에서 나온 값이다.</b> 실제로는 실행 id 가 시각 순으로
     * 발급되므로 PK 정렬도 오래된 것부터 집는다. 위 수치가 바로잡은 값이다.
     *
     * <p><b>⚠️ 삭제는 {@code IN} 목록이 아니라 id 하나씩이다. 이게 이 메서드에서 가장
     * 중요한 결정이다.</b> {@code IN} 목록이 그 테이블 행 수의 큰 비율이 되면 옵티마이저가
     * 인덱스를 버리고 <b>풀스캔</b>을 고르는데({@code EXPLAIN} 이 {@code type=ALL}),
     * 풀스캔 {@code DELETE} 는 <b>대상이 아닌 행까지 전부 잠근다.</b> 그러면 양쪽이 다 깨진다:
     *
     * <ul>
     *   <li><b>남을 막는다</b> — {@code REPEATABLE READ} 에서는 스캔한 레코드 + 갭 +
     *       supremum 에 X 락이 걸린다. 실측(MySQL 8.0.46, 인스턴스 180 / 실행 180 /
     *       Step 1,980, {@code IN} 90): {@code data_locks} 가
     *       {@code BATCH_JOB_INSTANCE} 181, {@code BATCH_STEP_EXECUTION} 2,003.
     *       그 청크가 열려 있는 동안 다른 세션의 <b>새 JobInstance INSERT · 새 JobExecution
     *       INSERT · 도는 잡의 STEP UPDATE</b> 가 전부 {@code ERROR 1205} 였다. 셋째가
     *       도는 잡의 청크 커밋이자 {@code RunningJobProbe} 가 읽는 하트비트다.</li>
     *   <li><b>남에게 막힌다</b> — 격리수준을 {@code READ COMMITTED} 로 내려도 이쪽은
     *       안 사라진다. 갭 락은 없어지지만 풀스캔은 여전히 <b>대상이 아닌 잠긴 행에서
     *       대기</b>한다(semi-consistent read 는 {@code UPDATE} 전용이다). 실측: 도는 잡이
     *       자기 STEP 행 하나를 잡고 있는 동안 RC 청크가 {@code ERROR 1205} 로 죽었다.</li>
     * </ul>
     *
     * <p>id 하나씩이면 계획이 고정된다 — 여섯 문장 전부 {@code type=const/range/ref} 이고
     * <b>읽는 행이 그 실행에 딸린 자식 수에만 비례한다</b>({@code verifyJob} 이면 Step 열하나라
     * Step 삭제가 {@code rows=11}, 나머지는 {@code rows=1}). <b>테이블 크기와 무관하고
     * 대상 밖 행을 아예 안 건드린다.</b> 같은 프로브를
     * {@code REPEATABLE READ} 와 {@code READ COMMITTED} 양쪽에서 돌려 <b>둘 다 네 가지가
     * 전부 통과</b>하는 것을 확인했다 — 그래서 격리수준은 기본값 그대로 둔다.
     * <b>병목은 격리수준이 아니라 계획이었다.</b>
     *
     * <p>대가는 문장 수다. 청크 하나가 {@code 6 × chunkSize} 문장이 된다 — 실측으로
     * 5,000 실행 삭제가 680ms(IN 목록) → 1,980ms(단건, 청크당 1 트랜잭션)로 약 2.9배다.
     * 청크(500)당 200ms 수준이라 {@code step-timeout-ms}(120초)에 한참 못 미친다.
     * <b>결정성을 그 값에 샀다.</b> {@code JdbcTemplate#batchUpdate} 로 보내 왕복은 묶는다.
     *
     * <p><b>{@code MANDATORY} 다.</b> 여섯 문장의 원자성이 이 메서드의 계약인데, 지금은
     * 호출자가 태스클릿 트랜잭션 안이라는 <i>사실</i>에만 기대고 있다. 나중에 관리 API 나
     * 다른 스케줄러가 트랜잭션 없이 부르면 문장마다 자동 커밋되고, 실행을 지운 뒤 죽으면
     * <b>고아 인스턴스만 남은 중간 상태</b>가 그대로 남는다({@code CleanupRepository} 가
     * 위험하다고 적어 둔 그 상태다). {@code MANDATORY} 면 그 호출이 배포 뒤가 아니라
     * <b>첫 호출에서</b> 거절된다 — 태스클릿 경로는 그대로 조인한다.
     *
     * <p>고아 인스턴스는 <b>같은 트랜잭션에서 그 실행들의 인스턴스만</b> 본다. 전역으로 훑지
     * 않으므로 남의 테스트가 남긴 행도 안 건드리고, 실행만 지워진 중간 상태도 안 남는다.
     * 판정은 {@code DELETE} 안의 anti-join 이 한다 — 조회로 고르고 id 로 지우면 그 사이에
     * 실행이 붙어 FK 위반으로 청크가 통째로 롤백된다(docs/04 가 금지한 "조회→판단→갱신").
     */
    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public PurgedMetadata deleteBatchMetadataChunk(LocalDateTime olderThan, int chunkSize) {
        List<long[]> targets = jdbcClient.sql("""
                        SELECT JOB_EXECUTION_ID, JOB_INSTANCE_ID
                          FROM BATCH_JOB_EXECUTION
                         WHERE CREATE_TIME < :olderThan
                           AND END_TIME IS NOT NULL
                           AND END_TIME < :olderThan
                         ORDER BY CREATE_TIME, JOB_EXECUTION_ID
                         LIMIT :chunkSize
                        """)
                .param("olderThan", olderThan)
                .param("chunkSize", chunkSize)
                .query((rs, rowNum) -> new long[] {rs.getLong(1), rs.getLong(2)})
                .list();
        if (targets.isEmpty()) {
            return new PurgedMetadata(0, 0);
        }
        List<Long> executionIds = targets.stream().map(row -> row[0]).toList();
        // 인스턴스는 실행마다 하나이고 중복될 수 있다. 순서를 지켜 중복만 걷는다.
        List<Long> instanceIds = targets.stream().map(row -> row[1]).distinct().toList();

        // FK 역순. 하나라도 순서를 바꾸면 제약 위반으로 한 행도 못 지운다.
        deleteEach(DELETE_STEP_CONTEXT, executionIds);
        deleteEach(DELETE_STEP_EXECUTION, executionIds);
        deleteEach(DELETE_EXECUTION_CONTEXT, executionIds);
        deleteEach(DELETE_EXECUTION_PARAMS, executionIds);
        deleteEach(DELETE_EXECUTION, executionIds);
        deleteEach(DELETE_ORPHAN_INSTANCE, instanceIds);

        int executions = executionIds.size() - countRemaining(COUNT_REMAINING_EXECUTIONS, executionIds);
        int purgedInstances = instanceIds.size() - countRemaining(COUNT_REMAINING_INSTANCES, instanceIds);

        return new PurgedMetadata(executions, purgedInstances);
    }

    /**
     * <b>id 하나씩 보내되 왕복은 묶는다.</b> {@code batchUpdate} 가 한 {@code PreparedStatement}
     * 에 파라미터만 갈아 끼우므로 계획은 문장마다 고정되고 네트워크 왕복은 배치 하나다.
     *
     * <p><b>반환값을 안 쓴다.</b> JDBC 배치는 원소마다 {@code SUCCESS_NO_INFO(-2)} 를 돌려줄 수
     * 있고 — 접속 URL 에 {@code rewriteBatchedStatements=true} 가 걸려 있어 더 그렇다 — 그것을
     * 그대로 더하면 합계가 <b>음수 쪽으로 조용히 망가진다.</b> 지금 드라이버가 그러지 않는 것은
     * 확인했지만(MySQL 8.0 + Connector/J 9.7.0, 배치 1·2·3·4·10·500 에서 전부 실제 카운트),
     * <b>드라이버가 바뀌는 날 조용히 틀리는 쪽에 관측 지표를 걸어 둘 이유가 없다.</b>
     * 그래서 지운 수는 {@link #countRemaining} 이 상태에서 뽑는다.
     */
    private void deleteEach(String sql, List<Long> ids) {
        jdbcTemplate.batchUpdate(sql, ids, ids.size(), (ps, id) -> ps.setLong(1, id));
    }

    /**
     * <b>지운 수를 상태에서 뽑는다.</b> "고른 수 − 남은 수" 라 드라이버가 무엇을 돌려주든 정확하고,
     * 고아 인스턴스처럼 <b>조건부로만 지워지는</b> 축도 그대로 센다.
     *
     * <p>여기는 {@code IN} 목록을 써도 된다 — <b>읽기라 락을 안 잡는다.</b> 삭제 쪽이 id 단건인
     * 이유(풀스캔이 대상 밖 행을 잠근다)는 이 조회에 해당하지 않는다.
     */
    private int countRemaining(String sql, List<Long> ids) {
        Integer remaining = jdbcClient.sql(sql).param("ids", ids).query(Integer.class).single();
        return remaining == null ? 0 : remaining;
    }
}
