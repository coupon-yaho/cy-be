// 검증이 남긴 파생 행을 걷는 어댑터입니다. 지우는 것은 나눠서, 고르는 것은 한 번에 합니다.
package com.kafkick.storage.db.verification;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.kafkick.core.verification.CleanupRepository;

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

    private final JdbcClient jdbcClient;

    public CleanupJdbcAdapter(JdbcClient jdbcClient) {
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
                           AND (EXISTS (SELECT 1 FROM asof_state a WHERE a.run_id = r.id)
                                OR EXISTS (SELECT 1 FROM coupon_stats c WHERE c.run_id = r.id)
                                OR EXISTS (SELECT 1 FROM verification_findings f
                                            WHERE f.run_id = r.id
                                              AND (r.verdict IS NULL
                                                   OR (r.dataset = 'CLEAN'
                                                       AND r.verdict = 'PASS'))))
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
}
