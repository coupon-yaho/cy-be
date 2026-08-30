// 검증이 남긴 파생 행을 걷는 계약입니다. 무엇을 남길지는 부르는 쪽이 정합니다.
package com.kafkick.core.verification;

import java.time.LocalDateTime;
import java.util.List;

/**
 * <b>왜 따로 있나.</b> 정리는 <i>"이 실행의 결과를 쓴다"</i> 가 아니라
 * <i>"더 이상 안 볼 실행을 고른다"</i> 이고, 그 판단은 실행 하나가 아니라 <b>실행 집합</b>을
 * 본다. 기존 포트들은 전부 {@code runId} 를 받는 쓰기 계약이라 이 질문을 담을 자리가 없다.
 *
 * <p><b>지우는 것은 파생 행뿐이다.</b> {@code verification_runs} 행 자체는 남긴다 —
 * 그것이 <i>"언제 무엇을 판정했나"</i> 의 이력이고, {@code cy_batch_last_success_seconds} 와
 * 관제 히스토리가 그 위에 서 있다. 무거운 것은 실행당 최대 300만 행인 {@code asof_state} 이고
 * 그쪽만 걷어도 목적은 달성된다.
 *
 * <p><b>멱등성 레코드는 여기 없다.</b> {@code idempotency_records} 는 테이블만 있고
 * 읽고 쓰는 코드가 저장소에 <b>하나도 없다</b>(영역 ③의 몫이다). 비어 있는 테이블을 지우는
 * 코드는 동작을 증명할 수 없고, 보존 기준도 그 경로를 만드는 쪽이 정해야 한다.
 */
public interface CleanupRepository {

    /**
     * 파생 행을 걷어도 되는 실행들. <b>최신 {@code keepRuns} 개는 빼고</b> 돌려준다.
     *
     * <p><b>데이터셋·범위별로 세지 않고 통째로 센다.</b> 여기서 갈라 세면 CLEAN 을 다섯,
     * CORRUPT 를 다섯 남기게 되는데, 발표 준비로 한쪽만 반복해 돌리는 날 반대쪽이
     * <b>남아 있어야 할 이유 없이</b> 자리를 차지한다. 최신 다섯이면 충분하다는 판단은
     * {@code batch.verify.asof-state-keep-runs} 가 진다.
     *
     * <p>id 오름차순으로 준다 — 오래된 것부터 지워야 중간에 끊겨도 남은 것이 최신 쪽이다.
     *
     * <p><b>도는 중일 수 있는 실행은 뺀다.</b> 보존 창은 <i>개수</i>로 세는데, 개수만 보면
     * 그 사이에 실행이 몇 개 더 생긴 것만으로 <b>아직 판정 중인 실행이 창 밖으로 밀린다</b> —
     * 그 파생 행을 걷으면 그 검증이 자기 입력을 잃고 판정이 데이터 변동으로 죽는다.
     * 그래서 <b>판정이 비어 있으면서 최근에 시작한</b> 실행은 개수와 무관하게 남긴다.
     * (테스트가 이 구멍을 잡았다 — 개수만 보던 첫 구현이 도는 중인 실행을 걷었다.)
     *
     * <p><b>시드가 심은 행은 대상이 아니다.</b> 그 행의 파생 행은 시드의 산출물이고 게이트의
     * 기준값이다 — CORRUPT 는 정답 800행을 그 {@code run_id} 에 붙인다. 구현이
     * {@code origin = 'BATCH'} 로 가른다.
     *
     * <p><b>{@code v_latest_stats_run} 이 가리키는 실행도 뺀다.</b> 보존 창은 {@code id} 순인데
     * 뷰는 {@code as_of} 순이라 두 축이 갈릴 수 있고, 갈리면 대시보드가 조용히 빈다.
     *
     * @param openRunGrace 판정이 비어 있는 실행을 이 시각 이후에 시작했으면 손대지 않는다
     */
    List<Long> purgeableRunIds(int keepRuns, LocalDateTime openRunGrace);

    /**
     * <b>판정을 못 내고 버려진 실행.</b> {@code assertFrozenStep} 이 데이터 변동을 잡으면
     * {@code finalizeRunStep} 이 안 돌아 실행 행이 열린 채 남는데(설계 의도다),
     * 그 실행이 이미 쓴 {@code asof_state} 는 아무도 안 걷는다.
     *
     * <p>CY-384 전에는 이 상황이 아예 못 생겼다 — 스케줄러를 켠 기동에서는 검증이 시작조차
     * 못 했다. 지금은 마이크로초짜리 창과 하드킬이 남아 있어 정상 운영에서 반복된다.
     *
     * <p><b>이 창은 두 번째 방어선이다.</b> 값의 근거가 <i>"300만 전수의 소요를 아직 안 쟀다"</i>
     * ({@code docs/13} §6 의 D) 뿐이라, 실제 소요가 그 값을 넘는 날 <b>도는 검증의 입력</b>이
     * 걷힌다 — 그때 규칙은 빈 상태를 읽고 예외 없이 <b>조용히 틀린 답</b>을 낸다.
     * 첫 번째 방어선은 부르는 쪽이 배치 메타에 묻는 <i>"지금 검증이 도는가"</i> 다.
     *
     * @param olderThan 이 시각보다 먼저 시작한 것만. 지금 도는 실행을 걷지 않기 위한 창이다
     */
    List<Long> abandonedRunIds(LocalDateTime olderThan);

    /**
     * 한 실행의 {@code asof_state} 를 <b>나눠</b> 지운다. 지운 행 수를 돌려준다.
     *
     * <p>한 문장으로 300만을 지우면 언두 로그가 부풀고 그 트랜잭션이 오래 잠긴다.
     * 부르는 쪽이 0 이 나올 때까지 반복한다.
     */
    int deleteAsOfStateChunk(long runId, int chunkSize);

    /**
     * 한 실행의 검출 행. {@code asof_state} 와 달리 건수가 작아 한 번에 지운다.
     *
     * <p><b>{@code FAIL} 판정의 검출 행은 남긴다 — 지우는 축 중 유일한 예외다.</b>
     * {@code verification_runs} 행을 남기기로 한 것과 같은 이유다: 남는 것이
     * <i>"{@code FAIL} 이었고 검출이 800건"</i> 뿐이면 <b>무엇이 틀렸는지가 사라진다.</b>
     * {@code VerificationVerdictFailed} 의 runbook 이 이 행을 직접 가리키고, 이 과제의
     * 합격 조건은 건수가 아니라 집합이다. 크기로도 그쪽이 맞다 — 검출은 규칙당
     * {@code max-findings-per-rule} 로 잘려 있고 무거운 것은 {@code asof_state} 다.
     */
    int deleteFindings(long runId);

    /** 한 청크가 걷은 것. 실행과 그 실행이 고아로 만든 인스턴스를 함께 센다. */
    record PurgedMetadata(int executions, int instances) {

        public boolean isEmpty() {
            return executions == 0 && instances == 0;
        }
    }

    /**
     * <b>보존 기간이 지난 배치 메타를 한 청크 지운다.</b>
     *
     * <p><b>끝난 실행만 지운다.</b> {@code END_TIME} 이 {@code NULL} 인 행 — 실행 중이거나
     * 종료 표시를 못 남기고 죽은 행 — 은 절대 안 건드린다. 시체 행을 지우면
     * {@code BatchStuckExecution} 이 조용해지는데, 그것은 고친 것이 아니라 <b>증거를 지운
     * 것</b>이다. 그 행은 CY-429 의 복구 API 가 사람의 판단으로 닫는다.
     *
     * <p><b>딸린 행을 FK 역순으로 함께 지운다</b> — Step 문맥 · Step · 잡 문맥 · 잡 파라미터 ·
     * 잡 실행 순. 하나라도 순서를 놓치면 제약 위반으로 한 행도 못 지운다.
     *
     * <p><b>고아가 된 인스턴스를 같은 호출에서 함께 지운다.</b> 나눠 두면 그 사이에
     * <b>실행은 없는데 인스턴스는 있는</b> 상태가 남는데, {@code VerificationRunJdbcAdapter}
     * 의 {@code nextAttempt} 가 배치 메타 파라미터를 <b>창 없이</b> 읽어 시도 번호를 정하므로
     * 그 상태에서 번호가 1 로 돌아가고 살아 있는 인스턴스의 {@code JOB_INST_UN} 에 걸린다 —
     * 그쪽이 <i>"몇 번을 눌러도 400 이고 자기 치유가 없다"</i> 고 적어 둔 자리다.
     *
     * @param olderThan 이 시각보다 <b>먼저 끝난</b> 실행이 대상이다
     * @param chunkSize 한 번에 지울 {@code JobExecution} 수. <b>행 수가 아니다</b> —
     *                  실행 하나에 딸린 행이 잡마다 크게 다르다(검증은 Step 열하나)
     */
    PurgedMetadata deleteBatchMetadataChunk(LocalDateTime olderThan, int chunkSize);
}
