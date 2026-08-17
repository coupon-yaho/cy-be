// 검출 결과 저장 계약입니다. 규칙 6종이 전부 이 계층으로 결과를 남깁니다.
package com.kafkick.core.verification;

import java.util.List;

/**
 * 읽기는 여기에 없습니다. 판정(양방향 MINUS·checksum)은 300만 행을 자바로 끌어올리지 않고
 * 집계 SQL 로 하므로 판정 쪽이 직접 질의합니다.
 */
public interface VerificationFindingRepository {

    /**
     * 검출 결과를 한 묶음으로 쌓는다.
     *
     * <p>같은 행을 다시 써도 되게 만든다. {@code uk_run_finding(run_id, finding_type, target_key)}
     * 가 걸려 있어 그냥 INSERT 면 같은 검출이 두 번 나오는 순간 중복키로 죽는다.
     * 지금 배선에서 재시작은 막혀 있지만(`preventRestart`), 청크 롤백 후 재실행과
     * 앞으로의 변경까지 덮으려면 멱등이 맞다.
     *
     * <p>행 수를 돌려주지 않는다. 배치 재작성이 켜지면 드라이버가 행마다
     * {@code SUCCESS_NO_INFO} 를 준다. 검출 건수는 판정 단계가 집계 SQL 로 따로 센다.
     */
    void appendAll(long runId, List<VerificationFinding> findings);

    /**
     * 이 실행의 검출 수. 판정과 함께 {@code verification_runs} 에 남습니다.
     *
     * <p>규칙 Step 이 센 것을 더하지 않고 <b>저장된 행을 다시 셉니다.</b> 더하면
     * {@code uk_run_finding} 이 접은 중복이 개수에만 남아, checksum 과 어긋난 수가 기록됩니다.
     */
    int countOf(long runId);

    /**
     * 검출 집합의 checksum. <b>재실행 결정론 판정의 근거</b>입니다.
     *
     * <p>계약({@code docs/contract.json} 의 {@code findings_checksum})이 정한 인코딩을 그대로 씁니다 —
     * 정렬된 {@code (finding_type, target_key)} 만, {@code finding_type + U+001F + target_key + U+001E}
     * 를 반복한 뒤 SHA-256. <b>다른 컬럼을 섞으면 안 됩니다</b> — {@code id} 나 시각이 들어가면
     * 같은 데이터의 재실행이 매번 다른 값을 냅니다.
     *
     * <p><b>DB 에서 접지 않고 자바에서 흘려 계산합니다.</b> {@code GROUP_CONCAT} 은
     * {@code group_concat_max_len} 을 넘으면 경고만 내고 조용히 잘라, 오염셋 800행에서
     * <b>뒤쪽 검출이 checksum 에 안 들어갑니다</b> — 결정론 판정이 열린 채로 통과합니다.
     *
     * <p>검출이 없으면 빈 입력의 SHA-256 입니다. {@code null} 이 아닙니다 —
     * 정상셋의 0건은 <b>판정 대상</b>이지 미실행이 아닙니다.
     */
    String checksumOf(long runId);
}
