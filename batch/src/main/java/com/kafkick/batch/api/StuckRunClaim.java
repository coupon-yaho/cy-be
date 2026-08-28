// 시체 복구가 쓰는 선점 문장을 한 곳에 둡니다.
package com.kafkick.batch.api;

/**
 * <b>판정과 쓰기 사이를 닫는 선점 문장.</b> {@code requireStuck} 으로 시체라고 판정한 뒤
 * 그 조건을 <b>쓰기 문장 안에 다시 걸고</b> affected rows 를 본다 — 그 사이에 실행이
 * 되살아날 수 있고(락 대기가 풀리는 경우가 그렇다) 그때 살아 있는 잡을 닫는다.
 *
 * <p><b>왜 모아 뒀나.</b> {@link ExpireRecoveryService} 와 {@link CleanupRecoveryService} 가
 * <b>글자까지 같은 문장</b>을 쓴다. 두 벌로 두면 한쪽만 고치는 날 판정과 쓰기가 다른 조건을
 * 보게 되는데, 그 어긋남은 <b>드물게만</b> 드러나 테스트로 잡기 어렵다.
 *
 * <p><b>{@link VerifyStopService} 는 자기 것을 따로 든다.</b> 그쪽은 {@code STOPPING} 을 빼야
 * 한다 — {@code SimpleJobOperator.stop} 이 {@code STARTED}·{@code STARTING} 만 받고 나머지에
 * {@code JobExecutionNotRunningException} 을 던지므로, 넣으면 선점만 성공하고 다음 줄이
 * 던져 <b>아무것도 안 한 채 VERSION 만 올린다.</b> 그 차이가 의도라는 것을 여기 적어 둔다.
 */
final class StuckRunClaim {

    private StuckRunClaim() {
    }

    /**
     * <b>{@code STATUS} 목록이 조회와 같아야 한다</b>({@code JdbcJobExecutionDao} 의 상수와
     * 글자까지 같다). 하나라도 빠지면 그 상태의 시체가 <b>아무것도 안 한 채 200</b> 을 받는다.
     *
     * <p><b>폴백까지 그대로 옮긴다.</b> {@code RunningJobProbe} 의 판정이
     * {@code MAX(LAST_UPDATED)} → {@code START_TIME} → {@code CREATE_TIME} 순으로 떨어지는데,
     * {@code NOT EXISTS} 로만 쓰면 <b>Step 행이 없는 실행에서 무조건 참</b>이 되어 방금 뜬
     * {@code STARTING} 실행도 닫는다.
     */
    static final String CLAIM = """
            UPDATE BATCH_JOB_EXECUTION je
               SET je.VERSION = je.VERSION + 1
             WHERE je.JOB_EXECUTION_ID = :id
               AND je.STATUS IN ('STARTING','STARTED','STOPPING')
               AND COALESCE((SELECT MAX(se.LAST_UPDATED) FROM BATCH_STEP_EXECUTION se
                              WHERE se.JOB_EXECUTION_ID = je.JOB_EXECUTION_ID),
                            je.START_TIME, je.CREATE_TIME) <= :stuckBefore
            """;

    /** 선점에 진 뒤 <b>현재 읽기</b>로 상태를 본다. 스냅샷 읽기는 옛 값을 준다(RR). */
    static final String CURRENT_STATUS = """
            SELECT STATUS FROM BATCH_JOB_EXECUTION
             WHERE JOB_EXECUTION_ID = :id FOR SHARE
            """;
}
